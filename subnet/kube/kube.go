// Copyright 2016 flannel authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kube

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/coreos/flannel/pkg/ip"
	"github.com/coreos/flannel/subnet"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	ErrUnimplemented = errors.New("unimplemented")
)

const (
	resyncPeriod              = 5 * time.Minute
	nodeControllerSyncTimeout = 10 * time.Minute

	subnetKubeManagedAnnotation = "flannel.alpha.coreos.com/kube-subnet-manager"
	backendDataAnnotation       = "flannel.alpha.coreos.com/backend-data"
	backendTypeAnnotation       = "flannel.alpha.coreos.com/backend-type"
	backendPublicIPAnnotation   = "flannel.alpha.coreos.com/public-ip"
	FlannelPodCIDRAnnotation    = "flannel.alpha.coreos.com/pod-cidr"

	netConfPath = "/etc/kube-flannel/net-conf.json"

	flannelPodCIDRConfigmapName = "flannel-pod-cidr"
)

type kubeSubnetManager struct {
	client         clientset.Interface
	nodeName       string
	nodeStore      listers.NodeLister
	nodeController cache.Controller
	subnetConf     *subnet.Config
	events         chan subnet.Event

	podNamespace string
}

func NewSubnetManager(apiUrl, kubeconfig string) (subnet.Manager, error) {

	var cfg *rest.Config
	var err error
	// Use out of cluster config if the URL or kubeconfig have been specified. Otherwise use incluster config.
	if apiUrl != "" || kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags(apiUrl, kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("unable to create k8s config: %v", err)
		}
	} else {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("unable to initialize inclusterconfig: %v", err)
		}
	}

	c, err := clientset.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize client: %v", err)
	}

	// The kube subnet mgr needs to know the k8s node name that it's running on so it can annotate it.
	// If we're running as a pod then the POD_NAME and POD_NAMESPACE will be populated and can be used to find the node
	// name. Otherwise, the environment variable NODE_NAME can be passed in.
	nodeName := os.Getenv("NODE_NAME")
	podNamespace := os.Getenv("POD_NAMESPACE")
	if nodeName == "" {
		podName := os.Getenv("POD_NAME")
		if podName == "" || podNamespace == "" {
			return nil, fmt.Errorf("env variables POD_NAME and POD_NAMESPACE must be set")
		}

		pod, err := c.Pods(podNamespace).Get(podName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("error retrieving pod spec for '%s/%s': %v", podNamespace, podName, err)
		}
		nodeName = pod.Spec.NodeName
		if nodeName == "" {
			return nil, fmt.Errorf("node name not present in pod spec '%s/%s'", podNamespace, podName)
		}
	}

	netConf, err := ioutil.ReadFile(netConfPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read net conf: %v", err)
	}

	sc, err := subnet.ParseConfig(string(netConf))
	if err != nil {
		return nil, fmt.Errorf("error parsing subnet config: %s", err)
	}

	sm, err := newKubeSubnetManager(c, sc, nodeName)
	if err != nil {
		return nil, fmt.Errorf("error creating network manager: %s", err)
	}
	sm.podNamespace = podNamespace
	go sm.Run(context.Background())

	glog.Infof("Waiting %s for node controller to sync", nodeControllerSyncTimeout)
	err = wait.Poll(time.Second, nodeControllerSyncTimeout, func() (bool, error) {
		return sm.nodeController.HasSynced(), nil
	})
	if err != nil {
		return nil, fmt.Errorf("error waiting for nodeController to sync state: %v", err)
	}

	if err := sm.ensureFlannelPodCIDRConfigmap(); err != nil {
		return nil, fmt.Errorf("ensure flannel pod CIDR configmap error:%v", err)
	}
	if err := sm.allocCIDRToNode(); err != nil {
		return nil, fmt.Errorf("alloc CIDR to node error:%v", err)
	}
	glog.Infof("Node controller sync successful")

	return sm, nil
}

func newKubeSubnetManager(c clientset.Interface, sc *subnet.Config, nodeName string) (*kubeSubnetManager, error) {
	var ksm kubeSubnetManager
	ksm.client = c
	ksm.nodeName = nodeName
	ksm.subnetConf = sc
	ksm.events = make(chan subnet.Event, 5000)
	indexer, controller := cache.NewIndexerInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return ksm.client.CoreV1().Nodes().List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return ksm.client.CoreV1().Nodes().Watch(options)
			},
		},
		&v1.Node{},
		resyncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				ksm.handleAddLeaseEvent(subnet.EventAdded, obj)
			},
			UpdateFunc: ksm.handleUpdateLeaseEvent,
			DeleteFunc: func(obj interface{}) {
				ksm.handleAddLeaseEvent(subnet.EventRemoved, obj)
			},
		},
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)
	ksm.nodeController = controller
	ksm.nodeStore = listers.NewNodeLister(indexer)
	return &ksm, nil
}

func (ksm *kubeSubnetManager) handleAddLeaseEvent(et subnet.EventType, obj interface{}) {
	n := obj.(*v1.Node)
	if s, ok := n.Annotations[subnetKubeManagedAnnotation]; !ok || s != "true" {
		return
	}

	l, err := nodeToLease(*n)
	if err != nil {
		glog.Infof("Error turning node %q to lease: %v", n.ObjectMeta.Name, err)
		return
	}
	ksm.events <- subnet.Event{et, l}
}

func (ksm *kubeSubnetManager) handleUpdateLeaseEvent(oldObj, newObj interface{}) {
	o := oldObj.(*v1.Node)
	n := newObj.(*v1.Node)
	if s, ok := n.Annotations[subnetKubeManagedAnnotation]; !ok || s != "true" {
		return
	}
	if o.Annotations[backendDataAnnotation] == n.Annotations[backendDataAnnotation] &&
		o.Annotations[backendTypeAnnotation] == n.Annotations[backendTypeAnnotation] &&
		o.Annotations[backendPublicIPAnnotation] == n.Annotations[backendPublicIPAnnotation] {
		return // No change to lease
	}

	l, err := nodeToLease(*n)
	if err != nil {
		glog.Infof("Error turning node %q to lease: %v", n.ObjectMeta.Name, err)
		return
	}
	ksm.events <- subnet.Event{subnet.EventAdded, l}
}

func (ksm *kubeSubnetManager) GetNetworkConfig(ctx context.Context) (*subnet.Config, error) {
	return ksm.subnetConf, nil
}

func (ksm *kubeSubnetManager) AcquireLease(ctx context.Context, attrs *subnet.LeaseAttrs) (*subnet.Lease, error) {
	cachedNode, err := ksm.nodeStore.Get(ksm.nodeName)
	if err != nil {
		return nil, err
	}
	nobj, err := api.Scheme.DeepCopy(cachedNode)
	if err != nil {
		return nil, err
	}
	n := nobj.(*v1.Node)

	flannelPodCIDR := nodeGetCIDR(n)
	if flannelPodCIDR == "" {
		return nil, fmt.Errorf("node %q pod cidr not assigned", ksm.nodeName)
	}
	bd, err := attrs.BackendData.MarshalJSON()
	if err != nil {
		return nil, err
	}
	_, cidr, err := net.ParseCIDR(flannelPodCIDR)
	if err != nil {
		return nil, err
	}
	if n.Annotations[backendDataAnnotation] != string(bd) ||
		n.Annotations[backendTypeAnnotation] != attrs.BackendType ||
		n.Annotations[backendPublicIPAnnotation] != attrs.PublicIP.String() ||
		n.Annotations[subnetKubeManagedAnnotation] != "true" {
		n.Annotations[backendTypeAnnotation] = attrs.BackendType
		n.Annotations[backendDataAnnotation] = string(bd)
		n.Annotations[backendPublicIPAnnotation] = attrs.PublicIP.String()
		n.Annotations[subnetKubeManagedAnnotation] = "true"

		oldData, err := json.Marshal(cachedNode)
		if err != nil {
			return nil, err
		}

		newData, err := json.Marshal(n)
		if err != nil {
			return nil, err
		}

		patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.Node{})
		if err != nil {
			return nil, fmt.Errorf("failed to create patch for node %q: %v", ksm.nodeName, err)
		}

		_, err = ksm.client.CoreV1().Nodes().Patch(ksm.nodeName, types.StrategicMergePatchType, patchBytes, "status")
		if err != nil {
			return nil, err
		}
	}
	return &subnet.Lease{
		Subnet:     ip.FromIPNet(cidr),
		Attrs:      *attrs,
		Expiration: time.Now().Add(24 * time.Hour),
	}, nil
}

func (ksm *kubeSubnetManager) WatchLeases(ctx context.Context, cursor interface{}) (subnet.LeaseWatchResult, error) {
	select {
	case event := <-ksm.events:
		return subnet.LeaseWatchResult{
			Events: []subnet.Event{event},
		}, nil
	case <-ctx.Done():
		return subnet.LeaseWatchResult{}, nil
	}
}

func (ksm *kubeSubnetManager) Run(ctx context.Context) {
	glog.Infof("Starting kube subnet manager")
	ksm.nodeController.Run(ctx.Done())
}

func nodeToLease(n v1.Node) (l subnet.Lease, err error) {
	l.Attrs.PublicIP, err = ip.ParseIP4(n.Annotations[backendPublicIPAnnotation])
	if err != nil {
		return l, err
	}

	l.Attrs.BackendType = n.Annotations[backendTypeAnnotation]
	l.Attrs.BackendData = json.RawMessage(n.Annotations[backendDataAnnotation])

	_, cidr, err := net.ParseCIDR(nodeGetCIDR(&n))
	if err != nil {
		return l, err
	}

	l.Subnet = ip.FromIPNet(cidr)
	return l, nil
}

// unimplemented
func (ksm *kubeSubnetManager) RenewLease(ctx context.Context, lease *subnet.Lease) error {
	return ErrUnimplemented
}

func (ksm *kubeSubnetManager) WatchLease(ctx context.Context, sn ip.IP4Net, cursor interface{}) (subnet.LeaseWatchResult, error) {
	return subnet.LeaseWatchResult{}, ErrUnimplemented
}

func (ksm *kubeSubnetManager) Name() string {
	return fmt.Sprintf("Kubernetes Subnet Manager - %s", ksm.nodeName)
}

func nodeGetCIDR(n *v1.Node) string {
	cidr := n.Annotations[flannelPodCIDRConfigmapName]
	return cidr
}

func (ksm *kubeSubnetManager) ensureFlannelPodCIDRConfigmap() error {
	_, err := ksm.client.CoreV1().ConfigMaps(ksm.podNamespace).Get(flannelPodCIDRConfigmapName, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		glog.Errorf("get flannel pod cidr configmap error:%v", err)
		return err
	} else if err == nil {
		return nil
	}

	cm := &v1.ConfigMap{}
	cm.Name = flannelPodCIDRConfigmapName
	cm.Kind = "ConfigMap"
	cm.APIVersion = "v1"
	_, err = ksm.client.CoreV1().ConfigMaps(ksm.podNamespace).Create(cm)
	if err != nil {
		glog.Errorf("create flannel pod cidr configmap error:%v", err)
	}
	return nil
}

func (ksm *kubeSubnetManager) patchNode(oldNode, newNode *v1.Node) error {
	oldData, err := json.Marshal(oldNode)
	if err != nil {
		return err
	}

	newData, err := json.Marshal(newNode)
	if err != nil {
		return err
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.Node{})
	if err != nil {
		return fmt.Errorf("failed to create patch for node %q: %v", ksm.nodeName, err)
	}

	_, err = ksm.client.CoreV1().Nodes().Patch(ksm.nodeName, types.StrategicMergePatchType, patchBytes, "status")
	if err != nil {
		return err
	}
	return nil
}

func (ksm *kubeSubnetManager) allocateSubnet(config *subnet.Config, ipnets []ip.IP4Net) (ip.IP4Net, error) {
	glog.Infof("Picking subnet in range %s ... %s", config.SubnetMin, config.SubnetMax)

	var bag []ip.IP4
	sn := ip.IP4Net{IP: config.SubnetMin, PrefixLen: config.SubnetLen}

OuterLoop:
	for ; sn.IP <= config.SubnetMax && len(bag) < 100; sn = sn.Next() {
		for _, ip := range ipnets {
			if sn.Overlaps(ip) {
				continue OuterLoop
			}
		}
		bag = append(bag, sn.IP)
	}

	if len(bag) == 0 {
		return ip.IP4Net{}, errors.New("out of subnets")
	}

	i := randInt(0, len(bag))
	return ip.IP4Net{IP: bag[i], PrefixLen: config.SubnetLen}, nil
}

func (ksm *kubeSubnetManager) allocCIDRToNode() error {
again:
	cachedNode, err := ksm.nodeStore.Get(ksm.nodeName)
	if err != nil {
		glog.Errorf("get cached node:%s error:%v", ksm.nodeName, err)
		return err
	}
	nobj, err := api.Scheme.DeepCopy(cachedNode)
	if err != nil {
		glog.Errorf("deep copy node error:%v", err)
		return err
	}
	n := nobj.(*v1.Node)

	cidr := nodeGetCIDR(n)
	if cidr != "" && !strings.HasPrefix(cidr, "-") {
		return nil
	}

	cm, err := ksm.client.CoreV1().ConfigMaps(ksm.podNamespace).Get(flannelPodCIDRConfigmapName, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		glog.Errorf("get flannel pod cidr configmap error:%v", err)
		return err
	}

	nodes, err := ksm.nodeStore.List(labels.Everything())
	if err != nil {
		glog.Errorf("list all nodes error:%v", err)
		return err
	}

	ipnets := []ip.IP4Net{}
	for _, n := range nodes {
		cidr := nodeGetCIDR(n)
		if cidr == "" {
			continue
		}
		if strings.HasPrefix(cidr, "-") {
			cidr = strings.TrimPrefix(cidr, "-")
		}
		_, ipnet, err := net.ParseCIDR(cidr)
		if err != nil {
			glog.Infof("parse CIDR %s error:%v", cidr, err)
		}
		ipnets = append(ipnets, ip.FromIPNet(ipnet))
	}

	config, _ := ksm.GetNetworkConfig(context.Background())
	ipnet, err := ksm.allocateSubnet(config, ipnets)
	if err != nil {
		glog.Errorf("allocate subnet error:%v", err)
		return err
	}

	n.Annotations[FlannelPodCIDRAnnotation] = "-" + ipnet.String()
	if err := ksm.patchNode(cachedNode, n); err != nil {
		glog.Errorf("patch node error:%v", err)
		return err
	}

	if _, err := ksm.client.CoreV1().ConfigMaps(ksm.podNamespace).Update(cm); err != nil {
		glog.Infof("update configmap error:%v", err)
		goto again
	}
	n.Annotations[FlannelPodCIDRAnnotation] = ipnet.String()
	if err := ksm.patchNode(cachedNode, n); err != nil {
		glog.Errorf("patch node error:%v", err)
		return err
	}
	return nil
}

var rnd *rand.Rand

func randInt(lo, hi int) int {
	return lo + int(rnd.Int31n(int32(hi-lo)))
}
