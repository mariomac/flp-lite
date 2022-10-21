/*
 * Copyright (C) 2021 IBM, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package transform

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/mariomac/flplite/pkg/flow"
	"github.com/mariomac/flplite/pkg/pipe/transform/kubernetes"
	"github.com/mariomac/flplite/pkg/pipe/transform/netdb"
	"github.com/mariomac/pipes/pkg/node"
	"github.com/sirupsen/logrus"
)

var log = logrus.WithField("component", "transform.Network")

func Network(cfg *NetworkConfig) (node.MiddleFunc[flow.Record, flow.Record], error) {
	nt, err := newTransformNetwork(cfg)
	if err != nil {
		return nil, fmt.Errorf("instantiating network transformer: %w", err)
	}
	return func(in <-chan flow.Record, out chan<- flow.Record) {
		log.Debug("starting network transformation loop")
		for flow := range in {
			out <- nt.transform(flow)
		}
		log.Debug("stopping network transformation loop")
	}, nil
}

type networkTransformer struct {
	kube     kubernetes.KubeData
	cfg      *NetworkConfig
	svcNames *netdb.ServiceNames
}

func (n *networkTransformer) transform(input flow.Record) flow.Record {
	out := input.Clone()


	// TODO: for efficiency and maintainability, maybe each case in the switch below should be an individual implementation of Transformer
	for _, rule := range n.cfg.Rules {
		switch rule.Type {
		case "add_subnet":
			_, ipv4Net, err := net.ParseCIDR(fmt.Sprintf("%v%s", out.Meta[rule.Input], rule.Parameters))
			if err != nil {
				log.Errorf("Can't find subnet for IP %v and prefix length %s - err %v", outputEntry[rule.Input], rule.Parameters, err)
				continue
			}
			outputEntry[rule.Output] = ipv4Net.String()
		case "add_service":
			protocol := fmt.Sprintf("%v", outputEntry[rule.Parameters])
			portNumber, err := strconv.Atoi(fmt.Sprintf("%v", outputEntry[rule.Input]))
			if err != nil {
				log.Errorf("Can't convert port to int: Port %v - err %v", outputEntry[rule.Input], err)
				continue
			}
			var serviceName string
			protocolAsNumber, err := strconv.Atoi(protocol)
			if err == nil {
				// protocol has been submitted as number
				serviceName = n.svcNames.ByPortAndProtocolNumber(portNumber, protocolAsNumber)
			} else {
				// protocol has been submitted as any string
				serviceName = n.svcNames.ByPortAndProtocolName(portNumber, protocol)
			}
			if serviceName == "" {
				if err != nil {
					log.Debugf("Can't find service name for Port %v and protocol %v - err %v", outputEntry[rule.Input], protocol, err)
					continue
				}
			}
			outputEntry[rule.Output] = serviceName
		case "add_kubernetes":
			kubeInfo, err := n.kube.GetInfo(fmt.Sprintf("%s", outputEntry[rule.Input]))
			if err != nil {
				log.Tracef("Can't find kubernetes info for IP %v err %v", outputEntry[rule.Input], err)
				continue
			}
			outputEntry[rule.Output+"_Namespace"] = kubeInfo.Namespace
			outputEntry[rule.Output+"_Name"] = kubeInfo.Name
			outputEntry[rule.Output+"_Type"] = kubeInfo.Type
			outputEntry[rule.Output+"_OwnerName"] = kubeInfo.Owner.Name
			outputEntry[rule.Output+"_OwnerType"] = kubeInfo.Owner.Type
			if rule.Parameters != "" {
				for labelKey, labelValue := range kubeInfo.Labels {
					outputEntry[rule.Parameters+"_"+labelKey] = labelValue
				}
			}
			if kubeInfo.HostIP != "" {
				outputEntry[rule.Output+"_HostIP"] = kubeInfo.HostIP
				if kubeInfo.HostName != "" {
					outputEntry[rule.Output+"_HostName"] = kubeInfo.HostName
				}
			}
		default:
			// TODO: this should be verified at instantiation time
			log.Panicf("unknown type %s for transform.Network rule: %v", rule.Type, rule)
		}
	}

	return outputEntry
}

// newTransformNetwork create a new transform
func newTransformNetwork(cfg *NetworkConfig) (*networkTransformer, error) {
	nt := networkTransformer{cfg: cfg}
	err := nt.kube.InitFromConfig(cfg.KubeConfigPath)
	if err != nil {
		return nil, err
	}

	pFilename, sFilename := cfg.GetServiceFiles()
	protos, err := os.Open(pFilename)
	if err != nil {
		return nil, fmt.Errorf("opening protocols file %q: %w", pFilename, err)
	}
	defer protos.Close()
	services, err := os.Open(sFilename)
	if err != nil {
		return nil, fmt.Errorf("opening services file %q: %w", sFilename, err)
	}
	defer services.Close()
	nt.svcNames, err = netdb.LoadServicesDB(protos, services)
	if err != nil {
		return nil, err
	}

	return &nt, nil
}
