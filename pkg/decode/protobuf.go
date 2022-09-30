package decode

import (
	"fmt"
	"net"
	"time"

	"github.com/mariomac/pipes/pkg/node"
	"github.com/netobserv/netobserv-ebpf-agent/pkg/pbflow"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

var log = logrus.WithField("component", "decode.Protobuf")

func Protobuf() node.MiddleFunc[[]byte, map[string]interface{}] {
	log.Debug("creating protobuf decoder")
	return func(in <-chan []byte, out chan<- map[string]interface{}) {
		log.Debug("starting protobuf decoder loop")
		for pbRaw := range in {
			record := pbflow.Record{}
			if err := proto.Unmarshal(pbRaw, &record); err != nil {
				log.WithError(err).Debug("can't unmarshall received protobuf flow. Ignoring")
				continue
			}
			out <- pbFlowToMap(&record)
		}
		log.Debug("exiting protobuf decoder loop")
	}
}

func pbFlowToMap(flow *pbflow.Record) map[string]interface{} {
	return map[string]interface{}{
		"FlowDirection":   int(flow.Direction.Number()),
		"Bytes":           flow.Bytes,
		"SrcAddr":         ipToStr(flow.Network.GetSrcAddr()),
		"DstAddr":         ipToStr(flow.Network.GetDstAddr()),
		"SrcMac":          macToStr(flow.DataLink.GetSrcMac()),
		"DstMac":          macToStr(flow.DataLink.GetDstMac()),
		"SrcPort":         flow.Transport.GetSrcPort(),
		"DstPort":         flow.Transport.GetDstPort(),
		"Etype":           flow.EthProtocol,
		"Packets":         flow.Packets,
		"Proto":           flow.Transport.GetProtocol(),
		"TimeFlowStartMs": flow.TimeFlowStart.AsTime().UnixMilli(),
		"TimeFlowEndMs":   flow.TimeFlowEnd.AsTime().UnixMilli(),
		"TimeReceived":    time.Now().Unix(),
		"Interface":       flow.Interface,
	}
}

func ipToStr(ip *pbflow.IP) string {
	if ip.GetIpv6() != nil {
		return net.IP(ip.GetIpv6()).String()
	} else {
		n := ip.GetIpv4()
		return fmt.Sprintf("%d.%d.%d.%d",
			byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
}

func macToStr(mac uint64) string {
	return fmt.Sprintf("%02X:%02X:%02X:%02X:%02X:%02X",
		uint8(mac>>40),
		uint8(mac>>32),
		uint8(mac>>24),
		uint8(mac>>16),
		uint8(mac>>8),
		uint8(mac))
}
