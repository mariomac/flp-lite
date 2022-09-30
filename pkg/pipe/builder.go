package pipe

import (
	"errors"
	"fmt"

	"github.com/mariomac/flplite/pkg/pipe/decode"
	"github.com/mariomac/flplite/pkg/pipe/ingest"
	"github.com/mariomac/flplite/pkg/pipe/transform"
	"github.com/mariomac/flplite/pkg/pipe/write"
	"github.com/mariomac/pipes/pkg/node"
	"github.com/sirupsen/logrus"
)

const defaultStageBuffer = 50
var log = logrus.WithField("component", "pipe.Builder")

func Build(cfg *ConfigFileStruct) (*node.Start[[]byte], error) {
	ingesterBatchLength := 500

	// We keep using ConfigFileStruct for compatibility with FLP deployments
	// but we don't really care about all the possible combinations of FLP.
	// We assume the structure: kafka_ingest --> protobuf_decode --> network_enricher --> loki_write
	var ingester node.StartFuncCtx[[]byte]
	var enricher node.MiddleFunc[map[string]interface{}, map[string]interface{}]
	var writer node.TerminalFunc[map[string]interface{}]
	var err error
	for _, cfg := range cfg.Parameters {
		if cfg.Ingest != nil && cfg.Ingest.Kafka != nil {
			// shoddy piece of work: reuse current kafkaConsumerQueueCapacity variable
			// to setup channels' buffer
			if cfg.Ingest.Kafka.PullQueueCapacity > 0 {
				ingesterBatchLength = cfg.Ingest.Kafka.PullQueueCapacity
			}
			log.Debugf("found ingester config: %#v", cfg.Ingest.Kafka)
			if ingester, err = ingest.Kafka(cfg.Ingest.Kafka); err != nil {
				return nil, fmt.Errorf("instantiating ingester: %w", err)
			}
		}
		if cfg.Transform != nil && cfg.Transform.Network != nil {
			log.Debugf("found enricher config: %#v", cfg.Transform.Network)
			if enricher, err = transform.Network(cfg.Transform.Network); err != nil {
				return nil, fmt.Errorf("instantiating enricher: %w", err)
			}
		}
		if cfg.Write != nil && cfg.Write.Loki != nil {
			log.Debugf("found writer config: %#v", cfg.Write.Loki)
			if writer, err = write.Loki(cfg.Write.Loki); err != nil {
				return nil, fmt.Errorf("instantiating writer: %w", err)
			}
		}
	}
	if ingester == nil {
		return nil, errors.New("missing ingester configuration")
	}
	if enricher == nil {
		return nil, errors.New("missing enricher configuration")
	}
	if writer == nil {
		return nil, errors.New("missing writer configuration")
	}

	ingestNode := node.AsStartCtx(ingester)
	decodeNode := node.AsMiddle(decode.Protobuf(), node.ChannelBufferLen(ingesterBatchLength))
	enrichNode := node.AsMiddle(enricher, node.ChannelBufferLen(defaultStageBuffer))
	writeNode := node.AsTerminal(writer, node.ChannelBufferLen(defaultStageBuffer))

	ingestNode.SendsTo(decodeNode)
	decodeNode.SendsTo(enrichNode)
	enrichNode.SendsTo(writeNode)

	return ingestNode, nil
}
