package ingest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mariomac/flplite/pkg/clients"
	"github.com/mariomac/pipes/pkg/node"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const (
	defaultBatchReadTimeout    = 1000 * time.Millisecond
	defaultKafkaCommitInterval = 500 * time.Millisecond
)

var log = logrus.WithField("component", "ingest.Kafka")

type KafkaConfig struct {
	Brokers           []string     `yaml:"brokers,omitempty" json:"brokers,omitempty" doc:"list of kafka broker addresses"`
	Topic             string       `yaml:"topic,omitempty" json:"topic,omitempty" doc:"kafka topic to listen on"`
	GroupId           string       `yaml:"groupid,omitempty" json:"groupid,omitempty" doc:"separate groupid for each consumer on specified topic"`
	GroupBalancers    []string     `yaml:"groupBalancers,omitempty" json:"groupBalancers,omitempty" doc:"list of balancing strategies (range, roundRobin, rackAffinity)"`
	StartOffset       string       `yaml:"startOffset,omitempty" json:"startOffset,omitempty" doc:"FirstOffset (least recent - default) or LastOffset (most recent) offset available for a partition"`
	BatchReadTimeout  int64        `yaml:"batchReadTimeout,omitempty" json:"batchReadTimeout,omitempty" doc:"how often (in milliseconds) to process input"`
	BatchMaxLen       int          `yaml:"batchMaxLen,omitempty" json:"batchMaxLen,omitempty" doc:"the number of accumulated flows before being forwarded for processing"`
	PullQueueCapacity int          `yaml:"pullQueueCapacity,omitempty" json:"pullQueueCapacity,omitempty" doc:"the capacity of the queue use to store pulled flows"`
	PullMaxBytes      int          `yaml:"pullMaxBytes,omitempty" json:"pullMaxBytes,omitempty" doc:"the maximum number of bytes being pulled from kafka"`
	CommitInterval    int64        `yaml:"commitInterval,omitempty" json:"commitInterval,omitempty" doc:"the interval (in milliseconds) at which offsets are committed to the broker.  If 0, commits will be handled synchronously."`
	TLS               *clients.TLS `yaml:"tls,omitempty" json:"tls" doc:"TLS client configuration (optional)"`
	ChannelBufLen     int          `yaml:"chanBufLen" json:"chanBufLen"`
}

func Kafka(cfg *KafkaConfig) (node.StartFuncCtx[[]byte], error) {
	log.Debug("creating kafka ingester")
	ki, err := createKafkaIngest(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating Kafka ingester: %w", err)
	}
	return ki.loop, nil
}

type kafkaIngester struct {
	readTimeout time.Duration
	reader      *kafkago.Reader
}

func createKafkaIngest(cfg *KafkaConfig) (*kafkaIngester, error) {
	ki := kafkaIngester{
		readTimeout: defaultBatchReadTimeout,
	}

	if cfg.BatchReadTimeout > 0 {
		ki.readTimeout = time.Duration(cfg.BatchReadTimeout) * time.Millisecond
	}

	readerConfig := kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupId,
		CommitInterval: defaultKafkaCommitInterval,
	}

	if cfg.PullQueueCapacity > 0 {
		readerConfig.QueueCapacity = cfg.PullQueueCapacity
	}

	if cfg.PullMaxBytes > 0 {
		readerConfig.MaxBytes = cfg.PullMaxBytes
	}

	switch cfg.StartOffset {
	case "FirstOffset", "":
		readerConfig.StartOffset = kafkago.FirstOffset
	case "LastOffset":
		readerConfig.StartOffset = kafkago.LastOffset
	default:
		return nil, fmt.Errorf("illegal value for StartOffset: %s", cfg.StartOffset)
	}

	for _, gb := range cfg.GroupBalancers {
		switch gb {
		case "range":
			readerConfig.GroupBalancers = append(readerConfig.GroupBalancers,
				&kafkago.RangeGroupBalancer{})
		case "roundRobin":
			readerConfig.GroupBalancers = append(readerConfig.GroupBalancers,
				&kafkago.RoundRobinGroupBalancer{})
		case "rackAffinity":
			readerConfig.GroupBalancers = append(readerConfig.GroupBalancers,
				&kafkago.RackAffinityGroupBalancer{})
		default:
			return nil, fmt.Errorf("invalid Group Balancer: %s", gb)
		}
	}

	if cfg.CommitInterval > 0 {
		readerConfig.CommitInterval = time.Duration(cfg.CommitInterval) * time.Millisecond
	}

	dialer := &kafkago.Dialer{
		Timeout:   kafkago.DefaultDialer.Timeout,
		DualStack: kafkago.DefaultDialer.DualStack,
	}
	if cfg.TLS != nil {
		tlsConfig, err := cfg.TLS.Build()
		if err != nil {
			return nil, err
		}
		dialer.TLS = tlsConfig
	}

	ki.reader = kafkago.NewReader(readerConfig)
	if ki.reader == nil {
		return nil, errors.New("can't create kafka reader")
	}

	return &ki, nil
}

func (ki *kafkaIngester) loop(ctx context.Context, out chan<- []byte) {
	log.Debug("starting Kafka Ingester loop")
	for {
		select {
		case <-ctx.Done():
			log.Debug("exiting Kafka Ingester loop")
			return
		default:
			msg, err := ki.reader.ReadMessage(ctx)
			if err != nil {
				log.WithError(err).Warn("reading message. Ignoring line")
				continue
			}
			out <- msg.Value
		}
	}
}
