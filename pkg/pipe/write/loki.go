/*
 * Copyright (C) 2022 IBM, Inc.
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

package write

import (
	"fmt"
	"math"
	"strings"
	"time"

	logAdapter "github.com/go-kit/kit/log/logrus"
	jsonIter "github.com/json-iterator/go"
	"github.com/mariomac/flplite/pkg/flow"
	"github.com/mariomac/pipes/pkg/node"
	"github.com/netobserv/loki-client-go/loki"
	"github.com/netobserv/loki-client-go/pkg/backoff"
	"github.com/netobserv/loki-client-go/pkg/urlutil"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"
)

var log = logrus.WithField("component", "write.Loki")
var (
	keyReplacer = strings.NewReplacer("/", "_", ".", "_", "-", "_")
)

type emitter interface {
	Handle(labels model.LabelSet, timestamp time.Time, record string) error
}

func Loki(cfg *LokiConfig) (node.TerminalFunc[*flow.Record], error) {
	log.Debug("instantiating Loki writer")
	lw, err := newWriteLoki(cfg)
	if err != nil {
		return nil, fmt.Errorf("instantiating loki writer: %w", err)
	}
	return func(in <-chan *flow.Record) {
		log.Debug("starting Loki writer loop")
		for flow := range in {
			if err := lw.ProcessRecord(flow); err != nil {
				log.WithError(err).Warn("processing/writing flow")
			}
		}
		log.Debug("exiting Loki writer loop")
	}, nil
}

// Loki record writer
type lokiWriter struct {
	lokiConfig     loki.Config
	cfg            *LokiConfig
	timestampScale float64
	saneLabels     map[string]model.LabelName
	client         emitter
	timeNow        func() time.Time
}

func buildLokiConfig(c *LokiConfig) (loki.Config, error) {
	batchWait, err := time.ParseDuration(c.BatchWait)
	if err != nil {
		return loki.Config{}, fmt.Errorf("failed in parsing BatchWait : %v", err)
	}

	timeout, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return loki.Config{}, fmt.Errorf("failed in parsing Timeout : %v", err)
	}

	minBackoff, err := time.ParseDuration(c.MinBackoff)
	if err != nil {
		return loki.Config{}, fmt.Errorf("failed in parsing MinBackoff : %v", err)
	}

	maxBackoff, err := time.ParseDuration(c.MaxBackoff)
	if err != nil {
		return loki.Config{}, fmt.Errorf("failed in parsing MaxBackoff : %v", err)
	}

	cfg := loki.Config{
		TenantID:  c.TenantID,
		BatchWait: batchWait,
		BatchSize: c.BatchSize,
		Timeout:   timeout,
		BackoffConfig: backoff.BackoffConfig{
			MinBackoff: minBackoff,
			MaxBackoff: maxBackoff,
			MaxRetries: c.MaxRetries,
		},
	}
	/* TODO: fix. By any reason, the following client config returns proxyconnect connection refused errors
	          "clientConfig": {
	            "proxy_url": null,
	            "tls_config": {
	              "insecure_skip_verify": false
	            },
	            "follow_redirects": false
	          },
		Strangely, this doesn't happen in flowlogs-pipeline, even if this code is mostly copied from there
	 */
	c.ClientConfig = nil

	if c.ClientConfig != nil {
		cfg.Client = *c.ClientConfig
	}
	var clientURL urlutil.URLValue
	err = clientURL.Set(strings.TrimSuffix(c.URL, "/") + "/loki/api/v1/push")
	if err != nil {
		return cfg, fmt.Errorf("failed to parse client URL: %w", err)
	}
	cfg.URL = clientURL
	return cfg, nil
}

// TODO: this can be split in a "Loki" transformer plus a "Loki" writer
func (l *lokiWriter) ProcessRecord(in *flow.Record) error {
	unlock := in.Lock()
	defer unlock()

	// Add static labels from config
	labels := model.LabelSet{}
	for k, v := range l.cfg.StaticLabels {
		labels[k] = v
	}
	l.addLabels(in, labels)

	// Remove labels and configured ignore list from record
	ignoreList := append(l.cfg.IgnoreList, l.cfg.Labels...)
	for _, label := range ignoreList {
		delete(in.Values, label)
	}

	js, err := jsonIter.ConfigDefault.Marshal(in.Values)
	if err != nil {
		return err
	}

	timestamp := l.extractTimestamp(in)
	return l.client.Handle(labels, timestamp, string(js))
}

func (l *lokiWriter) extractTimestamp(record *flow.Record) time.Time {
	if l.cfg.TimestampLabel == "" {
		return l.timeNow()
	}
	timestamp, ok := record.Values[string(l.cfg.TimestampLabel)]
	if !ok {
		log.WithField("timestampLabel", l.cfg.TimestampLabel).
			Warnf("Timestamp label not found in record. Using local time")
		return l.timeNow()
	}
	ft, ok := getFloat64(timestamp)
	if !ok {
		log.WithField(string(l.cfg.TimestampLabel), timestamp).
			Warnf("Invalid timestamp found: float64 expected but got %T. Using local time", timestamp)
		return l.timeNow()
	}
	if ft == 0 {
		log.WithField("timestampLabel", l.cfg.TimestampLabel).
			Warnf("Empty timestamp in record. Using local time")
		return l.timeNow()
	}

	tsNanos := int64(ft * l.timestampScale)
	return time.Unix(tsNanos/int64(time.Second), tsNanos%int64(time.Second))
}

func (l *lokiWriter) addLabels(record *flow.Record, labels model.LabelSet) {
	// Add non-static labels from record
	for _, label := range l.cfg.Labels {
		val, ok := record.Values[label]
		if !ok {
			continue
		}
		sanitized, ok := l.saneLabels[label]
		if !ok {
			continue
		}
		lv := model.LabelValue(fmt.Sprint(val))
		if !lv.IsValid() {
			log.WithFields(logrus.Fields{"key": label, "value": val}).
				Debug("Invalid label value. Ignoring it")
			continue
		}
		labels[sanitized] = lv
	}
}

func getFloat64(timestamp interface{}) (ft float64, ok bool) {
	switch i := timestamp.(type) {
	case float64:
		return i, true
	case float32:
		return float64(i), true
	case int64:
		return float64(i), true
	case int32:
		return float64(i), true
	case uint64:
		return float64(i), true
	case uint32:
		return float64(i), true
	case int:
		return float64(i), true
	default:
		log.Warnf("Type %T is not implemented for float64 conversion\n", i)
		return math.NaN(), false
	}
}

// newWriteLoki creates a Loki writer from configuration
func newWriteLoki(cfg *LokiConfig) (*lokiWriter, error) {
	// need to combine defaults with parameters that are provided in the config yaml file
	cfg.SetDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("the provided config is not valid: %w", err)
	}

	lokiConfig, err := buildLokiConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building loki config: %w", err)
	}
	client, err := loki.NewWithLogger(lokiConfig, logAdapter.NewLogger(log.WithField("module", "export/loki")))
	if err != nil {
		return nil, err
	}

	timestampScale, err := time.ParseDuration(cfg.TimestampScale)
	if err != nil {
		return nil, fmt.Errorf("cannot parse TimestampScale: %w", err)
	}

	// Sanitize label keys
	saneLabels := make(map[string]model.LabelName, len(cfg.Labels))
	for _, label := range cfg.Labels {
		sanitized := model.LabelName(keyReplacer.Replace(label))
		if sanitized.IsValid() {
			saneLabels[label] = sanitized
		} else {
			log.WithFields(logrus.Fields{"key": label, "sanitized": sanitized}).
				Debug("Invalid label. Ignoring it")
		}
	}

	return &lokiWriter{
		lokiConfig:     lokiConfig,
		cfg:            cfg,
		timestampScale: float64(timestampScale),
		saneLabels:     saneLabels,
		client:         client,
		timeNow:        time.Now,
	}, nil
}
