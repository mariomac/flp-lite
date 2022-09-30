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

package pipe

import (
	"github.com/mariomac/flplite/pkg/pipe/ingest"
	"github.com/mariomac/flplite/pkg/pipe/transform"
	"github.com/mariomac/flplite/pkg/pipe/write"
)

type ConfigFileStruct struct {
	LogLevel   string       `yaml:"log-level,omitempty" json:"log-level,omitempty"`
	Parameters []StageParam `yaml:"parameters,omitempty" json:"parameters,omitempty"`
	Profile    *Profile     `yaml:"profile,omitempty" json:"profile,omitempty"`
	Health     *Health      `yaml:"health,omitempty" json:"health,omitempty"`
}

type Health struct {
	Port int `yaml:"port,omitempty" json:"port,omitempty"`
}

type Profile struct {
	Port int
}

type StageParam struct {
	Name      string     `yaml:"name" json:"name"`
	Ingest    *Ingest    `yaml:"ingest,omitempty" json:"ingest,omitempty"`
	Transform *Transform `yaml:"transform,omitempty" json:"transform,omitempty"`
	Write     *Write     `yaml:"write,omitempty" json:"write,omitempty"`
}

type Ingest struct {
	Type  string              `yaml:"type" json:"type"`
	Kafka *ingest.KafkaConfig `yaml:"kafka,omitempty" json:"kafka,omitempty"`
}

type Transform struct {
	Type    string                   `yaml:"type" json:"type"`
	Network *transform.NetworkConfig `yaml:"network,omitempty" json:"network,omitempty"`
}

type Write struct {
	Type string            `yaml:"type" json:"type"`
	Loki *write.LokiConfig `yaml:"loki,omitempty" json:"loki,omitempty"`
}
