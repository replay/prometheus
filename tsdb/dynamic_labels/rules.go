// Copyright 2025 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dynamic_labels

import (
	"fmt"
	"os"
	"sync"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"gopkg.in/yaml.v2"
)

// RuleProvider provides access to dynamic label rules.
type RuleProvider interface {
	// GetRules returns all dynamic label rules.
	// The outer map key is the dynamic label name.
	// The inner map key is the dynamic label value.
	// The value is a list of matchers that a series must match to get the dynamic label.
	GetRules() map[string]map[string][]*labels.Matcher

	// GetDynamicLabelsForSeries returns the dynamic labels that should be added to the series
	// based on its intrinsic labels.
	GetDynamicLabelsForSeries(seriesLabels labels.Labels) labels.Labels
}

// FileRuleProvider implements RuleProvider using a YAML file.
type FileRuleProvider struct {
	mu    sync.RWMutex
	rules map[string]map[string][]*labels.Matcher
}

// NewFileRuleProvider creates a new FileRuleProvider.
func NewFileRuleProvider(filename string) (*FileRuleProvider, error) {
	p := &FileRuleProvider{
		rules: make(map[string]map[string][]*labels.Matcher),
	}

	if filename == "" {
		return p, nil
	}

	if err := p.Load(filename); err != nil {
		return nil, err
	}

	return p, nil
}

// Load loads rules from the given file.
func (p *FileRuleProvider) Load(filename string) error {
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("read dynamic labels file: %w", err)
	}

	var cfg config.DynamicLabelsConfig
	if err := yaml.UnmarshalStrict(content, &cfg); err != nil {
		return fmt.Errorf("parse dynamic labels file: %w", err)
	}

	rules := make(map[string]map[string][]*labels.Matcher)
	for labelName, valueRules := range cfg.DynamicLabels {
		rules[labelName] = make(map[string][]*labels.Matcher)
		for labelValue, matcherConfigs := range valueRules {
			matchers, err := ParseMatchers(matcherConfigs)
			if err != nil {
				return fmt.Errorf("parse matchers for dynamic label %q=%q: %w", labelName, labelValue, err)
			}
			rules[labelName][labelValue] = matchers
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = rules
	return nil
}

func (p *FileRuleProvider) GetRules() map[string]map[string][]*labels.Matcher {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.rules
}

func (p *FileRuleProvider) GetDynamicLabelsForSeries(seriesLabels labels.Labels) labels.Labels {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var builder labels.ScratchBuilder
	for name, values := range p.rules {
		for value, matchers := range values {
			matches := true
			for _, m := range matchers {
				val := seriesLabels.Get(m.Name)
				if !m.Matches(val) {
					matches = false
					break
				}
			}
			if matches {
				builder.Add(name, value)
			}
		}
	}
	builder.Sort()
	return builder.Labels()
}
