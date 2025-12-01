// Copyright 2024 The Prometheus Authors
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

package tsdb

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

// DynamicLabelRules stores dynamic label rules.
// Format: map[dynamicLabelName]map[dynamicLabelValue][]*labels.Matcher
// The first level key is the dynamic label name, the second level key is the
// dynamic label value, and the value is a list of matchers that define when
// this dynamic label name and value combination should be applied.
type DynamicLabelRules struct {
	// rules maps dynamic label name -> dynamic label value -> list of matchers
	rules map[string]map[string][]*labels.Matcher
	mu    sync.RWMutex
}

// NewDynamicLabelRules creates a new DynamicLabelRules instance.
func NewDynamicLabelRules() *DynamicLabelRules {
	return &DynamicLabelRules{
		rules: make(map[string]map[string][]*labels.Matcher),
	}
}

// SetRules replaces all existing rules with the provided rules.
// The rules parameter should follow the format:
// map[dynamicLabelName]map[dynamicLabelValue][]*labels.Matcher
func (d *DynamicLabelRules) SetRules(rules map[string]map[string][]*labels.Matcher) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Deep copy the rules to avoid external modifications
	d.rules = make(map[string]map[string][]*labels.Matcher, len(rules))
	for name, valueMap := range rules {
		d.rules[name] = make(map[string][]*labels.Matcher, len(valueMap))
		for value, matchers := range valueMap {
			// Copy the matchers slice
			matchersCopy := make([]*labels.Matcher, len(matchers))
			copy(matchersCopy, matchers)
			d.rules[name][value] = matchersCopy
		}
	}
}

// GetRulesForLabel returns all rules for a given dynamic label name.
// Returns nil if the label name has no rules.
func (d *DynamicLabelRules) GetRulesForLabel(name string) map[string][]*labels.Matcher {
	d.mu.RLock()
	defer d.mu.RUnlock()

	valueMap, exists := d.rules[name]
	if !exists {
		return nil
	}

	// Return a copy to avoid external modifications
	result := make(map[string][]*labels.Matcher, len(valueMap))
	for value, matchers := range valueMap {
		matchersCopy := make([]*labels.Matcher, len(matchers))
		copy(matchersCopy, matchers)
		result[value] = matchersCopy
	}
	return result
}

// GetRulesForLabelValue returns the matchers for a specific dynamic label name and value.
// Returns nil if no rules exist for this combination.
func (d *DynamicLabelRules) GetRulesForLabelValue(name, value string) []*labels.Matcher {
	d.mu.RLock()
	defer d.mu.RUnlock()

	valueMap, exists := d.rules[name]
	if !exists {
		return nil
	}

	matchers, exists := valueMap[value]
	if !exists {
		return nil
	}

	// Return a copy to avoid external modifications
	result := make([]*labels.Matcher, len(matchers))
	copy(result, matchers)
	return result
}

// FindMatchingRules finds all dynamic labels that should be applied to a series
// based on its intrinsic labels. Returns a map of dynamicLabelName -> dynamicLabelValue.
// If multiple rules match the same dynamic label name, the first match wins (first match in iteration order).
func (d *DynamicLabelRules) FindMatchingRules(lbls labels.Labels) map[string]string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make(map[string]string)

	// Iterate through all dynamic label names
	for dynamicName, valueMap := range d.rules {
		// Skip if we already have a match for this dynamic label name (first match wins)
		if _, exists := result[dynamicName]; exists {
			continue
		}

		// Iterate through all values for this dynamic label name
		for dynamicValue, matchers := range valueMap {
			// Check if all matchers match the series labels
			allMatch := true
			for _, matcher := range matchers {
				labelValue := lbls.Get(matcher.Name)
				if !matcher.Matches(labelValue) {
					allMatch = false
					break
				}
			}

			if allMatch {
				result[dynamicName] = dynamicValue
				// First match wins, so break after finding the first matching value
				break
			}
		}
	}

	return result
}

// FindSeriesForDynamicLabel finds all series IDs that match a dynamic label query.
// It returns postings for all series that have intrinsic labels matching the matchers
// associated with the given dynamic label name and value.
func (d *DynamicLabelRules) FindSeriesForDynamicLabel(ctx context.Context, ix IndexReader, name, value string) (index.Postings, error) {
	matchers := d.GetRulesForLabelValue(name, value)
	if matchers == nil {
		// No rules for this dynamic label name/value combination
		return index.EmptyPostings(), nil
	}

	// Use PostingsForMatchers to find series matching the rule's matchers
	return PostingsForMatchers(ctx, ix, matchers...)
}
