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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestFileRuleProvider(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "dynamic_labels.yml")

	content := `
dynamic_labels:
  - matchers:
      - '{zone="us-east-1a", cluster="prod"}'
    labels:
      region:
        set_value_static: us-east-1
  - matchers:
      - '{zone=~"eu-west-1.*"}'
    labels:
      region:
        set_value_static: eu-west-1
  - matchers:
      - '{zone="eu-central-0a"}'
      - '{zone="eu-central-0b"}'
    labels:
      region:
        set_value_static: eu-central-0
`
	err := os.WriteFile(filename, []byte(content), 0o666)
	require.NoError(t, err)

	provider, err := NewFileRuleProvider(filename)
	require.NoError(t, err)

	// Test GetRules
	rules := provider.GetRules()
	require.Len(t, rules, 3)
	// Check that each rule has the expected labels
	regionLabels := make(map[string]bool)
	for _, rule := range rules {
		if regionConfig, ok := rule.Labels["region"]; ok {
			if !regionConfig.IsTemplate {
				regionLabels[regionConfig.StaticValue] = true
			}
		}
	}
	require.True(t, regionLabels["us-east-1"])
	require.True(t, regionLabels["eu-west-1"])
	require.True(t, regionLabels["eu-central-0"])

	// Test GetDynamicLabelsForSeries
	cases := []struct {
		name     string
		labels   labels.Labels
		expected labels.Labels
	}{
		{
			name:     "match us-east-1",
			labels:   labels.FromStrings("zone", "us-east-1a", "cluster", "prod", "app", "foo"),
			expected: labels.FromStrings("region", "us-east-1", "zone", "us-east-1a", "cluster", "prod", "app", "foo"),
		},
		{
			name:     "match eu-west-1",
			labels:   labels.FromStrings("zone", "eu-west-1b", "app", "bar"),
			expected: labels.FromStrings("region", "eu-west-1", "zone", "eu-west-1b", "app", "bar"),
		},
		{
			name:     "no match",
			labels:   labels.FromStrings("zone", "us-west-1a", "app", "baz"),
			expected: labels.FromStrings("zone", "us-west-1a", "app", "baz"),
		},
		{
			name:     "partial match (missing cluster for us-east-1)",
			labels:   labels.FromStrings("zone", "us-east-1a", "app", "foo"),
			expected: labels.FromStrings("zone", "us-east-1a", "app", "foo"),
		},
		{
			name:     "partial match (test logical OR between matcher sets)",
			labels:   labels.FromStrings("zone", "eu-central-0b", "app", "foo"),
			expected: labels.FromStrings("region", "eu-central-0", "zone", "eu-central-0b", "app", "foo"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// enricher.EnrichLabels uses provider.GetDynamicLabelsForSeries
			// but we can test GetDynamicLabelsForSeries directly here first to see what it returns
			dl := provider.GetDynamicLabelsForSeries(tc.labels)

			// Now test EnrichLabels helper which merges them
			enriched := EnrichLabels(tc.labels, provider)
			require.Equal(t, tc.expected, enriched, "EnrichLabels mismatch")

			// Check that GetDynamicLabelsForSeries only returned the dynamic part
			if tc.expected.Len() > tc.labels.Len() {
				require.True(t, dl.Len() > 0)
			}
		})
	}

	// Clean up the watcher
	provider.Stop()
}

func TestParseMatchers(t *testing.T) {
	// This effectively tests parsing via the provider load test above,
	// but we can add specific edge cases here if needed.
}

func TestTemplateEvaluation(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "dynamic_labels.yml")

	content := `
dynamic_labels:
  - matchers:
      - '{instance=~"^localhost:90([0-9]{2})$"}'
    labels:
      abctest1:
        set_value_static: value1
      abctest2:
        set_value_from_labels: "${job}/${instance}"
`
	err := os.WriteFile(filename, []byte(content), 0o666)
	require.NoError(t, err)

	provider, err := NewFileRuleProvider(filename)
	require.NoError(t, err)
	defer provider.Stop()

	// Test template evaluation
	cases := []struct {
		name     string
		labels   labels.Labels
		expected labels.Labels
	}{
		{
			name:     "template evaluation with job and instance",
			labels:   labels.FromStrings("instance", "localhost:9090", "job", "prometheus", "app", "test"),
			expected: labels.FromStrings("abctest1", "value1", "abctest2", "prometheus/localhost:9090", "instance", "localhost:9090", "job", "prometheus", "app", "test"),
		},
		{
			name:     "template with missing label leaves pattern",
			labels:   labels.FromStrings("instance", "localhost:9090", "app", "test"),
			expected: labels.FromStrings("abctest1", "value1", "abctest2", "${job}/localhost:9090", "instance", "localhost:9090", "app", "test"),
		},
		{
			name:     "no match",
			labels:   labels.FromStrings("instance", "remote:9090", "job", "prometheus"),
			expected: labels.FromStrings("instance", "remote:9090", "job", "prometheus"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enriched := EnrichLabels(tc.labels, provider)
			require.Equal(t, tc.expected, enriched, "EnrichLabels mismatch")
		})
	}
}

func TestFileRuleProviderRuntimeReload(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "dynamic_labels.yml")

	// Initial content
	initialContent := `
dynamic_labels:
  - matchers:
      - '{zone="us-east-1a"}'
    labels:
      region:
        set_value_static: us-east-1
`
	err := os.WriteFile(filename, []byte(initialContent), 0o666)
	require.NoError(t, err)

	provider, err := NewFileRuleProvider(filename)
	require.NoError(t, err)
	defer provider.Stop()

	// Verify initial rules
	rules := provider.GetRules()
	require.Len(t, rules, 1)
	require.Equal(t, "us-east-1", rules[0].Labels["region"].StaticValue)
	require.False(t, rules[0].Labels["region"].IsTemplate)

	// Update the file with new rules
	updatedContent := `
dynamic_labels:
  - matchers:
      - '{zone="us-east-1a"}'
    labels:
      region:
        set_value_static: us-east-1
  - matchers:
      - '{zone=~"eu-west-1.*"}'
    labels:
      region:
        set_value_static: eu-west-1
  - matchers:
      - '{cluster="prod"}'
    labels:
      environment:
        set_value_static: production
`
	err = os.WriteFile(filename, []byte(updatedContent), 0o666)
	require.NoError(t, err)

	// Wait for the file watcher to detect the change and reload
	// The debounce is 100ms, so wait a bit longer
	time.Sleep(300 * time.Millisecond)

	// Verify updated rules
	rules = provider.GetRules()
	require.Len(t, rules, 3) // 3 rules total
	// Check that we have rules for both region and environment
	hasRegionEast := false
	hasRegionWest := false
	hasEnvProd := false
	for _, rule := range rules {
		if regionConfig, ok := rule.Labels["region"]; ok && !regionConfig.IsTemplate {
			if regionConfig.StaticValue == "us-east-1" {
				hasRegionEast = true
			} else if regionConfig.StaticValue == "eu-west-1" {
				hasRegionWest = true
			}
		}
		if envConfig, ok := rule.Labels["environment"]; ok && !envConfig.IsTemplate && envConfig.StaticValue == "production" {
			hasEnvProd = true
		}
	}
	require.True(t, hasRegionEast)
	require.True(t, hasRegionWest)
	require.True(t, hasEnvProd)

	// Test that the new rules are applied
	labels := labels.FromStrings("zone", "eu-west-1b", "app", "test")
	enriched := EnrichLabels(labels, provider)
	require.Equal(t, "eu-west-1", enriched.Get("region"))
}

func TestReverseEngineerTemplateValue(t *testing.T) {
	cases := []struct {
		name     string
		template string
		value    string
		expected map[string]string
	}{
		{
			name:     "simple template with two variables",
			template: "${job}/${instance}",
			value:    "job1/instance123",
			expected: map[string]string{
				"job":      "job1",
				"instance": "instance123",
			},
		},
		{
			name:     "template with three variables",
			template: "${job}/${instance}/${port}",
			value:    "prometheus/localhost:9090/9090",
			expected: map[string]string{
				"job":      "prometheus",
				"instance": "localhost:9090",
				"port":     "9090",
			},
		},
		{
			name:     "template with prefix and suffix literals",
			template: "prefix-${job}-suffix",
			value:    "prefix-prometheus-suffix",
			expected: map[string]string{
				"job": "prometheus",
			},
		},
		{
			name:     "template with special regex characters",
			template: "${job}.${instance}",
			value:    "prometheus.localhost:9090",
			expected: map[string]string{
				"job":      "prometheus",
				"instance": "localhost:9090",
			},
		},
		{
			name:     "value doesn't match template",
			template: "${job}/${instance}",
			value:    "job1",
			expected: nil,
		},
		{
			name:     "value with extra parts matches (interpreted as part of last variable)",
			template: "${job}/${instance}",
			value:    "job1/instance123/extra",
			expected: map[string]string{
				"job":      "job1",
				"instance": "instance123/extra",
			},
		},
		{
			name:     "empty value doesn't match",
			template: "${job}/${instance}",
			value:    "",
			expected: nil,
		},
		{
			name:     "single variable template",
			template: "${job}",
			value:    "prometheus",
			expected: map[string]string{
				"job": "prometheus",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the template structure for testing
			structure := parseTemplateStructure(tc.template)
			result := ReverseEngineerTemplateValue(&structure, tc.template, tc.value)
			if tc.expected == nil {
				require.Nil(t, result, "Expected nil but got %v", result)
			} else {
				require.Equal(t, tc.expected, result, "Reverse engineering mismatch")
			}
		})
	}
}
