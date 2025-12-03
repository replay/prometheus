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

package tsdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/dynamic_labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

// mockRuleProvider implements dynamic_labels.RuleProvider
type mockRuleProvider struct {
	rules []dynamic_labels.Rule
}

func (m *mockRuleProvider) GetRules() []dynamic_labels.Rule {
	return m.rules
}

func (m *mockRuleProvider) GetDynamicLabelsForSeries(seriesLabels labels.Labels) labels.Labels {
	// Not used in PostingsForMatchers
	return labels.Labels{}
}

func TestPostingsForMatchers_DynamicLabels(t *testing.T) {
	// Define rules: region="us-east-1" -> zone="us-east-1a" AND cluster="prod"
	//               region="eu-west-1" -> zone="eu-west-1b"
	rules := []dynamic_labels.Rule{
		{
			Matchers: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "zone", "us-east-1a"),
					labels.MustNewMatcher(labels.MatchEqual, "cluster", "prod"),
				},
			},
			Labels: map[string]string{
				"region": "us-east-1",
			},
		},
		{
			Matchers: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "zone", "eu-west-1b"),
				},
			},
			Labels: map[string]string{
				"region": "eu-west-1",
			},
		},
	}
	provider := &mockRuleProvider{rules: rules}

	// Mock index behavior:
	// Postings(zone="us-east-1a") -> [1, 2]
	// Postings(cluster="prod") -> [1, 3]
	// Postings(zone="eu-west-1b") -> [4]
	// Intersection(zone="us-east-1a", cluster="prod") -> [1]

	// We will mock the reader to return postings based on inputs.
	reader := &smartMockIndexReader{
		rules: provider,
		postings: map[string]map[string][]uint64{
			"zone": {
				"us-east-1a": {1, 2},
				"eu-west-1b": {4},
			},
			"cluster": {
				"prod": {1, 3},
			},
		},
	}

	ctx := context.Background()

	// Test 1: Query region="us-east-1"
	// Should translate to zone="us-east-1a" AND cluster="prod"
	// Result: [1]
	p, err := PostingsForMatchers(ctx, reader, labels.MustNewMatcher(labels.MatchEqual, "region", "us-east-1"))
	require.NoError(t, err)
	expected := []uint64{1}
	var actual []uint64
	for p.Next() {
		actual = append(actual, uint64(p.At()))
	}
	require.Equal(t, expected, actual)

	// Test 2: Query region="eu-west-1"
	// Should translate to zone="eu-west-1b"
	// Result: [4]
	p, err = PostingsForMatchers(ctx, reader, labels.MustNewMatcher(labels.MatchEqual, "region", "eu-west-1"))
	require.NoError(t, err)
	expected = []uint64{4}
	actual = nil
	for p.Next() {
		actual = append(actual, uint64(p.At()))
	}
	require.Equal(t, expected, actual)

	// Test 3: Query region != "us-east-1"
	// Should exclude series matching (zone="us-east-1a" AND cluster="prod")
	// We assume AllPostings is [1, 2, 3, 4, 5]
	reader.allPostings = []uint64{1, 2, 3, 4, 5}

	// (zone="us-east-1a" AND cluster="prod") -> [1]
	// AllPostings - [1] -> [2, 3, 4, 5]
	p, err = PostingsForMatchers(ctx, reader, labels.MustNewMatcher(labels.MatchNotEqual, "region", "us-east-1"))
	require.NoError(t, err)
	expected = []uint64{2, 3, 4, 5}
	actual = nil
	for p.Next() {
		actual = append(actual, uint64(p.At()))
	}
	require.Equal(t, expected, actual)
}

type smartMockIndexReader struct {
	IndexReader
	rules       dynamic_labels.RuleProvider
	postings    map[string]map[string][]uint64
	allPostings []uint64
}

func (m *smartMockIndexReader) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	if name == "" && (len(values) == 0 || (len(values) == 1 && values[0] == "")) { // AllPostings
		return index.NewListPostings(toStorageRefs(m.allPostings)), nil
	}

	var res []index.Postings
	for _, v := range values {
		if p, ok := m.postings[name][v]; ok {
			res = append(res, index.NewListPostings(toStorageRefs(p)))
		} else {
			res = append(res, index.EmptyPostings())
		}
	}
	return index.Merge(ctx, res...), nil
}

func (m *smartMockIndexReader) PostingsForLabelMatching(ctx context.Context, name string, match func(value string) bool) index.Postings {
	// Naive implementation for testing purposes
	var res []index.Postings
	if vals, ok := m.postings[name]; ok {
		for v, p := range vals {
			if match(v) {
				res = append(res, index.NewListPostings(toStorageRefs(p)))
			}
		}
	}
	return index.Merge(ctx, res...)
}

func (m *smartMockIndexReader) PostingsForAllLabelValues(ctx context.Context, name string) index.Postings {
	var res []index.Postings
	if vals, ok := m.postings[name]; ok {
		for _, p := range vals {
			res = append(res, index.NewListPostings(toStorageRefs(p)))
		}
	}
	return index.Merge(ctx, res...)
}

func (m *smartMockIndexReader) RuleProvider() dynamic_labels.RuleProvider {
	return m.rules
}

func toStorageRefs(refs []uint64) []storage.SeriesRef {
	res := make([]storage.SeriesRef, len(refs))
	for i, r := range refs {
		res[i] = storage.SeriesRef(r)
	}
	return res
}
