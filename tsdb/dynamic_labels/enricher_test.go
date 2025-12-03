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
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

type mockSeriesSet struct {
	storage.SeriesSet
	series []storage.Series
	idx    int
}

func (m *mockSeriesSet) Next() bool {
	m.idx++
	return m.idx < len(m.series)
}

func (m *mockSeriesSet) At() storage.Series {
	return m.series[m.idx]
}

type mockSeries struct {
	storage.Series
	lbls labels.Labels
}

func (m *mockSeries) Labels() labels.Labels {
	return m.lbls
}

func TestEnrichedSeriesSet(t *testing.T) {
	tmpDir := t.TempDir()
	filename := tmpDir + "/rules.yml"
	// Reuse content from rules_test
	content := `
dynamic_labels:
  - matchers:
      - '{zone="us-east-1a"}'
    labels:
      region: us-east-1
`
	require.NoError(t, os.WriteFile(filename, []byte(content), 0o666))
	provider, err := NewFileRuleProvider(filename)
	require.NoError(t, err)

	inputSeries := []storage.Series{
		&mockSeries{lbls: labels.FromStrings("zone", "us-east-1a", "__name__", "metric1")},
		&mockSeries{lbls: labels.FromStrings("zone", "us-west-1a", "__name__", "metric2")},
	}

	ms := &mockSeriesSet{series: inputSeries, idx: -1}
	es := NewEnrichedSeriesSet(ms, provider)

	require.True(t, es.Next())
	s := es.At()
	require.Equal(t, labels.FromStrings("region", "us-east-1", "zone", "us-east-1a", "__name__", "metric1"), s.Labels())

	require.True(t, es.Next())
	s = es.At()
	require.Equal(t, labels.FromStrings("zone", "us-west-1a", "__name__", "metric2"), s.Labels())

	require.False(t, es.Next())
}
