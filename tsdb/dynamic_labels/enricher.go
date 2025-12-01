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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// EnrichLabels enriches the given series labels with dynamic labels based on the rule provider.
func EnrichLabels(seriesLabels labels.Labels, ruleProvider RuleProvider) labels.Labels {
	if ruleProvider == nil {
		return seriesLabels
	}
	dynamicLabels := ruleProvider.GetDynamicLabelsForSeries(seriesLabels)
	if dynamicLabels.IsEmpty() {
		return seriesLabels
	}

	// Merge series labels and dynamic labels.
	// We prioritize dynamic labels if there is a conflict, although usually they should be distinct.
	// Or should we prioritize intrinsic labels? The plan says "enrich", which usually means adding.
	// If we assume dynamic labels are unique and don't conflict with intrinsic labels, we can just merge.
	// But safe implementation is using a builder.

	b := labels.NewScratchBuilder(seriesLabels.Len() + dynamicLabels.Len())
	seriesLabels.Range(func(l labels.Label) {
		b.Add(l.Name, l.Value)
	})
	dynamicLabels.Range(func(l labels.Label) {
		b.Add(l.Name, l.Value)
	})
	b.Sort()
	return b.Labels()
}

// EnrichedSeriesSet wraps a storage.SeriesSet and enriches the series with dynamic labels.
type EnrichedSeriesSet struct {
	storage.SeriesSet
	ruleProvider RuleProvider
}

// NewEnrichedSeriesSet creates a new EnrichedSeriesSet.
func NewEnrichedSeriesSet(set storage.SeriesSet, ruleProvider RuleProvider) *EnrichedSeriesSet {
	return &EnrichedSeriesSet{
		SeriesSet:    set,
		ruleProvider: ruleProvider,
	}
}

// At returns the current series with enriched labels.
func (s *EnrichedSeriesSet) At() storage.Series {
	series := s.SeriesSet.At()
	if s.ruleProvider == nil {
		return series
	}
	return &enrichedSeries{
		Series:       series,
		ruleProvider: s.ruleProvider,
	}
}

type enrichedSeries struct {
	storage.Series
	ruleProvider RuleProvider
}

func (s *enrichedSeries) Labels() labels.Labels {
	return EnrichLabels(s.Series.Labels(), s.ruleProvider)
}

// EnrichedChunkSeriesSet wraps a storage.ChunkSeriesSet and enriches the series with dynamic labels.
type EnrichedChunkSeriesSet struct {
	storage.ChunkSeriesSet
	ruleProvider RuleProvider
}

// NewEnrichedChunkSeriesSet creates a new EnrichedChunkSeriesSet.
func NewEnrichedChunkSeriesSet(set storage.ChunkSeriesSet, ruleProvider RuleProvider) *EnrichedChunkSeriesSet {
	return &EnrichedChunkSeriesSet{
		ChunkSeriesSet: set,
		ruleProvider:   ruleProvider,
	}
}

// At returns the current series with enriched labels.
func (s *EnrichedChunkSeriesSet) At() storage.ChunkSeries {
	series := s.ChunkSeriesSet.At()
	if s.ruleProvider == nil {
		return series
	}
	return &enrichedChunkSeries{
		ChunkSeries:  series,
		ruleProvider: s.ruleProvider,
	}
}

type enrichedChunkSeries struct {
	storage.ChunkSeries
	ruleProvider RuleProvider
}

func (s *enrichedChunkSeries) Labels() labels.Labels {
	return EnrichLabels(s.ChunkSeries.Labels(), s.ruleProvider)
}

// EnrichedGenericSeriesSet wraps a generic series set (interface{} equivalent in storage package mostly)
// However, we need to support the genericSeriesSet interface from storage/generic.go which is private.
// We can't easily wrap private interfaces from another package.
// But `tsdb` package uses `storage.SeriesSet` and `storage.ChunkSeriesSet` in its public API.
// `tsdb.blockQuerier` returns `storage.SeriesSet`.

// Helper for generic series set in tsdb package if needed.

