// Copyright 2018 The Prometheus Authors
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
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

func BenchmarkHeadStripeSeriesCreate(b *testing.B) {
	chunkDir, err := ioutil.TempDir("", "chunk_dir")
	require.NoError(b, err)
	defer func() {
		require.NoError(b, os.RemoveAll(chunkDir))
	}()
	// Put a series, select it. GC it and then access it.
	h, err := NewHead(nil, nil, nil, 1000, chunkDir, nil, chunks.DefaultWriteBufferSize, DefaultStripeSize, nil)
	require.NoError(b, err)
	defer h.Close()

	for i := 0; i < b.N; i++ {
		h.getOrCreate(uint64(i), labels.FromStrings("a", strconv.Itoa(i)))
	}
}

func BenchmarkHeadStripeSeriesCreateParallel(b *testing.B) {
	chunkDir, err := ioutil.TempDir("", "chunk_dir")
	require.NoError(b, err)
	defer func() {
		require.NoError(b, os.RemoveAll(chunkDir))
	}()
	// Put a series, select it. GC it and then access it.
	h, err := NewHead(nil, nil, nil, 1000, chunkDir, nil, chunks.DefaultWriteBufferSize, DefaultStripeSize, nil)
	require.NoError(b, err)
	defer h.Close()

	var count atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := count.Inc()
			h.getOrCreate(uint64(i), labels.FromStrings("a", strconv.Itoa(int(i))))
		}
	})
}

func BenchmarkHeadLabelValuesWithMatchers(b *testing.B) {
	head, _ := newTestHead(b, 1000, false)
	defer func() {
		require.NoError(b, head.Close())
	}()

	app := head.Appender(context.Background())

	metricCount := 1000000
	// In this loop we'll add <metricCount> metrics to the index.
	// The label "label1" will have 10 different values in total, the values are value1-value10,
	// each of these 10 values will be assigned to 10% of the metrics in consecutive blocks.
	// The first 90% of the metrics which we add will also have the label & value "label2=value1",
	// the last 10% of the metrics which we add will have the label & value "label2=value2".
	// This means the metrics where "label2=value1" will have the values "value1"-"value9" for label "label1",
	// but they won't have "label10".
	// Each metric will also get a unique value in "label3".
	for metricIdx := 0; metricIdx < metricCount; metricIdx++ {
		var label1Value, label2Value, label3Value string
		label3Value = fmt.Sprintf("value%d", metricIdx)
		label1Id := 100*metricIdx/metricCount + 1
		label1Value = fmt.Sprintf("value%d", label1Id)
		if label1Id < 10 {
			label2Value = "value1"
		} else {
			label2Value = "value2"
		}
		_, err := app.Add(labels.Labels{{Name: "label1", Value: label1Value}, {Name: "label2", Value: label2Value}, {Name: "label3", Value: label3Value}}, 100, 0)
		require.NoError(b, err)
	}
	require.NoError(b, app.Commit())

	headIdxReader := head.indexRange(0, 200)
	expectedValues := []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9"}

	b.ReportAllocs()
	b.ResetTimer()

	for benchIdx := 0; benchIdx < b.N; benchIdx++ {
		labelValues, err := headIdxReader.LabelValues("label1", labels.MustNewMatcher(labels.MatchEqual, "label2", "value1"))
		require.NoError(b, err)
		require.Equal(b, expectedValues, labelValues)
	}
}
