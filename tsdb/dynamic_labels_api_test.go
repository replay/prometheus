package tsdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/dynamic_labels"
	"github.com/stretchr/testify/require"
)

func TestDynamicLabelsAPI(t *testing.T) {
	tmpDir := t.TempDir()
	ruleFile := filepath.Join(tmpDir, "rules.yml")

	content := `
dynamic_labels:
  - matchers:
      - '{zone="us-east-1a"}'
    labels:
      region:
        set_value_static: us-east-1
      region_zone:
        set_value_from_prioritized_labels: ["zone"]
      joined:
        set_value_from_joined_labels:
          labels:
            - app
            - zone
          separator: "_"
`
	require.NoError(t, os.WriteFile(ruleFile, []byte(content), 0o666))

	rp, err := dynamic_labels.NewFileRuleProvider(ruleFile)
	require.NoError(t, err)
	defer rp.Stop()

	opts := DefaultHeadOptions()
	opts.ChunkDirRoot = tmpDir
	opts.RuleProvider = rp

	head, err := NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(t, err)
	defer head.Close()

	app := head.Appender(context.Background())
	// Add series matching the rule
	_, err = app.Append(0, labels.FromStrings("zone", "us-east-1a", "app", "foo", "__name__", "metric1"), 100, 1)
	require.NoError(t, err)
	// Add series NOT matching the rule
	_, err = app.Append(0, labels.FromStrings("zone", "eu-west-1b", "app", "bar", "__name__", "metric2"), 100, 1)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	q, err := NewBlockQuerier(NewRangeHead(head, 0, 200), 0, 200)
	require.NoError(t, err)
	defer q.Close()

	ctx := context.Background()

	// Test LabelNames (no matchers)
	// Should return intrinsic labels + dynamic labels (region, region_zone, joined)
	names, _, err := q.LabelNames(ctx, nil)
	require.NoError(t, err)
	require.Contains(t, names, "region")
	require.Contains(t, names, "region_zone")
	require.Contains(t, names, "joined")
	require.Contains(t, names, "zone")
	require.Contains(t, names, "app")

	// Test LabelNames (with matchers)
	// Match zone="us-east-1a" -> should have region
	names, _, err = q.LabelNames(ctx, nil, labels.MustNewMatcher(labels.MatchEqual, "zone", "us-east-1a"))
	require.NoError(t, err)
	require.Contains(t, names, "region")

	// Match zone="eu-west-1b" -> should NOT have region
	names, _, err = q.LabelNames(ctx, nil, labels.MustNewMatcher(labels.MatchEqual, "zone", "eu-west-1b"))
	require.NoError(t, err)
	require.NotContains(t, names, "region")

	// Test LabelValues for dynamic label "region"
	// Should return "us-east-1"
	vals, _, err := q.LabelValues(ctx, "region", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"us-east-1"}, vals)

	// Test LabelValues for dynamic label "region_zone" (template)
	// Should return "us-east-1a"
	vals, _, err = q.LabelValues(ctx, "region_zone", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"us-east-1a"}, vals)

	// Test LabelValues with matcher
	// zone="us-east-1a" -> region="us-east-1"
	vals, _, err = q.LabelValues(ctx, "region", nil, labels.MustNewMatcher(labels.MatchEqual, "zone", "us-east-1a"))
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"us-east-1"}, vals)

	// zone="eu-west-1b" -> region empty (should return empty list)
	vals, _, err = q.LabelValues(ctx, "region", nil, labels.MustNewMatcher(labels.MatchEqual, "zone", "eu-west-1b"))
	require.NoError(t, err)
	require.Empty(t, vals)

	// Test Select with dynamic matcher
	// region="us-east-1" -> metric1
	ss := q.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchEqual, "region", "us-east-1"))
	require.NoError(t, ss.Err())
	require.True(t, ss.Next())
	lbls := ss.At().Labels()
	require.Equal(t, "metric1", lbls.Get(labels.MetricName))
	require.False(t, ss.Next())

	// Test LabelValues for dynamic label "joined"
	// Should return "foo_us-east-1a"
	vals, _, err = q.LabelValues(ctx, "joined", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"foo_us-east-1a"}, vals)

	// Test Select with dynamic matcher for joined label
	// joined="foo_us-east-1a" -> metric1
	ss = q.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchEqual, "joined", "foo_us-east-1a"))
	require.NoError(t, ss.Err())
	require.True(t, ss.Next())
	lbls = ss.At().Labels()
	require.Equal(t, "metric1", lbls.Get(labels.MetricName))
	require.Equal(t, "foo_us-east-1a", lbls.Get("joined"))
	require.False(t, ss.Next())
}
