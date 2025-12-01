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

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
)

// ParseMatchers parses a list of LabelMatcherConfig into a list of labels.Matcher.
func ParseMatchers(configs []config.LabelMatcherConfig) ([]*labels.Matcher, error) {
	matchers := make([]*labels.Matcher, 0, len(configs))
	for _, cfg := range configs {
		var matchType labels.MatchType
		switch cfg.MatchType {
		case "", "=":
			matchType = labels.MatchEqual
		case "!=":
			matchType = labels.MatchNotEqual
		case "=~":
			matchType = labels.MatchRegexp
		case "!~":
			matchType = labels.MatchNotRegexp
		default:
			return nil, fmt.Errorf("invalid match type %q for matcher %s=%s", cfg.MatchType, cfg.Name, cfg.Value)
		}

		m, err := labels.NewMatcher(matchType, cfg.Name, cfg.Value)
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, m)
	}
	return matchers, nil
}
