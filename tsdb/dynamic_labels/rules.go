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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
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
	mu       sync.RWMutex
	rules    map[string]map[string][]*labels.Matcher
	filename string
	watcher  *fsnotify.Watcher
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewFileRuleProvider creates a new FileRuleProvider and starts watching the file for changes.
func NewFileRuleProvider(filename string) (*FileRuleProvider, error) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &FileRuleProvider{
		rules:    make(map[string]map[string][]*labels.Matcher),
		filename: filename,
		ctx:      ctx,
		cancel:   cancel,
	}

	if filename == "" {
		return p, nil
	}

	if err := p.Load(filename); err != nil {
		cancel()
		return nil, err
	}

	// Start watching the file for changes
	if err := p.startWatching(); err != nil {
		cancel()
		return nil, fmt.Errorf("start watching file: %w", err)
	}

	return p, nil
}

// Load loads rules from the given file.
// If filename is different from the currently watched file, the watcher is not updated.
// Use Reload() or create a new FileRuleProvider to change the watched file.
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

// startWatching starts a goroutine that watches the file for changes and reloads rules automatically.
func (p *FileRuleProvider) startWatching() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create file watcher: %w", err)
	}
	p.watcher = watcher

	// Try to watch the file directly first
	if err := watcher.Add(p.filename); err != nil {
		// If watching the file fails, watch the directory instead
		// This handles cases where the file doesn't exist yet or the system doesn't support file watching
		dir := filepath.Dir(p.filename)
		if dir == "" {
			dir = "."
		}
		if err := watcher.Add(dir); err != nil {
			watcher.Close()
			return fmt.Errorf("add watch for file %q or directory %q: %w", p.filename, dir, err)
		}
	} else {
		// Also watch the directory to catch rename operations (common with editors)
		dir := filepath.Dir(p.filename)
		if dir != "" && dir != "." {
			// Ignore error if directory watch fails, file watch should still work
			_ = watcher.Add(dir)
		}
	}

	p.wg.Add(1)
	go p.watchLoop()

	return nil
}

// watchLoop runs in a goroutine and watches for file changes.
func (p *FileRuleProvider) watchLoop() {
	defer p.wg.Done()

	// Debounce rapid file changes
	var reloadTimer *time.Timer
	var reloadTimerMu sync.Mutex

	reload := func() {
		reloadTimerMu.Lock()
		defer reloadTimerMu.Unlock()

		if reloadTimer != nil {
			reloadTimer.Stop()
		}

		// Debounce: wait 100ms after the last change before reloading
		reloadTimer = time.AfterFunc(100*time.Millisecond, func() {
			if err := p.Load(p.filename); err != nil {
				// Log error but don't stop watching
				// In a real implementation, we might want to use a logger here
				// For now, we'll silently continue watching
				_ = err
			}
		})
	}

	for {
		select {
		case <-p.ctx.Done():
			reloadTimerMu.Lock()
			if reloadTimer != nil {
				reloadTimer.Stop()
			}
			reloadTimerMu.Unlock()
			return

		case event, ok := <-p.watcher.Events:
			if !ok {
				return
			}

			// fsnotify sometimes sends events without name or operation - filter them out
			if len(event.Name) == 0 {
				continue
			}

			// Only reload on write/rename/create events, not on chmod
			if event.Op&fsnotify.Chmod == event.Op {
				continue
			}

			// Check if the event is for our file
			// We watch the directory, so we need to check if the event is specifically for our file
			eventPath, err := filepath.Abs(event.Name)
			if err != nil {
				continue
			}
			targetPath, err := filepath.Abs(p.filename)
			if err != nil {
				continue
			}

			// For rename events (common when editors save), check if the target is our file
			if event.Op&fsnotify.Rename != 0 {
				// After a rename, the file might have been moved to our target
				// Give it a moment and check if our file exists and was modified
				time.Sleep(50 * time.Millisecond)
				if _, err := os.Stat(p.filename); err == nil {
					// Check if this rename event might be related to our file
					// (e.g., editor writes to temp file then renames to our file)
					reload()
				}
			} else if eventPath == targetPath {
				// For write/create events, reload if it's our file
				reload()
			}

		case err, ok := <-p.watcher.Errors:
			if !ok {
				return
			}
			// Log error but continue watching
			// In a real implementation, we might want to use a logger here
			_ = err
		}
	}
}

// Stop stops watching the file and cleans up resources.
func (p *FileRuleProvider) Stop() {
	p.cancel()
	if p.watcher != nil {
		p.watcher.Close()
	}
	p.wg.Wait()
}
