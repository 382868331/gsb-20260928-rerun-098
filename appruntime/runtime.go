package appruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Config contains the raw configuration for every registered module, keyed by
// module name.
type Config map[string]json.RawMessage

// Module declares a functional module and the modules it depends on.
type Module interface {
	Name() string
	DependsOn() []string
	ValidateConfig(json.RawMessage) error
	PrepareResource(context.Context, json.RawMessage) (Resource, error)
}

// Resource is a live resource created from one configuration version.
type Resource interface {
	Close(context.Context) error
}

// ResourceFunc adapts a close function to the Resource interface.
type ResourceFunc func(context.Context) error

func (f ResourceFunc) Close(ctx context.Context) error {
	return f(ctx)
}

// Snapshot is an immutable view of one complete configuration version.
// Callers must invoke Done after using the snapshot.
type Snapshot struct {
	version   uint64
	resources map[string]Resource
	live      *liveVersion
	released  bool
}

// Resource returns the resource contained in this immutable snapshot.
func (s *Snapshot) Resource(name string) (Resource, bool) {
	if s == nil || s.resources == nil {
		return nil, false
	}
	r, ok := s.resources[name]
	return r, ok
}

// Version returns the one-based number of the active successful configuration.
func (s *Snapshot) Version() uint64 {
	if s == nil {
		return 0
	}
	return s.version
}

// Done releases the caller's reference to the snapshot. It is safe to call more
// than once.
func (s *Snapshot) Done() {
	if s == nil || s.live == nil {
		return
	}
	s.live.mu.Lock()
	if s.released {
		s.live.mu.Unlock()
		return
	}
	s.released = true
	if s.live.refs > 0 {
		s.live.refs--
	}
	if s.live.retired && s.live.refs == 0 {
		s.live.cond.Broadcast()
	}
	s.live.mu.Unlock()
}

type liveVersion struct {
	id        uint64
	resources map[string]Resource
	order     []string

	mu      sync.Mutex
	cond    *sync.Cond
	refs    int
	retired bool
}

func newLiveVersion(id uint64, resources map[string]Resource, order []string) *liveVersion {
	v := &liveVersion{id: id, resources: resources, order: order}
	v.cond = sync.NewCond(&v.mu)
	return v
}

func (v *liveVersion) acquire() *Snapshot {
	v.mu.Lock()
	v.refs++
	v.mu.Unlock()
	return &Snapshot{version: v.id, resources: v.resources, live: v}
}

func (v *liveVersion) release() {
	v.mu.Lock()
	if v.refs > 0 {
		v.refs--
	}
	if v.retired && v.refs == 0 {
		v.cond.Broadcast()
	}
	v.mu.Unlock()
}

func (v *liveVersion) markRetired() {
	v.mu.Lock()
	v.retired = true
	if v.refs == 0 {
		v.cond.Broadcast()
	}
	v.mu.Unlock()
}

func (v *liveVersion) waitUntilIdle() {
	v.mu.Lock()
	for v.refs > 0 {
		v.cond.Wait()
	}
	v.mu.Unlock()
}

// Framework starts, swaps, and shuts down registered modules in dependency
// order.
type Framework struct {
	modules map[string]Module

	reloadMu sync.Mutex
	stateMu  sync.RWMutex
	active   *liveVersion
	nextID   uint64
	closed   bool

	closeTasks sync.WaitGroup
}

// New creates a framework from uniquely named modules.
func New(modules []Module) (*Framework, error) {
	if len(modules) == 0 {
		return nil, errors.New("appruntime: at least one module is required")
	}
	f := &Framework{modules: make(map[string]Module, len(modules)), nextID: 1}
	for _, module := range modules {
		name := module.Name()
		if name == "" {
			return nil, errors.New("appruntime: module name cannot be empty")
		}
		if _, exists := f.modules[name]; exists {
			return nil, fmt.Errorf("appruntime: duplicate module %q", name)
		}
		f.modules[name] = module
	}
	return f, nil
}

// Current returns a reference to the complete active version. It returns nil
// before the first configuration has been started.
func (f *Framework) Current() *Snapshot {
	f.stateMu.RLock()
	active := f.active
	if active == nil {
		f.stateMu.RUnlock()
		return nil
	}
	snapshot := active.acquire()
	f.stateMu.RUnlock()
	return snapshot
}

// Reload validates and prepares the next version, then atomically publishes it.
// Any failure leaves the previous version active. Successful reloads are saved
// to path when path is not empty.
func (f *Framework) Reload(ctx context.Context, config Config, path string) error {
	f.reloadMu.Lock()
	defer f.reloadMu.Unlock()

	f.stateMu.RLock()
	closed := f.closed
	f.stateMu.RUnlock()
	if closed {
		return errors.New("appruntime: framework is shut down")
	}

	order, err := f.validate(config)
	if err != nil {
		return err
	}

	resources, prepared, err := f.prepare(ctx, config, order)
	if err != nil {
		return errors.Join(err, closeResourcesReverse(ctx, resources, prepared))
	}

	if path != "" {
		if err := writeConfig(path, config); err != nil {
			return errors.Join(err, closeResourcesReverse(ctx, resources, order))
		}
	}

	f.publish(resources, order)
	return nil
}

// Load starts the last successfully persisted configuration. It does not modify
// the saved file.
func (f *Framework) Load(ctx context.Context, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	f.reloadMu.Lock()
	defer f.reloadMu.Unlock()

	f.stateMu.RLock()
	closed := f.closed
	f.stateMu.RUnlock()
	if closed {
		return errors.New("appruntime: framework is shut down")
	}

	order, err := f.validate(config)
	if err != nil {
		return err
	}
	resources, prepared, err := f.prepare(ctx, config, order)
	if err != nil {
		return errors.Join(err, closeResourcesReverse(ctx, resources, prepared))
	}
	f.publish(resources, order)
	return nil
}

// Shutdown waits for all in-flight snapshots to become idle and closes all
// resources in reverse dependency order.
func (f *Framework) Shutdown(ctx context.Context) error {
	f.reloadMu.Lock()
	defer f.reloadMu.Unlock()

	f.stateMu.Lock()
	if f.closed {
		f.stateMu.Unlock()
		return errors.New("appruntime: framework is already shut down")
	}
	f.closed = true
	active := f.active
	f.active = nil
	f.stateMu.Unlock()

	done := make(chan struct{})
	go func() {
		if active != nil {
			active.markRetired()
			active.waitUntilIdle()
		}
		f.closeTasks.Wait()
		close(done)
	}()

	select {
	case <-done:
		if active == nil {
			return nil
		}
		return closeResourcesReverse(ctx, active.resources, active.order)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *Framework) validate(config Config) ([]string, error) {
	if len(config) != len(f.modules) {
		return nil, fmt.Errorf("appruntime: expected %d module configurations, got %d", len(f.modules), len(config))
	}
	for name := range f.modules {
		raw, ok := config[name]
		if !ok {
			return nil, fmt.Errorf("appruntime: missing configuration for module %q", name)
		}
		if !json.Valid(raw) {
			return nil, fmt.Errorf("appruntime: invalid JSON for module %q", name)
		}
		if err := f.modules[name].ValidateConfig(raw); err != nil {
			return nil, fmt.Errorf("validate %q: %w", name, err)
		}
	}
	return dependencyOrder(f.modules)
}

func (f *Framework) prepare(ctx context.Context, config Config, order []string) (map[string]Resource, []string, error) {
	resources := make(map[string]Resource, len(order))
	prepared := make([]string, 0, len(order))
	for _, name := range order {
		if ctx.Err() != nil {
			return resources, prepared, ctx.Err()
		}
		resource, err := f.modules[name].PrepareResource(ctx, config[name])
		if err != nil {
			return resources, prepared, fmt.Errorf("prepare %q: %w", name, err)
		}
		if resource == nil {
			return resources, prepared, fmt.Errorf("prepare %q: resource is nil", name)
		}
		resources[name] = resource
		prepared = append(prepared, name)
	}
	return resources, prepared, nil
}

func (f *Framework) publish(resources map[string]Resource, order []string) {
	f.stateMu.Lock()
	version := newLiveVersion(f.nextID, resources, order)
	f.nextID++
	old := f.active
	f.active = version
	f.stateMu.Unlock()

	if old == nil {
		return
	}
	old.markRetired()
	f.closeTasks.Add(1)
	go func() {
		defer f.closeTasks.Done()
		old.waitUntilIdle()
		_ = closeResourcesReverse(context.Background(), old.resources, old.order)
	}()
}

func dependencyOrder(modules map[string]Module) ([]string, error) {
	state := make(map[string]uint8, len(modules))
	var order []string
	var visit func(string, []string) error
	visit = func(name string, path []string) error {
		switch state[name] {
		case 1:
			return fmt.Errorf("module dependency cycle: %s", append(path, name))
		case 2:
			return nil
		}
		module, ok := modules[name]
		if !ok {
			return fmt.Errorf("module %q is not registered", name)
		}
		state[name] = 1
		dependencies := append([]string(nil), module.DependsOn()...)
		sort.Strings(dependencies)
		for _, dependency := range dependencies {
			if dependency == name {
				return fmt.Errorf("module %q cannot depend on itself", name)
			}
			if err := visit(dependency, append(path, name)); err != nil {
				return err
			}
		}
		state[name] = 2
		order = append(order, name)
		return nil
	}

	names := make([]string, 0, len(modules))
	for name := range modules {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := visit(name, []string{name}); err != nil {
			return nil, err
		}
	}
	return order, nil
}

func closeResourcesReverse(ctx context.Context, resources map[string]Resource, order []string) error {
	var errs []error
	for i := len(order) - 1; i >= 0; i-- {
		name := order[i]
		if resource, ok := resources[name]; ok {
			if err := resource.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("close %q: %w", name, err))
			}
		}
	}
	return errors.Join(errs...)
}

func writeConfig(path string, config Config) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("persist configuration: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("persist configuration: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".runtime-config-*.tmp")
	if err != nil {
		return fmt.Errorf("persist configuration: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("persist configuration: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("persist configuration: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("persist configuration: %w", err)
	}
	return nil
}
