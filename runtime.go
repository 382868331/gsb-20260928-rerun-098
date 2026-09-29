package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

type ModuleConfig struct {
	Name string
	Raw  json.RawMessage
}

func (c ModuleConfig) Decode(v any) error {
	if len(c.Raw) == 0 || string(c.Raw) == "null" {
		return nil
	}
	return json.Unmarshal(c.Raw, v)
}

type Config map[string]json.RawMessage

type Resource interface {
	Close(ctx context.Context) error
}

type Module interface {
	Name() string
	Dependencies() []string
	Validate(ctx context.Context, config ModuleConfig) error
	Prepare(ctx context.Context, config ModuleConfig) (Resource, error)
}

type Lease struct {
	version *version
	release func()
}

func (l *Lease) Resource(name string) Resource {
	if l == nil || l.version == nil {
		return nil
	}
	return l.version.resources[name]
}

func (l *Lease) Config(name string) ModuleConfig {
	if l == nil || l.version == nil {
		return ModuleConfig{Name: name}
	}
	return ModuleConfig{Name: name, Raw: l.version.config[name]}
}

func (l *Lease) Release() {
	if l != nil && l.release != nil {
		l.release()
		l.release = nil
	}
}

type version struct {
	config    Config
	resources map[string]Resource
	order     []string
	leases    sync.WaitGroup
}

type Manager struct {
	configPath string
	modules    map[string]Module
	order      []string

	stateMu   sync.RWMutex
	switchMu  sync.Mutex
	active    atomic.Pointer[version]
	retired   []*retiredVersion
	closed    bool
	closeOnce sync.Once
	closeDone chan struct{}
}

type retiredVersion struct {
	version *version
	done    chan error
}

func NewManager(configPath string, modules ...Module) (*Manager, error) {
	m := &Manager{
		configPath: configPath,
		modules:    make(map[string]Module, len(modules)),
		closeDone:  make(chan struct{}),
	}
	for _, module := range modules {
		name := module.Name()
		if name == "" {
			return nil, errors.New("module name must not be empty")
		}
		if _, exists := m.modules[name]; exists {
			return nil, fmt.Errorf("duplicate module %q", name)
		}
		m.modules[name] = module
		m.order = append(m.order, name)
	}
	if _, err := m.startupOrder(); err != nil {
		return nil, err
	}
	return m, nil
}

func Open(ctx context.Context, configPath string, modules ...Module) (*Manager, error) {
	m, err := NewManager(configPath, modules...)
	if err != nil {
		return nil, err
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		next, err := m.buildVersion(ctx, cfg)
		if err != nil {
			return nil, err
		}
		m.active.Store(next)
	}
	return m, nil
}

func (m *Manager) Start(ctx context.Context, cfg Config) error {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	next, err := m.buildVersion(ctx, cloneConfig(cfg))
	if err != nil {
		return err
	}
	if err := persistConfig(m.configPath, next.config); err != nil {
		_ = closeResources(context.WithoutCancel(ctx), next)
		return err
	}
	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		_ = closeResources(context.WithoutCancel(ctx), next)
		return errors.New("runtime manager is closed")
	}
	if m.active.Load() != nil {
		m.stateMu.Unlock()
		_ = closeResources(context.WithoutCancel(ctx), next)
		return errors.New("runtime already started")
	}
	m.active.Store(next)
	m.stateMu.Unlock()
	return nil
}

func (m *Manager) Reload(ctx context.Context, cfg Config) error {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	next, err := m.buildVersion(ctx, cloneConfig(cfg))
	if err != nil {
		return err
	}
	if err := persistConfig(m.configPath, next.config); err != nil {
		_ = closeResources(context.WithoutCancel(ctx), next)
		return err
	}
	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		_ = closeResources(context.WithoutCancel(ctx), next)
		return errors.New("runtime manager is closed")
	}
	previous := m.active.Swap(next)
	if previous != nil {
		m.retireVersion(previous)
	}
	m.stateMu.Unlock()
	return nil
}

func (m *Manager) Lease() (*Lease, error) {
	m.stateMu.RLock()
	if m.closed {
		m.stateMu.RUnlock()
		return nil, errors.New("runtime manager is closed")
	}
	v := m.active.Load()
	if v == nil {
		m.stateMu.RUnlock()
		return nil, errors.New("runtime is not started")
	}
	v.leases.Add(1)
	m.stateMu.RUnlock()
	return &Lease{version: v, release: v.leases.Done}, nil
}

func (m *Manager) ActiveConfig() (Config, bool) {
	v := m.active.Load()
	if v == nil {
		return nil, false
	}
	return cloneConfig(v.config), true
}

func (m *Manager) Close(ctx context.Context) error {
	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		<-m.closeDone
		return nil
	}
	m.closed = true
	active := m.active.Swap(nil)
	retired := append([]*retiredVersion(nil), m.retired...)
	m.retired = nil
	m.stateMu.Unlock()

	if active != nil {
		active.leases.Wait()
	}
	errCh := make(chan error, len(retired)+1)
	var wg sync.WaitGroup
	for _, item := range retired {
		wg.Add(1)
		go func(item *retiredVersion) {
			defer wg.Done()
			errCh <- <-item.done
		}(item)
	}
	if active != nil {
		errCh <- closeResources(ctx, active)
	}
	wg.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}
	m.closeOnce.Do(func() { close(m.closeDone) })
	return errors.Join(errs...)
}

func (m *Manager) startupOrder() ([]string, error) {
	index := make(map[string]int, len(m.order))
	for i, name := range m.order {
		index[name] = i
	}
	visited := make(map[string]uint8, len(m.order))
	ordered := make([]string, 0, len(m.order))
	var visit func(name string, path []string) error
	visit = func(name string, path []string) error {
		switch visited[name] {
		case 1:
			return fmt.Errorf("module dependency cycle: %s", joinNames(append(path, name)))
		case 2:
			return nil
		}
		visited[name] = 1
		dependencies := append([]string(nil), m.modules[name].Dependencies()...)
		sort.Slice(dependencies, func(i, j int) bool {
			return index[dependencies[i]] < index[dependencies[j]]
		})
		for _, dependency := range dependencies {
			if _, ok := m.modules[dependency]; !ok {
				return fmt.Errorf("module %q depends on missing module %q", name, dependency)
			}
			if err := visit(dependency, append(path, name)); err != nil {
				return err
			}
		}
		visited[name] = 2
		ordered = append(ordered, name)
		return nil
	}
	for _, name := range m.order {
		if err := visit(name, nil); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

func (m *Manager) buildVersion(ctx context.Context, cfg Config) (*version, error) {
	order, err := m.startupOrder()
	if err != nil {
		return nil, err
	}
	for _, name := range order {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := m.modules[name].Validate(ctx, ModuleConfig{Name: name, Raw: cfg[name]}); err != nil {
			return nil, fmt.Errorf("validate module %q: %w", name, err)
		}
	}
	v := &version{config: cloneConfig(cfg), resources: make(map[string]Resource, len(order)), order: order}
	for _, name := range order {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, closeResources(context.WithoutCancel(ctx), v))
		}
		resource, err := m.modules[name].Prepare(ctx, ModuleConfig{Name: name, Raw: cfg[name]})
		if err != nil {
			return nil, errors.Join(fmt.Errorf("prepare module %q: %w", name, err), closeResources(context.WithoutCancel(ctx), v))
		}
		if resource == nil {
			resource = noopResource{}
		}
		v.resources[name] = resource
	}
	return v, nil
}

func (m *Manager) retireVersion(v *version) {
	item := &retiredVersion{version: v, done: make(chan error, 1)}
	m.retired = append(m.retired, item)
	go func() {
		v.leases.Wait()
		m.stateMu.Lock()
		err := closeResources(context.Background(), item.version)
		remaining := m.retired[:0]
		for _, retired := range m.retired {
			if retired != item {
				remaining = append(remaining, retired)
			}
		}
		m.retired = remaining
		m.stateMu.Unlock()
		item.done <- err
	}()
}

type noopResource struct{}

func (noopResource) Close(context.Context) error { return nil }

func closeResources(ctx context.Context, v *version) error {
	var errs []error
	for i := len(v.order) - 1; i >= 0; i-- {
		name := v.order[i]
		if resource := v.resources[name]; resource != nil {
			if err := resource.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("close module %q: %w", name, err))
			}
		}
	}
	return errors.Join(errs...)
}

func loadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("load configuration: %w", err)
	}
	return cfg, nil
}

func persistConfig(path string, cfg Config) error {
	if path == "" {
		return errors.New("configuration path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	target, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".config-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, target)
}

func cloneConfig(cfg Config) Config {
	clone := make(Config, len(cfg))
	for name, raw := range cfg {
		clone[name] = append(json.RawMessage(nil), raw...)
	}
	return clone
}

func joinNames(names []string) string {
	if len(names) == 0 {
		return ""
	}
	result := names[0]
	for _, name := range names[1:] {
		result += " -> " + name
	}
	return result
}
