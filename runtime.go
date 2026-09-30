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

var (
	ErrClosed         = errors.New("runtime manager is closed")
	ErrAlreadyStarted = errors.New("runtime already started")
	ErrNotStarted     = errors.New("runtime is not started")
)

// Candidate is a fully prepared configuration version that has not yet been
// published. Its resources exist, but it is neither active nor persisted.
// A candidate must end with either Commit (publish) or Abort (discard); a
// failed Commit discards the candidate automatically.
type Candidate struct {
	manager   *Manager
	version   *version
	abandoned atomic.Bool
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

	candidate, err := m.Prepare(ctx, cfg)
	if err != nil {
		return err
	}
	return candidate.Commit(ctx, false)
}

func (m *Manager) Reload(ctx context.Context, cfg Config) error {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	candidate, err := m.Prepare(ctx, cfg)
	if err != nil {
		return err
	}
	return candidate.Commit(ctx, true)
}

// Prepare validates cfg and creates every resource it needs, producing a
// candidate that can later be committed, rolled back to, or abandoned.
// Preparation never touches the active version and never writes disk, so a
// failed preparation leaves both current state and the persisted record
// untouched. All resources created so far are released on failure.
func (m *Manager) Prepare(ctx context.Context, cfg Config) (*Candidate, error) {
	next, err := m.buildVersion(ctx, cloneConfig(cfg))
	if err != nil {
		return nil, err
	}
	return &Candidate{manager: m, version: next}, nil
}

// Commit publishes the candidate atomically with its persistence record.
//
// The persisted file is written first while holding the manager state lock;
// only a durable, successful write can activate the new version. Therefore:
//   - a duplicate Start, a closed manager, or a persistence failure never
//     overwrites the on-disk configuration of the currently running version;
//   - a process restart can only ever restore a configuration that was truly
//     published.
//
// On any failure the candidate's freshly prepared resources are abandoned.
// When requireActive is false the candidate performs an initial start; when
// it is true the candidate replaces an existing active version (reload).
func (c *Candidate) Commit(ctx context.Context, requireActive bool) error {
	if c == nil {
		return errors.New("commit of nil candidate")
	}
	if c.manager == nil || c.abandoned.Load() {
		return errors.New("candidate has already been committed or abandoned")
	}
	m := c.manager
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	activeExists := m.active.Load() != nil
	if m.closed {
		c.abortLocked(context.WithoutCancel(ctx))
		return ErrClosed
	}
	if !requireActive && activeExists {
		c.abortLocked(context.WithoutCancel(ctx))
		return ErrAlreadyStarted
	}
	if requireActive && !activeExists {
		c.abortLocked(context.WithoutCancel(ctx))
		return ErrNotStarted
	}
	// Persist while locked: a failed write (including duplicate-start style
	// races) can never leave a newer configuration on disk than in memory.
	if err := persistConfig(m.configPath, c.version.config); err != nil {
		c.abortLocked(context.WithoutCancel(ctx))
		return err
	}
	previous := m.active.Swap(c.version)
	c.version = nil
	c.abandoned.Store(true)
	if previous != nil {
		m.retireVersionLocked(previous)
	}
	return nil
}

// Abort discards a candidate that will never be published. Prepared
// resources are closed in reverse dependency order. It is safe to call once
// and after a successful Commit (the latter is a no-op).
func (c *Candidate) Abort(ctx context.Context) error {
	if c == nil || c.manager == nil {
		return nil
	}
	c.manager.stateMu.Lock()
	defer c.manager.stateMu.Unlock()
	return c.abortLocked(ctx)
}

func (c *Candidate) abortLocked(ctx context.Context) error {
	if c.abandoned.Swap(true) {
		return nil
	}
	if c.version == nil {
		return nil
	}
	v := c.version
	c.version = nil
	return closeResources(ctx, v)
}

// Rollback publishes the candidate as a recovery point: it replaces the
// currently active version like Reload, but it does not overwrite the
// persisted configuration. This is used to revert in-memory state to a
// configuration known to be durably stored when a newer candidate cannot be
// published. The previously active version is retired and its resources are
// kept alive until every in-flight lease is released.
func (m *Manager) Rollback(ctx context.Context, cfg Config) error {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	candidate, err := m.Prepare(ctx, cfg)
	if err != nil {
		return err
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.closed {
		_ = candidate.abortLocked(context.WithoutCancel(ctx))
		return ErrClosed
	}
	previous := m.active.Swap(candidate.version)
	candidate.version = nil
	if previous != nil {
		m.retireVersionLocked(previous)
	}
	return nil
}

func (m *Manager) Lease() (*Lease, error) {
	m.stateMu.RLock()
	if m.closed {
		m.stateMu.RUnlock()
		return nil, ErrClosed
	}
	v := m.active.Load()
	if v == nil {
		m.stateMu.RUnlock()
		return nil, ErrNotStarted
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

// retireVersionLocked registers v for asynchronous retirement. Resources stay
// open until every outstanding lease is released, then close in reverse
// dependency order. Caller must hold m.stateMu.
func (m *Manager) retireVersionLocked(v *version) {
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
