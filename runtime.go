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
	"time"
)

var (
	// ErrBaselineChanged indicates that a candidate was prepared from a base
	// revision which is no longer active. The candidate must be abandoned and
	// re-prepared against the current revision; it can never overwrite a newer
	// publish.
	ErrBaselineChanged = errors.New("candidate baseline is no longer active")
	ErrCandidateClosed = errors.New("candidate is no longer open")
	ErrRevisionMissing = errors.New("revision is not part of the release history")
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
	revision  int64
	config    Config
	resources map[string]Resource
	order     []string
	leases    sync.WaitGroup
}

const (
	candidatePrepared  = "prepared"
	candidatePublished = "published"
	candidateAbandoned = "abandoned"
)

// Candidate is a fully prepared configuration that has not yet been published.
// Its resources stay alive until Publish, Abandon or Manager.Close, but a
// prepared candidate never becomes active on its own.
type Candidate struct {
	manager  *Manager
	id       string
	baseline int64
	version  *version
	created  time.Time

	mu    sync.Mutex
	state string
}

func (c *Candidate) ID() string {
	return c.id
}

// Baseline is the revision that was active when the candidate was prepared.
func (c *Candidate) Baseline() int64 {
	return c.baseline
}

func (c *Candidate) Config() Config {
	return cloneConfig(c.version.config)
}

func (c *Candidate) CreatedAt() time.Time {
	return c.created
}

func (c *Candidate) Status() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// Publish switches the active version to the candidate. It fails with
// ErrBaselineChanged when another publish has moved the active revision since
// the candidate was prepared; the candidate and its resources stay intact in
// that case so the caller can inspect them before calling Abandon.
func (c *Candidate) Publish(ctx context.Context) error {
	m := c.manager
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	c.mu.Lock()
	if c.state != candidatePrepared {
		state := c.state
		c.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrCandidateClosed, state)
	}
	c.mu.Unlock()

	if err := ctx.Err(); err != nil {
		m.recordAttempt("publish", c.id, c.baseline, 0, 0, false, "failed", err)
		return err
	}
	if m.configPath == "" {
		err := errors.New("configuration path is empty")
		m.recordAttempt("publish", c.id, c.baseline, 0, 0, false, "failed", err)
		return err
	}

	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		err := errors.New("runtime manager is closed")
		m.recordAttempt("publish", c.id, c.baseline, 0, 0, false, "failed", err)
		return err
	}
	active := m.active.Load()
	activeRevision := int64(0)
	if active != nil {
		activeRevision = active.revision
	}
	if activeRevision != c.baseline {
		m.stateMu.Unlock()
		err := fmt.Errorf("%w: candidate baseline %d, current revision %d", ErrBaselineChanged, c.baseline, activeRevision)
		m.recordAttempt("publish", c.id, c.baseline, activeRevision, 0, false, "conflict", err)
		return err
	}
	m.stateMu.Unlock()

	if err := persistConfig(m.configPath, c.version.config); err != nil {
		c.markAbandoned()
		_ = c.closeResources(context.WithoutCancel(ctx))
		m.forgetCandidate(c)
		m.recordAttempt("publish", c.id, c.baseline, activeRevision, 0, false, "failed", err)
		return err
	}

	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		err := errors.New("runtime manager is closed")
		c.markAbandoned()
		_ = c.closeResources(context.WithoutCancel(ctx))
		m.forgetCandidate(c)
		m.recordAttempt("publish", c.id, c.baseline, activeRevision, 0, false, "failed", err)
		return err
	}
	current := m.active.Load()
	currentRevision := int64(0)
	if current != nil {
		currentRevision = current.revision
	}
	if currentRevision != c.baseline {
		m.stateMu.Unlock()
		err := fmt.Errorf("%w: candidate baseline %d, current revision %d", ErrBaselineChanged, c.baseline, currentRevision)
		m.recordAttempt("publish", c.id, c.baseline, currentRevision, 0, false, "conflict", err)
		return err
	}
	m.revisionSeq++
	revision := m.revisionSeq
	c.version.revision = revision
	m.active.Store(c.version)
	if current != nil {
		m.retireVersion(current)
	}
	m.appendHistory(ReleaseInfo{
		Revision:    revision,
		Config:      cloneConfig(c.version.config),
		PublishedAt: time.Now().UTC(),
		Source:      "publish",
	})
	c.mu.Lock()
	c.state = candidatePublished
	c.mu.Unlock()
	delete(m.candidates, c.id)
	m.stateMu.Unlock()

	m.recordAttempt("publish", c.id, c.baseline, activeRevision, revision, true, "switched", nil)
	return nil
}

// Abandon releases every resource owned by the candidate. The active version is
// never touched. Abandon is idempotent and works after a failed Publish.
func (c *Candidate) Abandon(ctx context.Context) error {
	c.mu.Lock()
	if c.state != candidatePrepared {
		state := c.state
		c.mu.Unlock()
		if state == candidateAbandoned {
			return nil
		}
		return fmt.Errorf("%w: %s", ErrCandidateClosed, state)
	}
	c.state = candidateAbandoned
	c.mu.Unlock()

	err := c.closeResources(ctx)
	m := c.manager
	m.forgetCandidate(c)
	m.recordAttempt("abandon", c.id, c.baseline, 0, 0, false, "abandoned", err)
	return err
}

func (c *Candidate) closeResources(ctx context.Context) error {
	c.mu.Lock()
	version := c.version
	c.mu.Unlock()
	return closeResources(ctx, version)
}

func (c *Candidate) markAbandoned() {
	c.mu.Lock()
	c.state = candidateAbandoned
	c.mu.Unlock()
}

// CandidateInfo describes a prepared but not yet published candidate.
type CandidateInfo struct {
	ID        string
	Baseline  int64
	Config    Config
	CreatedAt time.Time
}

// ReleaseInfo describes one revision that actually became active.
type ReleaseInfo struct {
	Revision    int64
	Config      Config
	PublishedAt time.Time
	Source      string
}

// AttemptRecord is an audit entry for one real switch or one attempt that did
// not take effect (validation/preparation failure, baseline conflict,
// persistence failure, explicit abandon).
type AttemptRecord struct {
	Time         time.Time
	Kind         string
	CandidateID  string
	Baseline     int64
	FromRevision int64
	ToRevision   int64
	Switched     bool
	Result       string
	Err          string
}

type Manager struct {
	configPath string
	modules    map[string]Module
	order      []string

	stateMu      sync.RWMutex
	switchMu     sync.Mutex
	active       atomic.Pointer[version]
	retired      []*retiredVersion
	candidates   map[string]*Candidate
	revisionSeq  int64
	history      []ReleaseInfo
	attempts     []AttemptRecord
	candidateSeq int64
	closed       bool
	closeOnce    sync.Once
	closeDone    chan struct{}
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
		candidates: make(map[string]*Candidate),
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
		v, err := m.prepareResources(ctx, cfg)
		if err != nil {
			return nil, err
		}
		m.stateMu.Lock()
		m.revisionSeq = 1
		v.revision = 1
		m.active.Store(v)
		m.appendHistory(ReleaseInfo{
			Revision:    1,
			Config:      cloneConfig(v.config),
			PublishedAt: time.Now().UTC(),
			Source:      "recover",
		})
		m.stateMu.Unlock()
	}
	return m, nil
}

func (m *Manager) Start(ctx context.Context, cfg Config) error {
	base := m.currentRevision()
	if base != 0 {
		return errors.New("runtime already started")
	}
	candidate, err := m.Prepare(ctx, cfg)
	if err != nil {
		return err
	}
	if err := candidate.Publish(ctx); err != nil {
		_ = candidate.Abandon(context.WithoutCancel(ctx))
		if errors.Is(err, ErrBaselineChanged) {
			return errors.New("runtime already started")
		}
		return err
	}
	return nil
}

func (m *Manager) Reload(ctx context.Context, cfg Config) error {
	base := m.currentRevision()
	if base == 0 {
		return errors.New("runtime is not started")
	}
	candidate, err := m.Prepare(ctx, cfg)
	if err != nil {
		return err
	}
	if err := candidate.Publish(ctx); err != nil {
		_ = candidate.Abandon(context.WithoutCancel(ctx))
		return err
	}
	return nil
}

// Prepare validates and prepares a new candidate without publishing it. The
// candidate is pinned to the revision currently active; resources created by a
// failed preparation attempt are released before the error is returned.
func (m *Manager) Prepare(ctx context.Context, cfg Config) (*Candidate, error) {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	if ctx.Err() != nil {
		m.recordAttempt("prepare", "", m.currentRevision(), 0, 0, false, "failed", ctx.Err())
		return nil, ctx.Err()
	}
	if m.configPath == "" {
		err := errors.New("configuration path is empty")
		m.recordAttempt("prepare", "", 0, 0, 0, false, "failed", err)
		return nil, err
	}
	m.stateMu.RLock()
	closed := m.closed
	m.stateMu.RUnlock()
	if closed {
		err := errors.New("runtime manager is closed")
		m.recordAttempt("prepare", "", m.currentRevision(), 0, 0, false, "failed", err)
		return nil, err
	}

	baseline := m.currentRevision()
	version, err := m.prepareResources(ctx, cfg)
	if err != nil {
		m.recordAttempt("prepare", "", baseline, 0, 0, false, "failed", err)
		return nil, err
	}
	candidate := &Candidate{
		manager:  m,
		baseline: baseline,
		version:  version,
		created:  time.Now().UTC(),
		state:    candidatePrepared,
	}
	m.stateMu.Lock()
	m.candidateSeq++
	candidate.id = fmt.Sprintf("candidate-%d", m.candidateSeq)
	if m.closed {
		m.stateMu.Unlock()
		abandonErr := closeResources(context.WithoutCancel(ctx), version)
		err := errors.Join(errors.New("runtime manager is closed"), abandonErr)
		m.recordAttempt("prepare", candidate.id, baseline, 0, 0, false, "failed", err)
		return nil, err
	}
	m.candidates[candidate.id] = candidate
	m.stateMu.Unlock()
	m.recordAttempt("prepare", candidate.id, baseline, 0, 0, false, "prepared", nil)
	return candidate, nil
}

// Rollback prepares and publishes the configuration of a past revision, pinned
// to the currently active revision. It performs a real switch and therefore
// consumes a new revision number; the old revision stays in history.
func (m *Manager) Rollback(ctx context.Context, revision int64) error {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	info, ok := m.releaseInfo(revision)
	if !ok {
		err := fmt.Errorf("%w: %d", ErrRevisionMissing, revision)
		m.recordAttempt("rollback", "", m.currentRevision(), 0, 0, false, "failed", err)
		return err
	}
	if m.closed {
		err := errors.New("runtime manager is closed")
		m.recordAttempt("rollback", "", m.currentRevision(), 0, 0, false, "failed", err)
		return err
	}
	baseline := m.currentRevision()
	if baseline == 0 {
		err := errors.New("runtime is not started")
		m.recordAttempt("rollback", "", 0, 0, 0, false, "failed", err)
		return err
	}
	version, err := m.prepareResources(ctx, info.Config)
	if err != nil {
		m.recordAttempt("rollback", "", baseline, 0, 0, false, "failed", err)
		return err
	}
	if err := persistConfig(m.configPath, version.config); err != nil {
		_ = closeResources(context.WithoutCancel(ctx), version)
		m.recordAttempt("rollback", "", baseline, 0, 0, false, "failed", err)
		return err
	}
	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		_ = closeResources(context.WithoutCancel(ctx), version)
		err := errors.New("runtime manager is closed")
		m.recordAttempt("rollback", "", baseline, 0, 0, false, "failed", err)
		return err
	}
	current := m.active.Load()
	if current == nil || current.revision != baseline {
		m.stateMu.Unlock()
		_ = closeResources(context.WithoutCancel(ctx), version)
		err := ErrBaselineChanged
		m.recordAttempt("rollback", "", baseline, m.currentRevision(), 0, false, "conflict", err)
		return err
	}
	m.revisionSeq++
	newRevision := m.revisionSeq
	version.revision = newRevision
	m.active.Store(version)
	m.retireVersion(current)
	m.appendHistory(ReleaseInfo{
		Revision:    newRevision,
		Config:      cloneConfig(version.config),
		PublishedAt: time.Now().UTC(),
		Source:      fmt.Sprintf("rollback:r%d", revision),
	})
	m.stateMu.Unlock()
	m.recordAttempt("rollback", "", baseline, baseline, newRevision, true, "switched", nil)
	return nil
}

// Candidates returns all prepared candidates that are still open.
func (m *Manager) Candidates() []CandidateInfo {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	infos := make([]CandidateInfo, 0, len(m.candidates))
	for _, candidate := range m.candidates {
		infos = append(infos, CandidateInfo{
			ID:        candidate.id,
			Baseline:  candidate.baseline,
			Config:    candidate.Config(),
			CreatedAt: candidate.created,
		})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].ID < infos[j].ID })
	return infos
}

// History returns every revision that actually became active, oldest first.
func (m *Manager) History() []ReleaseInfo {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	history := make([]ReleaseInfo, len(m.history))
	for i, info := range m.history {
		history[i] = ReleaseInfo{
			Revision:    info.Revision,
			Config:      cloneConfig(info.Config),
			PublishedAt: info.PublishedAt,
			Source:      info.Source,
		}
	}
	return history
}

// Attempts returns the audit trail of real switches and attempts that did not
// take effect, oldest first.
func (m *Manager) Attempts() []AttemptRecord {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return append([]AttemptRecord(nil), m.attempts...)
}

func (m *Manager) currentRevision() int64 {
	v := m.active.Load()
	if v == nil {
		return 0
	}
	return v.revision
}

func (m *Manager) forgetCandidate(candidate *Candidate) {
	m.stateMu.Lock()
	delete(m.candidates, candidate.id)
	m.stateMu.Unlock()
}

func (m *Manager) releaseInfo(revision int64) (ReleaseInfo, bool) {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	for _, info := range m.history {
		if info.Revision == revision {
			return ReleaseInfo{
				Revision:    info.Revision,
				Config:      cloneConfig(info.Config),
				PublishedAt: info.PublishedAt,
				Source:      info.Source,
			}, true
		}
	}
	return ReleaseInfo{}, false
}

// appendHistory must be called with stateMu held.
func (m *Manager) appendHistory(info ReleaseInfo) {
	m.history = append(m.history, info)
}

func (m *Manager) recordAttempt(kind, candidateID string, baseline, fromRevision, toRevision int64, switched bool, result string, err error) {
	record := AttemptRecord{
		Time:         time.Now().UTC(),
		Kind:         kind,
		CandidateID:  candidateID,
		Baseline:     baseline,
		FromRevision: fromRevision,
		ToRevision:   toRevision,
		Switched:     switched,
		Result:       result,
	}
	if err != nil {
		record.Err = err.Error()
	}
	m.stateMu.Lock()
	m.attempts = append(m.attempts, record)
	m.stateMu.Unlock()
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
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	m.stateMu.Lock()
	if m.closed {
		done := m.closeDone
		m.stateMu.Unlock()
		<-done
		return nil
	}
	m.closed = true
	active := m.active.Swap(nil)
	retired := append([]*retiredVersion(nil), m.retired...)
	m.retired = nil
	candidates := make([]*Candidate, 0, len(m.candidates))
	for _, candidate := range m.candidates {
		candidates = append(candidates, candidate)
	}
	m.candidates = make(map[string]*Candidate)
	m.stateMu.Unlock()

	for _, candidate := range candidates {
		candidate.mu.Lock()
		if candidate.state == candidatePrepared {
			candidate.state = candidateAbandoned
			candidate.mu.Unlock()
			_ = closeResources(ctx, candidate.version)
		} else {
			candidate.mu.Unlock()
		}
	}
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

func (m *Manager) prepareResources(ctx context.Context, cfg Config) (*version, error) {
	order, err := m.startupOrder()
	if err != nil {
		return nil, err
	}
	v := &version{
		config:    cloneConfig(cfg),
		resources: make(map[string]Resource, len(order)),
		order:     order,
	}
	cleanup := func(cause error) (*version, error) {
		return nil, errors.Join(cause, closeResources(context.WithoutCancel(ctx), v))
	}
	for _, name := range order {
		if err := ctx.Err(); err != nil {
			return cleanup(err)
		}
		if err := m.modules[name].Validate(ctx, ModuleConfig{Name: name, Raw: cfg[name]}); err != nil {
			return cleanup(fmt.Errorf("validate module %q: %w", name, err))
		}
	}
	for _, name := range order {
		if err := ctx.Err(); err != nil {
			return cleanup(err)
		}
		resource, err := m.modules[name].Prepare(ctx, ModuleConfig{Name: name, Raw: cfg[name]})
		if err != nil {
			return cleanup(fmt.Errorf("prepare module %q: %w", name, err))
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
