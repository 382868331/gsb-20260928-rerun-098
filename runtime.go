package runtime

import (
	"bytes"
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
	ErrBatchNotReady   = errors.New("batch required modules are not all prepared")
	ErrBatchNotFound   = errors.New("batch is not open")
	ErrIncompatible    = errors.New("batch modules are incompatible")
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

// Revision reports the release revision this lease is pinned to. It never
// changes for the lifetime of the lease, even after later publishes.
func (l *Lease) Revision() int64 {
	if l == nil || l.version == nil {
		return 0
	}
	return l.version.revision
}

// BatchID reports the joint batch the pinned release originated from. It is
// empty for the initial start, plain reloads and rollbacks.
func (l *Lease) BatchID() string {
	if l == nil || l.version == nil {
		return ""
	}
	return l.version.batchID
}

func (l *Lease) Resource(name string) Resource {
	if l == nil || l.version == nil {
		return nil
	}
	if h := l.version.handles[name]; h != nil {
		return h.resource
	}
	return nil
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
	revision int64
	batchID  string
	config   Config
	handles  map[string]*resourceHandle
	order    []string
	leases   sync.WaitGroup
}

// resourceHandle owns one live Resource and tracks how many live versions and
// open candidates still reference it. The underlying resource is closed once,
// when the last reference disappears, so a module whose configuration did not
// change keeps being served by the very same resource across releases and is
// never closed while an in-flight lease (of any compatible version) uses it.
type resourceHandle struct {
	manager  *Manager
	resource Resource
	id       string
	module   string
	new      bool
	refs     int64
	closed   bool
	mu       sync.Mutex
}

// resourceNamer may be implemented by a Resource to return a stable
// identifier for cleanup records; otherwise the module name is used.
type resourceNamer interface {
	ResourceID() string
}

func (m *Manager) newHandle(module string, resource Resource) *resourceHandle {
	id := module
	if namer, ok := resource.(resourceNamer); ok {
		if stable := namer.ResourceID(); stable != "" {
			id = stable
		}
	}
	return &resourceHandle{
		manager:  m,
		resource: resource,
		id:       id,
		module:   module,
		new:      true,
		refs:     1,
	}
}

// share takes one more reference on the handle. It reports false when the
// handle is already closed and cannot be reused.
func (h *resourceHandle) share() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.refs++
	return true
}

func (h *resourceHandle) release(ctx context.Context) error {
	h.mu.Lock()
	h.refs--
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	if h.refs > 0 {
		h.mu.Unlock()
		h.manager.recordCleanup(h, CleanupInUse, nil)
		return nil
	}
	h.closed = true
	res := h.resource
	h.mu.Unlock()
	err := res.Close(ctx)
	if err != nil {
		h.manager.recordCleanup(h, CleanupFailed, err)
	} else {
		h.manager.recordCleanup(h, CleanupClosed, nil)
	}
	return err
}

// Cleanup outcomes recorded for every released handle.
const (
	// CleanupClosed marks a newly created resource whose last reference
	// disappeared and whose Close succeeded.
	CleanupClosed = "closed"
	// CleanupInUse marks a resource that is still referenced by another
	// live version or open candidate, so it was not closed.
	CleanupInUse = "in-use"
	// CleanupFailed marks a resource whose Close returned an error.
	CleanupFailed = "failed"
)

// CleanupRecord describes the fate of one released resource handle: whether
// it was a newly created resource that got closed, a resource still used by
// another version, or a release that failed.
type CleanupRecord struct {
	Time     time.Time
	Module   string
	Resource string
	New      bool
	Outcome  string
	Err      string
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
		BatchID:     c.version.batchID,
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

const (
	batchOpen      = "open"
	batchCommitted = "committed"
	batchAbandoned = "abandoned"
)

// Batch is a joint release unit for a set of related modules. Every required
// module must be prepared before Commit; modules outside the batch keep
// their current resources and configuration, so a committed batch switches
// readers atomically to one compatible configuration set. A batch is pinned
// to the revision active at creation: Commit fails with ErrBaselineChanged
// once another publish moves the active revision.
type Batch struct {
	manager  *Manager
	id       string
	baseline int64
	created  time.Time

	mu       sync.Mutex
	required []string
	inBatch  map[string]bool
	prepared map[string]*resourceHandle
	configs  map[string]json.RawMessage
	state    string
}

// NewBatch opens a joint batch over the given modules. The runtime must be
// started; module names must be known and unique within the batch.
func (m *Manager) NewBatch(id string, modules ...string) (*Batch, error) {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	if id == "" {
		return nil, errors.New("batch id must not be empty")
	}
	if len(modules) == 0 {
		return nil, errors.New("batch must include at least one module")
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.closed {
		return nil, errors.New("runtime manager is closed")
	}
	baseline := m.currentRevision()
	if baseline == 0 {
		return nil, errors.New("runtime is not started")
	}
	if _, exists := m.batches[id]; exists {
		return nil, fmt.Errorf("batch %q already exists", id)
	}
	batch := &Batch{
		manager:  m,
		id:       id,
		baseline: baseline,
		created:  time.Now().UTC(),
		inBatch:  make(map[string]bool, len(modules)),
		prepared: make(map[string]*resourceHandle, len(modules)),
		configs:  make(map[string]json.RawMessage, len(modules)),
		state:    batchOpen,
	}
	for _, name := range modules {
		if _, ok := m.modules[name]; !ok {
			return nil, fmt.Errorf("batch %q references unknown module %q", id, name)
		}
		if batch.inBatch[name] {
			return nil, fmt.Errorf("batch %q lists module %q twice", id, name)
		}
		batch.inBatch[name] = true
		batch.required = append(batch.required, name)
	}
	m.batches[id] = batch
	return batch, nil
}

// Batch returns the open batch with the given id, or ErrBatchNotFound.
func (m *Manager) Batch(id string) (*Batch, error) {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	batch, ok := m.batches[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrBatchNotFound, id)
	}
	return batch, nil
}

func (b *Batch) ID() string {
	return b.id
}

// Baseline is the revision that was active when the batch was created.
func (b *Batch) Baseline() int64 {
	return b.baseline
}

func (b *Batch) CreatedAt() time.Time {
	return b.created
}

// Status reports "open", "committed" or "abandoned".
func (b *Batch) Status() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Modules returns the required module set of the batch.
func (b *Batch) Modules() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.required...)
}

// Prepare validates and prepares one required module of the batch with its
// new configuration. The resource stays private to the batch until Commit.
func (b *Batch) Prepare(ctx context.Context, module string, raw json.RawMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m := b.manager
	b.mu.Lock()
	state := b.state
	inBatch := b.inBatch[module]
	b.mu.Unlock()
	if state != batchOpen {
		err := fmt.Errorf("%w: %s", ErrBatchNotFound, state)
		m.recordBatchAttempt("batch-prepare", b.id, b.baseline, 0, 0, false, "failed", err)
		return err
	}
	mod, ok := m.modules[module]
	if !ok || !inBatch {
		return fmt.Errorf("module %q is not part of batch %q", module, b.id)
	}
	config := ModuleConfig{Name: module, Raw: raw}
	if err := mod.Validate(ctx, config); err != nil {
		err = fmt.Errorf("validate module %q: %w", module, err)
		m.recordBatchAttempt("batch-prepare", b.id, b.baseline, 0, 0, false, "failed", err)
		return err
	}
	resource, err := mod.Prepare(ctx, config)
	if err != nil {
		err = fmt.Errorf("prepare module %q: %w", module, err)
		m.recordBatchAttempt("batch-prepare", b.id, b.baseline, 0, 0, false, "failed", err)
		return err
	}
	if resource == nil {
		resource = noopResource{}
	}
	handle := m.newHandle(module, resource)
	b.mu.Lock()
	if b.state != batchOpen {
		b.mu.Unlock()
		_ = handle.release(context.WithoutCancel(ctx))
		err := fmt.Errorf("%w: %s", ErrBatchNotFound, batchAbandoned)
		m.recordBatchAttempt("batch-prepare", b.id, b.baseline, 0, 0, false, "failed", err)
		return err
	}
	if previous := b.prepared[module]; previous != nil {
		_ = previous.release(context.WithoutCancel(ctx))
	}
	b.prepared[module] = handle
	b.configs[module] = append(json.RawMessage(nil), raw...)
	b.mu.Unlock()
	m.recordBatchAttempt("batch-prepare", b.id, b.baseline, 0, 0, false, "prepared", nil)
	return nil
}

// checkCompatible requires that every module depending on a batched module is
// itself part of the batch, so a joint release never leaves a dependent
// running against a resource it was not prepared with.
func (b *Batch) checkCompatible() error {
	m := b.manager
	for _, name := range b.required {
		for _, other := range m.order {
			if b.inBatch[other] {
				continue
			}
			for _, dependency := range m.modules[other].Dependencies() {
				if dependency == name {
					return fmt.Errorf("%w: module %q depends on batched module %q but is not in the batch", ErrIncompatible, other, name)
				}
			}
		}
	}
	return nil
}

// Commit atomically publishes the joint update: batched modules switch to
// their prepared resources, every other module keeps its current resource
// and configuration. It fails with ErrBatchNotReady while required modules
// are unprepared, with ErrIncompatible when a dependent is missing from the
// batch, and with ErrBaselineChanged when the active revision moved since
// the batch was created. On failure the batch stays open and its prepared
// resources stay alive.
func (b *Batch) Commit(ctx context.Context) error {
	m := b.manager
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	b.mu.Lock()
	if b.state != batchOpen {
		state := b.state
		b.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrBatchNotFound, state)
	}
	b.mu.Unlock()

	if err := ctx.Err(); err != nil {
		m.recordBatchAttempt("batch-commit", b.id, b.baseline, 0, 0, false, "failed", err)
		return err
	}

	b.mu.Lock()
	var missing []string
	for _, name := range b.required {
		if b.prepared[name] == nil {
			missing = append(missing, name)
		}
	}
	b.mu.Unlock()
	if len(missing) > 0 {
		err := fmt.Errorf("%w: %s", ErrBatchNotReady, joinNames(missing))
		m.recordBatchAttempt("batch-commit", b.id, b.baseline, 0, 0, false, "failed", err)
		return err
	}
	if err := b.checkCompatible(); err != nil {
		m.recordBatchAttempt("batch-commit", b.id, b.baseline, 0, 0, false, "failed", err)
		return err
	}

	m.stateMu.RLock()
	if m.closed {
		m.stateMu.RUnlock()
		err := errors.New("runtime manager is closed")
		m.recordBatchAttempt("batch-commit", b.id, b.baseline, 0, 0, false, "failed", err)
		return err
	}
	active := m.active.Load()
	m.stateMu.RUnlock()
	if active == nil {
		err := errors.New("runtime is not started")
		m.recordBatchAttempt("batch-commit", b.id, b.baseline, 0, 0, false, "failed", err)
		return err
	}

	b.mu.Lock()
	merged := cloneConfig(active.config)
	for _, name := range b.required {
		merged[name] = append(json.RawMessage(nil), b.configs[name]...)
	}
	b.mu.Unlock()

	if err := persistConfig(m.configPath, merged); err != nil {
		m.recordBatchAttempt("batch-commit", b.id, b.baseline, active.revision, 0, false, "failed", err)
		return err
	}

	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		err := errors.New("runtime manager is closed")
		m.recordBatchAttempt("batch-commit", b.id, b.baseline, active.revision, 0, false, "failed", err)
		return err
	}
	current := m.active.Load()
	if current == nil || current.revision != b.baseline {
		currentRevision := int64(0)
		if current != nil {
			currentRevision = current.revision
		}
		m.stateMu.Unlock()
		err := fmt.Errorf("%w: batch baseline %d, current revision %d", ErrBaselineChanged, b.baseline, currentRevision)
		m.recordBatchAttempt("batch-commit", b.id, b.baseline, currentRevision, 0, false, "conflict", err)
		return err
	}
	v := &version{
		batchID: b.id,
		config:  merged,
		handles: make(map[string]*resourceHandle, len(current.order)),
		order:   current.order,
	}
	b.mu.Lock()
	for _, name := range v.order {
		if handle := b.prepared[name]; handle != nil {
			v.handles[name] = handle
			continue
		}
		v.handles[name] = current.handles[name]
		v.handles[name].share()
	}
	b.state = batchCommitted
	b.prepared = nil
	b.configs = nil
	b.mu.Unlock()

	m.revisionSeq++
	revision := m.revisionSeq
	v.revision = revision
	m.active.Store(v)
	m.retireVersion(current)
	m.appendHistory(ReleaseInfo{
		Revision:    revision,
		BatchID:     b.id,
		Config:      cloneConfig(v.config),
		PublishedAt: time.Now().UTC(),
		Source:      "batch:" + b.id,
	})
	delete(m.batches, b.id)
	m.stateMu.Unlock()

	m.recordBatchAttempt("batch-commit", b.id, b.baseline, b.baseline, revision, true, "switched", nil)
	return nil
}

// Abandon releases every resource prepared by the batch in reverse dependency
// order and closes the batch. The active version is never touched. Abandon is
// idempotent and works after a failed Commit.
func (b *Batch) Abandon(ctx context.Context) error {
	m := b.manager
	b.mu.Lock()
	if b.state == batchAbandoned {
		b.mu.Unlock()
		return nil
	}
	if b.state != batchOpen {
		state := b.state
		b.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrBatchNotFound, state)
	}
	b.state = batchAbandoned
	prepared := b.prepared
	b.prepared = nil
	b.configs = nil
	b.mu.Unlock()

	order, err := m.startupOrder()
	if err != nil {
		return err
	}
	var errs []error
	for i := len(order) - 1; i >= 0; i-- {
		if handle := prepared[order[i]]; handle != nil {
			if err := handle.release(ctx); err != nil {
				errs = append(errs, fmt.Errorf("close module %q: %w", order[i], err))
			}
		}
	}
	err = errors.Join(errs...)
	m.stateMu.Lock()
	delete(m.batches, b.id)
	m.stateMu.Unlock()
	m.recordBatchAttempt("batch-abandon", b.id, b.baseline, 0, 0, false, "abandoned", err)
	return err
}

// ReleaseInfo describes one revision that actually became active.
type ReleaseInfo struct {
	Revision    int64
	BatchID     string
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
	BatchID      string
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
	batches      map[string]*Batch
	revisionSeq  int64
	history      []ReleaseInfo
	attempts     []AttemptRecord
	cleanups     []CleanupRecord
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
		batches:    make(map[string]*Batch),
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
		BatchID:     version.batchID,
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
			BatchID:     info.BatchID,
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

// Cleanups returns the cleanup records for released resource handles, oldest
// first. Each record distinguishes newly created resources that were closed,
// resources still used by another live version, and releases that failed.
func (m *Manager) Cleanups() []CleanupRecord {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return append([]CleanupRecord(nil), m.cleanups...)
}

func (m *Manager) recordCleanup(h *resourceHandle, outcome string, err error) {
	record := CleanupRecord{
		Time:     time.Now().UTC(),
		Module:   h.module,
		Resource: h.id,
		New:      h.new,
		Outcome:  outcome,
	}
	if err != nil {
		record.Err = err.Error()
	}
	m.stateMu.Lock()
	m.cleanups = append(m.cleanups, record)
	m.stateMu.Unlock()
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
				BatchID:     info.BatchID,
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
	m.record(kind, candidateID, "", baseline, fromRevision, toRevision, switched, result, err)
}

func (m *Manager) recordBatchAttempt(kind, batchID string, baseline, fromRevision, toRevision int64, switched bool, result string, err error) {
	m.record(kind, "", batchID, baseline, fromRevision, toRevision, switched, result, err)
}

func (m *Manager) record(kind, candidateID, batchID string, baseline, fromRevision, toRevision int64, switched bool, result string, err error) {
	record := AttemptRecord{
		Time:         time.Now().UTC(),
		Kind:         kind,
		CandidateID:  candidateID,
		BatchID:      batchID,
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
	batches := make([]*Batch, 0, len(m.batches))
	for _, batch := range m.batches {
		batches = append(batches, batch)
	}
	m.batches = make(map[string]*Batch)
	m.stateMu.Unlock()

	for _, batch := range batches {
		batch.mu.Lock()
		if batch.state != batchOpen {
			batch.mu.Unlock()
			continue
		}
		batch.state = batchAbandoned
		prepared := batch.prepared
		batch.prepared = nil
		batch.configs = nil
		batch.mu.Unlock()
		order, err := m.startupOrder()
		if err != nil {
			continue
		}
		for i := len(order) - 1; i >= 0; i-- {
			if handle := prepared[order[i]]; handle != nil {
				_ = handle.release(ctx)
			}
		}
	}
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
		config:  cloneConfig(cfg),
		handles: make(map[string]*resourceHandle, len(order)),
		order:   order,
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
	m.stateMu.RLock()
	var active *version
	if !m.closed {
		active = m.active.Load()
	}
	m.stateMu.RUnlock()
	for _, name := range order {
		if err := ctx.Err(); err != nil {
			return cleanup(err)
		}
		if active != nil {
			if h := active.handles[name]; h != nil && bytes.Equal(active.config[name], cfg[name]) && h.share() {
				v.handles[name] = h
				continue
			}
		}
		resource, err := m.modules[name].Prepare(ctx, ModuleConfig{Name: name, Raw: cfg[name]})
		if err != nil {
			return cleanup(fmt.Errorf("prepare module %q: %w", name, err))
		}
		if resource == nil {
			resource = noopResource{}
		}
		v.handles[name] = m.newHandle(name, resource)
	}
	return v, nil
}

func (m *Manager) retireVersion(v *version) {
	item := &retiredVersion{version: v, done: make(chan error, 1)}
	m.retired = append(m.retired, item)
	go func() {
		v.leases.Wait()
		err := closeResources(context.Background(), item.version)
		m.stateMu.Lock()
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
		if h := v.handles[name]; h != nil {
			if err := h.release(ctx); err != nil {
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
