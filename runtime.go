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

// CompatibilityChecker optionally validates one module against the dependency
// configurations in the same joint release. Dependencies contains only the
// modules named by Dependencies. Returning an error rejects the whole batch
// before any changed resource is published.
type CompatibilityChecker interface {
	CheckCompatibility(ctx context.Context, config ModuleConfig, dependencies map[string]ModuleConfig) error
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
	handle := l.version.handles[name]
	if handle == nil {
		return nil
	}
	return handle.resource
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
	created  map[string]bool
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

type resourceNamer interface {
	ResourceID() string
}

func (v *version) release(ctx context.Context, closedResult string) error {
	var errs []error
	for i := len(v.order) - 1; i >= 0; i-- {
		name := v.order[i]
		handle := v.handles[name]
		if handle == nil {
			continue
		}
		handle.mu.Lock()
		remaining := handle.refs - 1
		handle.refs = remaining
		if remaining > 0 || handle.closed {
			id, module := handle.id, handle.module
			handle.mu.Unlock()
			handle.manager.recordCleanup("retained", module, id, "")
			continue
		}
		res := handle.resource
		id, module := handle.id, handle.module
		handle.mu.Unlock()

		err := res.Close(ctx)
		if err != nil {
			handle.manager.recordCleanup("release-failed", module, id, err.Error())
			errs = append(errs, fmt.Errorf("close module %q resource %q: %w", module, id, err))
			continue
		}
		handle.mu.Lock()
		handle.closed = true
		handle.mu.Unlock()
		result := closedResult
		if v.created[name] {
			result = "added"
		}
		handle.manager.recordCleanup(result, module, id, "")
	}
	return errors.Join(errs...)
}

func (v *version) createdHandles() map[string]bool {
	return cloneSet(v.created)
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
	manager   *Manager
	id        string
	baseline  int64
	version   *version
	modules   map[string]bool
	batchID   string
	inherited map[string]bool
	detached  bool
	created   time.Time

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

// Modules reports the modules explicitly included in a joint preparation. It
// is nil for a candidate prepared from one complete configuration.
func (c *Candidate) Modules() []string {
	if c.modules == nil {
		return nil
	}
	modules := make([]string, 0, len(c.modules))
	for module := range c.modules {
		modules = append(modules, module)
	}
	sort.Strings(modules)
	return modules
}

// BatchID reports the joint batch this candidate belongs to. It is empty for
// ordinary full-configuration candidates and releases.
func (c *Candidate) BatchID() string {
	return c.batchID
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
		if detachErr := c.detachInherited(context.WithoutCancel(ctx)); detachErr != nil {
			m.recordAttempt("cleanup", c.id, c.baseline, activeRevision, 0, false, "failed", detachErr)
		}
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
		if detachErr := c.detachInherited(context.WithoutCancel(ctx)); detachErr != nil {
			m.recordAttempt("cleanup", c.id, c.baseline, currentRevision, 0, false, "failed", detachErr)
		}
		m.recordAttempt("publish", c.id, c.baseline, currentRevision, 0, false, "conflict", err)
		return err
	}
	m.revisionSeq++
	revision := m.revisionSeq
	c.version.revision = revision
	c.version.batchID = c.batchID
	m.active.Store(c.version)
	m.appendHistory(ReleaseInfo{
		Revision:    revision,
		BatchID:     c.batchID,
		Config:      cloneConfig(c.version.config),
		PublishedAt: time.Now().UTC(),
		Source:      publishSource(c.batchID),
	})
	c.mu.Lock()
	c.state = candidatePublished
	c.mu.Unlock()
	delete(m.candidates, c.id)
	m.stateMu.Unlock()
	if current != nil {
		if err := m.retireVersion(context.WithoutCancel(ctx), current); err != nil {
			m.recordAttempt("cleanup", c.id, c.baseline, revision, revision, false, "failed", err)
		}
	}

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
	detached := c.detached
	c.mu.Unlock()
	if detached {
		return c.releaseAdded(ctx)
	}
	return errors.Join(c.detachInherited(ctx), version.release(ctx, "added"))
}

func (c *Candidate) releaseAdded(ctx context.Context) error {
	c.mu.Lock()
	version := c.version
	c.mu.Unlock()
	var errs []error
	for i := len(version.order) - 1; i >= 0; i-- {
		name := version.order[i]
		if !version.created[name] {
			continue
		}
		handle := version.handles[name]
		if handle == nil {
			continue
		}
		handle.mu.Lock()
		remaining := handle.refs - 1
		handle.refs = remaining
		if remaining > 0 || handle.closed {
			id, module := handle.id, handle.module
			handle.mu.Unlock()
			c.manager.recordCleanup("retained", module, id, "")
			continue
		}
		res := handle.resource
		id, module := handle.id, handle.module
		handle.mu.Unlock()
		if err := res.Close(ctx); err != nil {
			c.manager.recordCleanup("release-failed", module, id, err.Error())
			errs = append(errs, fmt.Errorf("close module %q resource %q: %w", module, id, err))
			continue
		}
		handle.mu.Lock()
		handle.closed = true
		handle.mu.Unlock()
		c.manager.recordCleanup("added", module, id, "")
	}
	return errors.Join(errs...)
}

func (c *Candidate) detachInherited(ctx context.Context) error {
	c.mu.Lock()
	inherited := cloneSet(c.inherited)
	version := c.version
	c.inherited = nil
	c.detached = true
	c.mu.Unlock()
	var errs []error
	for name := range inherited {
		handle := version.handles[name]
		if handle == nil {
			continue
		}
		handle.mu.Lock()
		remaining := handle.refs - 1
		handle.refs = remaining
		if remaining > 0 || handle.closed {
			id, module := handle.id, handle.module
			handle.mu.Unlock()
			c.manager.recordCleanup("retained", module, id, "")
			continue
		}
		res := handle.resource
		id, module := handle.id, handle.module
		handle.mu.Unlock()
		if err := res.Close(ctx); err != nil {
			handle.mu.Lock()
			handle.refs++
			handle.mu.Unlock()
			c.manager.recordCleanup("release-failed", module, id, err.Error())
			errs = append(errs, fmt.Errorf("close module %q resource %q: %w", module, id, err))
			continue
		}
		handle.mu.Lock()
		handle.closed = true
		handle.mu.Unlock()
		c.manager.recordCleanup("released", module, id, "")
	}
	return errors.Join(errs...)
}

func (c *Candidate) markAbandoned() {
	c.mu.Lock()
	c.state = candidateAbandoned
	c.mu.Unlock()
}

// CandidateInfo describes a prepared but not yet published candidate.
type CandidateInfo struct {
	ID        string
	BatchID   string
	Baseline  int64
	Modules   []string
	Config    Config
	CreatedAt time.Time
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
	Baseline     int64
	FromRevision int64
	ToRevision   int64
	Switched     bool
	Result       string
	Err          string
}

// CleanupRecord explains what happened to one resource reference when a
// candidate or retired version was released. Added resources were created by
// the released scope, retained resources remain referenced by another active
// version/open candidate, and release-failed resources still need attention.
type CleanupRecord struct {
	Time       time.Time
	Result     string
	Module     string
	ResourceID string
	Err        string
}

// Batch is a jointly prepared update of one or more modules.
type Batch struct {
	candidate *Candidate
}

func (b *Batch) ID() string {
	return b.candidate.id
}

func (b *Batch) BatchID() string {
	return b.candidate.batchID
}

func (b *Batch) Baseline() int64 {
	return b.candidate.baseline
}

func (b *Batch) Modules() []string {
	return b.candidate.Modules()
}

func (b *Batch) Config() Config {
	return b.candidate.Config()
}

func (b *Batch) Status() string {
	return b.candidate.Status()
}

// Confirm atomically publishes every module in the batch. It returns
// ErrBaselineChanged without changing either the active release or this batch
// when the confirmation basis has moved.
func (b *Batch) Confirm(ctx context.Context) error {
	return b.candidate.Publish(ctx)
}

// Abandon releases resources newly prepared by this batch. Handles inherited
// from the baseline and shared with other candidates stay alive.
func (b *Batch) Abandon(ctx context.Context) error {
	return b.candidate.Abandon(ctx)
}

type BatchInfo struct {
	ID        string
	BatchID   string
	Baseline  int64
	Modules   []string
	Config    Config
	CreatedAt time.Time
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
	resourceSeq  int64
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
		v, _, err := m.prepareVersion(ctx, cfg, nil, "")
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
	version, _, err := m.prepareVersion(ctx, cfg, nil, "")
	if err != nil {
		m.recordAttempt("prepare", "", baseline, 0, 0, false, "failed", err)
		return nil, err
	}
	candidate := m.newCandidate("", baseline, version, nil, nil)
	m.stateMu.Lock()
	m.candidateSeq++
	candidate.id = fmt.Sprintf("candidate-%d", m.candidateSeq)
	if m.closed {
		m.stateMu.Unlock()
		abandonErr := version.release(context.WithoutCancel(ctx), "added")
		err := errors.Join(errors.New("runtime manager is closed"), abandonErr)
		m.recordAttempt("prepare", candidate.id, baseline, 0, 0, false, "failed", err)
		return nil, err
	}
	m.candidates[candidate.id] = candidate
	m.stateMu.Unlock()
	m.recordAttempt("prepare", candidate.id, baseline, 0, 0, false, "prepared", nil)
	return candidate, nil
}

// PrepareBatch validates compatibility and prepares a joint update for the
// named modules. updates is a partial configuration: omitted modules keep the
// exact active configuration and continue using their existing resources. An
// empty batchID is assigned automatically.
func (m *Manager) PrepareBatch(ctx context.Context, batchID string, modules []string, updates Config) (*Batch, error) {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	baseline := m.currentRevision()
	if baseline == 0 {
		err := errors.New("runtime is not started")
		m.recordAttempt("prepare-batch", "", 0, 0, 0, false, "failed", err)
		return nil, err
	}
	if m.isClosed() {
		err := errors.New("runtime manager is closed")
		m.recordAttempt("prepare-batch", "", baseline, 0, 0, false, "failed", err)
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		m.recordAttempt("prepare-batch", "", baseline, 0, 0, false, "failed", err)
		return nil, err
	}

	selected, err := normalizeModules(m.modules, modules)
	if err != nil {
		m.recordAttempt("prepare-batch", "", baseline, 0, 0, false, "failed", err)
		return nil, err
	}
	m.stateMu.Lock()
	if batchID == "" {
		m.candidateSeq++
		batchID = fmt.Sprintf("batch-%d", m.candidateSeq)
	}
	if _, exists := m.candidates[batchID]; exists {
		m.stateMu.Unlock()
		err := fmt.Errorf("batch %q is already open", batchID)
		m.recordAttempt("prepare-batch", batchID, baseline, 0, 0, false, "failed", err)
		return nil, err
	}
	m.stateMu.Unlock()

	version, _, err := m.prepareVersion(ctx, updates, selected, batchID)
	if err != nil {
		m.recordAttempt("prepare-batch", batchID, baseline, 0, 0, false, "failed", err)
		return nil, err
	}
	candidate := m.newCandidate(batchID, baseline, version, selected, inheritedSet(version, selected))
	candidate.id = batchID
	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		abandonErr := version.release(context.WithoutCancel(ctx), "added")
		err := errors.Join(errors.New("runtime manager is closed"), abandonErr)
		m.recordAttempt("prepare-batch", batchID, baseline, 0, 0, false, "failed", err)
		return nil, err
	}
	m.candidates[batchID] = candidate
	m.stateMu.Unlock()
	m.recordAttempt("prepare-batch", batchID, baseline, 0, 0, false, "prepared", nil)
	return &Batch{candidate: candidate}, nil
}

// Batches returns joint candidates that are still open.
func (m *Manager) Batches() []BatchInfo {
	infos := make([]BatchInfo, 0)
	for _, candidate := range m.Candidates() {
		if candidate.BatchID == "" {
			continue
		}
		infos = append(infos, BatchInfo{
			ID:        candidate.ID,
			BatchID:   candidate.BatchID,
			Baseline:  candidate.Baseline,
			Modules:   candidate.Modules,
			Config:    candidate.Config,
			CreatedAt: candidate.CreatedAt,
		})
	}
	return infos
}

// Batch returns the open joint batch with the supplied ID.
func (m *Manager) Batch(id string) (*Batch, error) {
	m.stateMu.RLock()
	candidate, ok := m.candidates[id]
	m.stateMu.RUnlock()
	if !ok || candidate == nil || candidate.batchID == "" {
		return nil, fmt.Errorf("%w: %s", ErrBatchNotFound, id)
	}
	return &Batch{candidate: candidate}, nil
}

// ConfirmBatch publishes an open joint batch as one complete release.
func (m *Manager) ConfirmBatch(ctx context.Context, id string) error {
	batch, err := m.Batch(id)
	if err != nil {
		return err
	}
	return batch.Confirm(ctx)
}

// AbandonBatch discards an open joint batch without affecting the active
// release, other batches or active leases.
func (m *Manager) AbandonBatch(ctx context.Context, id string) error {
	batch, err := m.Batch(id)
	if err != nil {
		return err
	}
	return batch.Abandon(ctx)
}

func (m *Manager) newCandidate(batchID string, baseline int64, version *version, modules, inherited map[string]bool) *Candidate {
	return &Candidate{
		manager:   m,
		id:        batchID,
		batchID:   batchID,
		baseline:  baseline,
		version:   version,
		modules:   modules,
		inherited: inherited,
		created:   time.Now().UTC(),
		state:     candidatePrepared,
	}
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
	version, _, err := m.prepareVersion(ctx, info.Config, nil, "")
	if err != nil {
		m.recordAttempt("rollback", "", baseline, 0, 0, false, "failed", err)
		return err
	}
	if err := persistConfig(m.configPath, version.config); err != nil {
		_ = version.release(context.WithoutCancel(ctx), "added")
		m.recordAttempt("rollback", "", baseline, 0, 0, false, "failed", err)
		return err
	}
	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		_ = version.release(context.WithoutCancel(ctx), "added")
		err := errors.New("runtime manager is closed")
		m.recordAttempt("rollback", "", baseline, 0, 0, false, "failed", err)
		return err
	}
	current := m.active.Load()
	if current == nil || current.revision != baseline {
		m.stateMu.Unlock()
		_ = version.release(context.WithoutCancel(ctx), "added")
		err := ErrBaselineChanged
		m.recordAttempt("rollback", "", baseline, m.currentRevision(), 0, false, "conflict", err)
		return err
	}
	m.revisionSeq++
	newRevision := m.revisionSeq
	version.revision = newRevision
	version.batchID = ""
	m.active.Store(version)
	m.appendHistory(ReleaseInfo{
		Revision:    newRevision,
		BatchID:     "",
		Config:      cloneConfig(version.config),
		PublishedAt: time.Now().UTC(),
		Source:      fmt.Sprintf("rollback:r%d", revision),
	})
	m.stateMu.Unlock()
	if err := m.retireVersion(context.WithoutCancel(ctx), current); err != nil {
		m.recordAttempt("cleanup", "", baseline, baseline, newRevision, false, "failed", err)
	}
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
			BatchID:   candidate.batchID,
			Baseline:  candidate.baseline,
			Modules:   candidate.Modules(),
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

// CleanupRecords returns resource cleanup audit entries. Results are "added"
// for new resources whose candidate/version failed or was abandoned,
// "retained" for handles still used elsewhere, "released" for resources that
// became unused after a successful switch, and "release-failed" for failed
// Close calls.
func (m *Manager) CleanupRecords() []CleanupRecord {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return append([]CleanupRecord(nil), m.cleanups...)
}

func (m *Manager) recordCleanup(result, module, id, errText string) {
	m.stateMu.Lock()
	m.cleanups = append(m.cleanups, CleanupRecord{
		Time:       time.Now().UTC(),
		Result:     result,
		Module:     module,
		ResourceID: id,
		Err:        errText,
	})
	m.stateMu.Unlock()
}

func (m *Manager) isClosed() bool {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return m.closed
}

func normalizeModules(known map[string]Module, modules []string) (map[string]bool, error) {
	if len(modules) == 0 {
		return nil, errors.New("joint batch must contain at least one module")
	}
	selected := make(map[string]bool, len(modules))
	for _, module := range modules {
		if module == "" {
			return nil, errors.New("joint batch module name must not be empty")
		}
		if _, ok := known[module]; !ok {
			return nil, fmt.Errorf("unknown batch module %q", module)
		}
		if selected[module] {
			return nil, fmt.Errorf("duplicate batch module %q", module)
		}
		selected[module] = true
	}
	return selected, nil
}

func cloneSet(set map[string]bool) map[string]bool {
	clone := make(map[string]bool, len(set))
	for key, value := range set {
		clone[key] = value
	}
	return clone
}

func inheritedSet(v *version, selected map[string]bool) map[string]bool {
	inherited := make(map[string]bool)
	for _, name := range v.order {
		if !selected[name] {
			inherited[name] = true
		}
	}
	return inherited
}

func publishSource(batchID string) string {
	if batchID == "" {
		return "publish"
	}
	return "batch:" + batchID
}

func configEqual(a, b json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(a), bytes.TrimSpace(b))
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
			_ = candidate.version.release(ctx, "added")
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
		errCh <- active.release(ctx, "released")
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

func (m *Manager) prepareVersion(ctx context.Context, updates Config, selected map[string]bool, batchID string) (*version, map[string]bool, error) {
	order, err := m.startupOrder()
	if err != nil {
		return nil, nil, err
	}
	active := m.active.Load()
	cfg := Config{}
	if active != nil {
		cfg = cloneConfig(active.config)
	}
	for name, raw := range updates {
		if _, known := m.modules[name]; !known {
			return nil, nil, fmt.Errorf("unknown module %q", name)
		}
		if selected != nil && !selected[name] {
			return nil, nil, fmt.Errorf("%w: update for module %q outside batch", ErrIncompatible, name)
		}
		cfg[name] = append(json.RawMessage(nil), raw...)
	}
	v := &version{
		batchID: batchID,
		config:  cfg,
		handles: make(map[string]*resourceHandle, len(order)),
		order:   order,
		created: make(map[string]bool, len(order)),
	}
	cleanup := func(cause error) (*version, map[string]bool, error) {
		return nil, nil, errors.Join(cause, v.release(context.WithoutCancel(ctx), "added"))
	}
	for _, name := range order {
		if _, known := m.modules[name]; !known {
			return cleanup(fmt.Errorf("missing configuration for module %q", name))
		}
		if err := ctx.Err(); err != nil {
			return cleanup(err)
		}
		if err := m.modules[name].Validate(ctx, ModuleConfig{Name: name, Raw: cfg[name]}); err != nil {
			return cleanup(fmt.Errorf("validate module %q: %w", name, err))
		}
	}
	for _, name := range order {
		if selected != nil && !selected[name] && active != nil && configEqual(active.config[name], cfg[name]) {
			handle := active.handles[name]
			handle.mu.Lock()
			handle.refs++
			handle.mu.Unlock()
			v.handles[name] = handle
			continue
		}
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
		m.stateMu.Lock()
		m.resourceSeq++
		handle := &resourceHandle{manager: m, resource: resource, id: resourceID(name, m.resourceSeq, resource), module: name, refs: 1}
		m.stateMu.Unlock()
		v.handles[name] = handle
		v.created[name] = true
	}
	for _, name := range order {
		if selected == nil {
			continue
		}
		if !selected[name] {
			continue
		}
		checker, ok := m.modules[name].(CompatibilityChecker)
		if !ok {
			continue
		}
		dependencies := make(map[string]ModuleConfig)
		for _, dependency := range m.modules[name].Dependencies() {
			dependencies[dependency] = ModuleConfig{Name: dependency, Raw: cfg[dependency]}
		}
		if err := checker.CheckCompatibility(ctx, ModuleConfig{Name: name, Raw: cfg[name]}, dependencies); err != nil {
			return cleanup(fmt.Errorf("%w: compatibility module %q: %w", ErrIncompatible, name, err))
		}
	}
	return v, cloneSet(v.created), nil
}

func (m *Manager) retireVersion(ctx context.Context, v *version) error {
	item := &retiredVersion{version: v, done: make(chan error, 1)}
	m.stateMu.Lock()
	m.retired = append(m.retired, item)
	m.stateMu.Unlock()
	go func() {
		v.leases.Wait()
		err := item.version.release(ctx, "released")
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
	return nil
}

type noopResource struct{}

func (noopResource) Close(context.Context) error { return nil }

func resourceID(module string, seq int64, resource Resource) string {
	if named, ok := resource.(resourceNamer); ok && named.ResourceID() != "" {
		return named.ResourceID()
	}
	return fmt.Sprintf("%s-%d", module, seq)
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
