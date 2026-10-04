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
	// ErrBatchNotReady indicates that a joint batch cannot be confirmed because
	// at least one required module is not ready or the batch is incompatible.
	ErrBatchNotReady = errors.New("batch is not ready to confirm")
	// ErrBatchClosed indicates that a joint batch has already reached a
	// terminal state (confirmed, abandoned or failed preparation).
	ErrBatchClosed = errors.New("batch is no longer open")
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
	if node := l.version.nodes[name]; node != nil {
		return node.resource
	}
	return nil
}

func (l *Lease) Config(name string) ModuleConfig {
	if l == nil || l.version == nil {
		return ModuleConfig{Name: name}
	}
	return ModuleConfig{Name: name, Raw: l.version.config[name]}
}

// Revision reports the configuration revision the lease is pinned to. Reads
// started before a switch keep returning this revision until Release.
func (l *Lease) Revision() int64 {
	if l == nil || l.version == nil {
		return 0
	}
	return l.version.revision
}

func (l *Lease) Release() {
	if l != nil && l.release != nil {
		l.release()
		l.release = nil
	}
}

// resourceNode is one prepared resource shared by every version and open batch
// that references the same underlying preparation. The resource is closed
// exactly once, only after every holder has released it, so abandoning a
// candidate or retiring an old version can never destroy a resource that is
// still serving in-flight reads or another candidate batch.
type resourceNode struct {
	name     string
	resource Resource

	mu       sync.Mutex
	users    int
	closed   bool
	closeErr error
}

func newResourceNode(name string, resource Resource, users int) *resourceNode {
	return &resourceNode{name: name, resource: resource, users: users}
}

func (n *resourceNode) acquire() {
	n.mu.Lock()
	n.users++
	n.mu.Unlock()
}

// release decrements the holder count. When this release removes the last
// holder the underlying resource is closed exactly once; closed reports
// whether this call performed the close and err carries its result (also
// remembered on the node for later inspection).
func (n *resourceNode) release(ctx context.Context) (closed bool, err error) {
	n.mu.Lock()
	if n.users > 0 {
		n.users--
	}
	if n.users > 0 || n.closed {
		err = n.closeErr
		n.mu.Unlock()
		return false, err
	}
	n.closed = true
	resource := n.resource
	n.mu.Unlock()

	closeErr := resource.Close(ctx)

	n.mu.Lock()
	n.closeErr = closeErr
	n.mu.Unlock()
	return true, closeErr
}

func (n *resourceNode) state() (users int, closed bool, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.users, n.closed, n.closeErr
}

type version struct {
	revision int64
	config   Config
	nodes    map[string]*resourceNode
	order    []string
	leases   sync.WaitGroup
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
		m.recordAttempt("publish", c.id, "", c.baseline, 0, 0, false, "failed", err)
		return err
	}
	if m.configPath == "" {
		err := errors.New("configuration path is empty")
		m.recordAttempt("publish", c.id, "", c.baseline, 0, 0, false, "failed", err)
		return err
	}

	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		err := errors.New("runtime manager is closed")
		m.recordAttempt("publish", c.id, "", c.baseline, 0, 0, false, "failed", err)
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
		m.recordAttempt("publish", c.id, "", c.baseline, activeRevision, 0, false, "conflict", err)
		return err
	}
	m.stateMu.Unlock()

	if err := persistConfig(m.configPath, c.version.config); err != nil {
		c.markAbandoned()
		_ = m.releaseVersion(ctx, c.version)
		m.forgetCandidate(c)
		m.recordAttempt("publish", c.id, "", c.baseline, activeRevision, 0, false, "failed", err)
		return err
	}

	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		err := errors.New("runtime manager is closed")
		c.markAbandoned()
		_ = m.releaseVersion(context.WithoutCancel(ctx), c.version)
		m.forgetCandidate(c)
		m.recordAttempt("publish", c.id, "", c.baseline, activeRevision, 0, false, "failed", err)
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
		m.recordAttempt("publish", c.id, "", c.baseline, currentRevision, 0, false, "conflict", err)
		return err
	}
	// The candidate keeps its hold while the new active version acquires every
	// node; releasing the candidate afterwards cannot close a resource that is
	// now shared with the active version.
	for _, node := range c.version.nodes {
		node.acquire()
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

	// Drop the candidate's own hold; resources stay alive via the active
	// version and only close once its leases finish.
	_ = m.releaseVersion(context.WithoutCancel(ctx), c.version)
	m.recordAttempt("publish", c.id, "", c.baseline, activeRevision, revision, true, "switched", nil)
	return nil
}

// Abandon releases every resource exclusively owned by the candidate. Shared
// resources keep serving the active version or other candidates. The active
// version is never touched. Abandon is idempotent and works after a failed
// Publish.
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

	err := c.manager.releaseVersion(ctx, c.version)
	m := c.manager
	m.forgetCandidate(c)
	m.recordAttempt("abandon", c.id, "", c.baseline, 0, 0, false, "abandoned", err)
	return err
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
	BatchID      string
	Baseline     int64
	FromRevision int64
	ToRevision   int64
	Switched     bool
	Result       string
	Err          string
}

// BatchModule selects one module for a joint batch.
type BatchModule struct {
	Name string
	// Raw is the new module configuration. Required marks the module
	// mandatory: the batch cannot be confirmed until it is ready. An optional
	// module that fails preparation is skipped and excluded from the assembled
	// configuration instead of failing the whole batch.
	Raw      json.RawMessage
	Required bool
}

// CompatibilityFunc checks cross-module compatibility conditions over the full
// assembled configuration (unchanged baseline modules plus every ready batch
// module). It runs after all module preparations and again at confirm time, so
// a batch can never become effective unless all declared conditions hold.
type CompatibilityFunc func(view map[string]ModuleConfig) error

// BatchSpec describes a joint release candidate for several modules.
type BatchSpec struct {
	Modules    []BatchModule
	Compatible CompatibilityFunc
}

const (
	batchPreparing    = "preparing"
	batchPrepared     = "prepared"
	batchIncompatible = "incompatible"
	batchConfirmed    = "confirmed"
	batchAbandoned    = "abandoned"
	batchFailed       = "failed"
)

// ModuleState is the per-module readiness view of a joint batch.
type ModuleState struct {
	Name string
	// Status is "ready" (newly prepared), "unchanged" (inherited from the
	// baseline revision), "failed" (required module failed), or "skipped"
	// (optional module failed and was excluded).
	Status string
	// Required reports whether the module must be ready for confirmation.
	Required bool
	// Inherited is true for modules whose resource is taken over from the
	// baseline revision rather than created by this batch.
	Inherited bool
	// Reason carries the validation/preparation failure description.
	Reason string
	// Resource is the resource currently occupied for the module (newly
	// created or still serving the baseline), nil when unavailable.
	Resource Resource
}

// Resource disposition categories recorded when a batch ends.
const (
	// ResourceNew marks a resource created by the batch and released by it.
	ResourceNew = "new"
	// ResourceKept marks a resource still used by the old batch / baseline or
	// another candidate, so it was not closed.
	ResourceKept = "kept"
	// ResourceCleanupFailed marks a newly created resource whose Close failed.
	ResourceCleanupFailed = "cleanup-failed"
)

// ResourceRecord is a queryable disposition entry for one resource touched by
// a batch during failure or teardown.
type ResourceRecord struct {
	Module string
	// Kind is ResourceNew, ResourceKept or ResourceCleanupFailed.
	Kind string
	// New reports whether the resource was created by this batch.
	New bool
	// InUse reports that another holder (active revision, in-flight lease or
	// another candidate batch) still references the resource.
	InUse bool
	// Err carries the Close failure for ResourceCleanupFailed entries.
	Err  string
	Time time.Time
}

// Batch is a joint candidate covering several associated modules. While open
// it pins every resource it needs, including unchanged resources inherited
// from its baseline revision, which guarantees that readers always observe
// one mutually compatible set and that another candidate's teardown cannot
// destroy a resource this batch relies on.
type Batch struct {
	manager  *Manager
	id       string
	baseline int64
	created  time.Time
	reason   string

	mu         sync.Mutex
	state      string
	modules    map[string]*modulePreparation
	nodes      map[string]*resourceNode
	order      []string
	records    []ResourceRecord
	compatible CompatibilityFunc
}

type modulePreparation struct {
	spec      BatchModule
	status    string
	reason    string
	inherited bool
	node      *resourceNode
}

func (b *Batch) ID() string           { return b.id }
func (b *Batch) Baseline() int64      { return b.baseline }
func (b *Batch) CreatedAt() time.Time { return b.created }
func (b *Batch) Status() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Reason reports the batch-level failure, e.g. an unsatisfied compatibility
// condition.
func (b *Batch) Reason() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reason
}

// Ready reports whether every required module is prepared and all declared
// compatibility conditions hold. Confirm fails unless Ready is true.
func (b *Batch) Ready() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != batchPrepared {
		return false
	}
	for _, module := range b.modules {
		if module.spec.Required && module.status != "ready" {
			return false
		}
	}
	return true
}

// Modules returns the readiness, occupied resource and failure reason of
// every registered module, including modules not part of the batch (reported
// as "unchanged" and inherited from the baseline).
func (b *Batch) Modules() []ModuleState {
	m := b.manager
	m.stateMu.RLock()
	order := append([]string(nil), m.order...)
	active := m.active.Load()
	m.stateMu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()
	states := make([]ModuleState, 0, len(order))
	for _, name := range order {
		state := ModuleState{Name: name, Status: "unchanged", Inherited: true, Required: false}
		if module, ok := b.modules[name]; ok {
			state.Status = module.status
			state.Required = module.spec.Required
			state.Inherited = module.inherited
			state.Reason = module.reason
			if module.node != nil {
				state.Resource = module.node.resource
			}
		} else if active != nil && active.nodes[name] != nil {
			state.Resource = active.nodes[name].resource
		}
		states = append(states, state)
	}
	return states
}

// Resources returns the recorded disposition of every resource the batch
// created or inherited, covering newly added resources, resources kept by the
// old batch and resources whose cleanup failed.
func (b *Batch) Resources() []ResourceRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]ResourceRecord(nil), b.records...)
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
	batchArchive []*Batch
	revisionSeq  int64
	history      []ReleaseInfo
	attempts     []AttemptRecord
	candidateSeq int64
	batchSeq     int64
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
	cfg, found, err := loadConfig(configPath)
	if err != nil {
		return nil, err
	}
	if found {
		v, err := m.buildVersion(ctx, cfg)
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
		m.recordAttempt("prepare", "", "", m.currentRevision(), 0, 0, false, "failed", ctx.Err())
		return nil, ctx.Err()
	}
	if m.configPath == "" {
		err := errors.New("configuration path is empty")
		m.recordAttempt("prepare", "", "", 0, 0, 0, false, "failed", err)
		return nil, err
	}
	m.stateMu.RLock()
	closed := m.closed
	m.stateMu.RUnlock()
	if closed {
		err := errors.New("runtime manager is closed")
		m.recordAttempt("prepare", "", "", m.currentRevision(), 0, 0, false, "failed", err)
		return nil, err
	}

	baseline := m.currentRevision()
	version, err := m.buildVersion(ctx, cfg)
	if err != nil {
		m.recordAttempt("prepare", "", "", baseline, 0, 0, false, "failed", err)
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
		abandonErr := m.releaseVersion(context.WithoutCancel(ctx), version)
		err := errors.Join(errors.New("runtime manager is closed"), abandonErr)
		m.recordAttempt("prepare", candidate.id, "", baseline, 0, 0, false, "failed", err)
		return nil, err
	}
	m.candidates[candidate.id] = candidate
	m.stateMu.Unlock()
	m.recordAttempt("prepare", candidate.id, "", baseline, 0, 0, false, "prepared", nil)
	return candidate, nil
}

// PrepareBatch validates and prepares a joint release candidate spanning
// several modules. Modules not listed keep their baseline resources, which the
// batch pins for its whole lifetime so the confirmed set is always mutually
// compatible. Required module failures and compatibility failures leave the
// batch open for inspection in a failed/incompatible state; the returned
// error carries the same reason. Two batches may be prepared concurrently.
func (m *Manager) PrepareBatch(ctx context.Context, spec BatchSpec) (*Batch, error) {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	m.stateMu.RLock()
	closed := m.closed
	m.stateMu.RUnlock()
	if closed {
		err := errors.New("runtime manager is closed")
		m.recordAttempt("prepare-batch", "", "", m.currentRevision(), 0, 0, false, "failed", err)
		return nil, err
	}
	if m.configPath == "" {
		err := errors.New("configuration path is empty")
		m.recordAttempt("prepare-batch", "", "", 0, 0, 0, false, "failed", err)
		return nil, err
	}
	baseline := m.currentRevision()
	if baseline == 0 {
		err := errors.New("runtime is not started")
		m.recordAttempt("prepare-batch", "", "", 0, 0, 0, false, "failed", err)
		return nil, err
	}
	if len(spec.Modules) == 0 {
		err := errors.New("batch must contain at least one module")
		m.recordAttempt("prepare-batch", "", "", baseline, 0, 0, false, "failed", err)
		return nil, err
	}

	m.stateMu.Lock()
	m.batchSeq++
	batch := &Batch{
		manager:    m,
		id:         fmt.Sprintf("batch-%d", m.batchSeq),
		baseline:   baseline,
		created:    time.Now().UTC(),
		state:      batchPreparing,
		modules:    make(map[string]*modulePreparation, len(spec.Modules)),
		nodes:      make(map[string]*resourceNode),
		order:      nil,
		compatible: spec.Compatible,
	}
	m.stateMu.Unlock()

	selected := make(map[string]BatchModule, len(spec.Modules))
	for _, moduleSpec := range spec.Modules {
		if _, ok := m.modules[moduleSpec.Name]; !ok {
			err := fmt.Errorf("unknown module %q", moduleSpec.Name)
			m.failBatch(batch, err)
			m.recordAttempt("prepare-batch", "", batch.id, baseline, 0, 0, false, "failed", err)
			return batch, err
		}
		if _, dup := selected[moduleSpec.Name]; dup {
			err := fmt.Errorf("module %q listed more than once", moduleSpec.Name)
			m.failBatch(batch, err)
			m.recordAttempt("prepare-batch", "", batch.id, baseline, 0, 0, false, "failed", err)
			return batch, err
		}
		selected[moduleSpec.Name] = moduleSpec
	}

	order, err := m.startupOrder()
	if err != nil {
		m.failBatch(batch, err)
		m.recordAttempt("prepare-batch", "", batch.id, baseline, 0, 0, false, "failed", err)
		return batch, err
	}
	m.stateMu.RLock()
	active := m.active.Load()
	m.stateMu.RUnlock()

	// Pin every baseline resource first; failed batches therefore always keep
	// the old set alive and never create mixed reads.
	for _, name := range order {
		node := active.nodes[name]
		node.acquire()
		batch.nodes[name] = node
		batch.order = append(batch.order, name)
		batch.modules[name] = &modulePreparation{status: "unchanged", inherited: true, node: node}
	}

	var requiredFailed bool
	for _, name := range order {
		moduleSpec, ok := selected[name]
		if !ok {
			continue
		}
		if err := ctx.Err(); err != nil {
			m.finishBatchFailure(context.WithoutCancel(ctx), batch, moduleSpec, "canceled: "+err.Error())
			requiredFailed = true
			break
		}
		moduleCfg := ModuleConfig{Name: name, Raw: moduleSpec.Raw}
		if validateErr := m.modules[name].Validate(ctx, moduleCfg); validateErr != nil {
			reason := fmt.Sprintf("validate module %q: %v", name, validateErr)
			m.finishBatchFailure(context.WithoutCancel(ctx), batch, moduleSpec, reason)
			if moduleSpec.Required {
				requiredFailed = true
			}
			continue
		}
		resource, prepareErr := m.modules[name].Prepare(ctx, moduleCfg)
		if prepareErr != nil {
			reason := fmt.Sprintf("prepare module %q: %v", name, prepareErr)
			m.finishBatchFailure(context.WithoutCancel(ctx), batch, moduleSpec, reason)
			if moduleSpec.Required {
				requiredFailed = true
			}
			continue
		}
		if resource == nil {
			resource = noopResource{}
		}
		// The new resource replaces the inherited hold: release the baseline
		// pin (it stays alive via the active version) and keep the new node.
		inherited := batch.nodes[name]
		newNode := newResourceNode(name, resource, 1)
		batch.nodes[name] = newNode
		if _, releaseErr := inherited.release(context.WithoutCancel(ctx)); releaseErr != nil {
			// Baseline close failure is recorded but does not affect readiness
			// of the new resource.
			batch.appendRecord(ResourceRecord{Module: name, Kind: ResourceCleanupFailed, New: false, InUse: false, Err: releaseErr.Error(), Time: time.Now().UTC()})
		}
		batch.modules[name] = &modulePreparation{spec: moduleSpec, status: "ready", node: newNode}
	}

	if requiredFailed {
		err := errors.New("one or more required modules failed preparation")
		m.failBatch(batch, err)
		m.cleanupFailedBatch(context.WithoutCancel(ctx), batch)
		m.recordAttempt("prepare-batch", "", batch.id, baseline, 0, 0, false, "failed", err)
		return batch, err
	}

	if batch.compatible != nil {
		compatErr := batch.compatible(batch.assembledViewWith(active))
		if compatErr != nil {
			batch.mu.Lock()
			batch.state = batchIncompatible
			batch.reason = compatErr.Error()
			batch.mu.Unlock()
			m.cleanupFailedBatch(context.WithoutCancel(ctx), batch)
			m.archiveBatch(batch)
			m.recordAttempt("prepare-batch", "", batch.id, baseline, 0, 0, false, "incompatible", compatErr)
			return batch, compatErr
		}
	}

	batch.mu.Lock()
	batch.state = batchPrepared
	for _, moduleSpec := range selected {
		if prep, ok := batch.modules[moduleSpec.Name]; ok {
			prep.spec = moduleSpec
		}
	}
	batch.mu.Unlock()
	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		err := errors.New("runtime manager is closed")
		m.cleanupFailedBatch(context.WithoutCancel(ctx), batch)
		m.recordAttempt("prepare-batch", "", batch.id, baseline, 0, 0, false, "failed", err)
		return batch, err
	}
	m.batches[batch.id] = batch
	m.stateMu.Unlock()
	m.recordAttempt("prepare-batch", "", batch.id, baseline, 0, 0, false, "prepared", nil)
	return batch, nil
}

// Confirm atomically publishes the assembled joint configuration: new
// resources for ready modules plus unchanged baseline resources for the rest.
// It fails with ErrBatchNotReady unless every required module is ready and the
// declared compatibility conditions still hold, and with ErrBaselineChanged
// when the baseline revision moved before confirmation. Readers always switch
// from one complete compatible set to another; old resources wait for
// in-flight leases and cannot be reused by the new set.
func (b *Batch) Confirm(ctx context.Context) error {
	m := b.manager
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	b.mu.Lock()
	state := b.state
	b.mu.Unlock()
	if state != batchPrepared {
		return fmt.Errorf("%w: %s", ErrBatchClosed, state)
	}
	for _, module := range b.snapshotModules() {
		if module.spec.Required && module.status != "ready" {
			err := fmt.Errorf("%w: required module %q is %s", ErrBatchNotReady, module.spec.Name, module.status)
			m.recordAttempt("confirm", "", b.id, b.baseline, 0, 0, false, "not-ready", err)
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		m.recordAttempt("confirm", "", b.id, b.baseline, 0, 0, false, "failed", err)
		return err
	}

	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		err := errors.New("runtime manager is closed")
		m.recordAttempt("confirm", "", b.id, b.baseline, 0, 0, false, "failed", err)
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
		m.recordAttempt("confirm", "", b.id, b.baseline, currentRevision, 0, false, "conflict", err)
		return err
	}
	if err := b.checkCompatibility(current); err != nil {
		m.stateMu.Unlock()
		m.recordAttempt("confirm", "", b.id, b.baseline, current.revision, 0, false, "incompatible", err)
		return err
	}
	assembled := b.assembleConfigLocked(current)
	m.stateMu.Unlock()

	if err := persistConfig(m.configPath, assembled); err != nil {
		m.recordAttempt("confirm", "", b.id, b.baseline, 0, 0, false, "failed", err)
		return err
	}

	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		err := errors.New("runtime manager is closed")
		m.recordAttempt("confirm", "", b.id, b.baseline, 0, 0, false, "failed", err)
		return err
	}
	current = m.active.Load()
	if current == nil || current.revision != b.baseline {
		currentRevision := int64(0)
		if current != nil {
			currentRevision = current.revision
		}
		m.stateMu.Unlock()
		err := fmt.Errorf("%w: batch baseline %d, current revision %d", ErrBaselineChanged, b.baseline, currentRevision)
		m.recordAttempt("confirm", "", b.id, b.baseline, currentRevision, 0, false, "conflict", err)
		return err
	}

	nodes := make(map[string]*resourceNode, len(b.nodes))
	order := make([]string, 0, len(b.nodes))
	for _, name := range m.order {
		node, ok := b.nodes[name]
		if !ok {
			m.stateMu.Unlock()
			err := fmt.Errorf("batch is missing prepared module %q", name)
			m.recordAttempt("confirm", "", b.id, b.baseline, 0, 0, false, "failed", err)
			return err
		}
		nodes[name] = node
		order = append(order, name)
	}
	for _, node := range nodes {
		node.acquire()
	}
	newVersion := &version{
		config: cloneConfig(assembled),
		nodes:  nodes,
		order:  order,
	}
	m.revisionSeq++
	revision := m.revisionSeq
	newVersion.revision = revision
	m.active.Store(newVersion)
	m.retireVersion(current)
	m.appendHistory(ReleaseInfo{
		Revision:    revision,
		Config:      cloneConfig(assembled),
		PublishedAt: time.Now().UTC(),
		Source:      "batch:" + b.id,
	})
	b.mu.Lock()
	b.state = batchConfirmed
	b.mu.Unlock()
	delete(m.batches, b.id)
	m.batchArchive = append(m.batchArchive, b)
	m.stateMu.Unlock()

	// Release the batch's own holds; every node stays alive through the new
	// active version until its in-flight leases complete.
	_ = m.releaseNodes(context.WithoutCancel(ctx), b.nodes, b.order)
	m.recordAttempt("confirm", "", b.id, b.baseline, b.baseline, revision, true, "switched", nil)
	return nil
}

// Abandon releases the resources created by the batch. Inherited baseline
// resources remain in service; resources shared with other candidates are kept
// until their last holder goes away. A queryable record distinguishes new,
// kept and cleanup-failed resources. Abandon is idempotent.
func (b *Batch) Abandon(ctx context.Context) error {
	b.mu.Lock()
	state := b.state
	if state == batchAbandoned {
		b.mu.Unlock()
		return nil
	}
	if state == batchConfirmed {
		b.mu.Unlock()
		return fmt.Errorf("%w: confirmed", ErrBatchClosed)
	}
	b.state = batchAbandoned
	b.mu.Unlock()

	err := b.manager.disposeBatchResources(ctx, b)
	m := b.manager
	m.forgetBatch(b)
	m.archiveBatch(b)
	m.recordAttempt("abandon-batch", "", b.id, b.baseline, 0, 0, false, "abandoned", err)
	return err
}

func (b *Batch) snapshotModules() map[string]*modulePreparation {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]*modulePreparation, len(b.modules))
	for name, prep := range b.modules {
		copy := *prep
		out[name] = &copy
	}
	return out
}

func (b *Batch) appendRecord(record ResourceRecord) {
	b.mu.Lock()
	b.records = append(b.records, record)
	b.mu.Unlock()
}

// checkCompatibility evaluates the declared conditions against the full
// assembled view (newly prepared module configs plus baseline configs for
// unchanged and skipped modules).
func (b *Batch) checkCompatibility(active *version) error {
	if b.compatible == nil {
		return nil
	}
	view := make(map[string]ModuleConfig, len(b.nodes))
	for name := range b.nodes {
		raw := active.config[name]
		if prep, ok := b.modules[name]; ok && prep.status == "ready" {
			raw = prep.spec.Raw
		}
		view[name] = ModuleConfig{Name: name, Raw: raw}
	}
	return b.compatible(view)
}

func (b *Batch) assembledView() map[string]ModuleConfig {
	m := b.manager
	m.stateMu.RLock()
	active := m.active.Load()
	m.stateMu.RUnlock()
	return b.assembledViewWith(active)
}

func (b *Batch) assembledViewWith(active *version) map[string]ModuleConfig {
	view := make(map[string]ModuleConfig, len(b.nodes))
	for name := range b.nodes {
		raw := active.config[name]
		if prep, ok := b.modules[name]; ok && prep.status == "ready" {
			raw = prep.spec.Raw
		}
		view[name] = ModuleConfig{Name: name, Raw: raw}
	}
	return view
}

func (b *Batch) assembleConfigLocked(active *version) Config {
	cfg := make(Config, len(b.nodes))
	for name := range b.nodes {
		raw := active.config[name]
		if prep, ok := b.modules[name]; ok && prep.status == "ready" {
			raw = prep.spec.Raw
		}
		cfg[name] = append(json.RawMessage(nil), raw...)
	}
	return cfg
}

// finishBatchFailure records a failed module. Required failures move the
// module to "failed"; optional failures degrade it to "skipped" and keep the
// inherited baseline resource in the assembled set.
func (m *Manager) finishBatchFailure(_ context.Context, batch *Batch, spec BatchModule, reason string) {
	batch.mu.Lock()
	defer batch.mu.Unlock()
	prep := batch.modules[spec.Name]
	prep.spec = spec
	prep.reason = reason
	if spec.Required {
		prep.status = "failed"
	} else {
		prep.status = "skipped"
	}
}

func (m *Manager) failBatch(batch *Batch, cause error) {
	batch.mu.Lock()
	batch.state = batchFailed
	batch.reason = cause.Error()
	batch.mu.Unlock()
	m.archiveBatch(batch)
}

// cleanupFailedBatch releases everything a failed batch pinned: newly created
// resources are closed (or marked cleanup-failed), inherited pins are dropped
// while the baseline keeps serving.
func (m *Manager) cleanupFailedBatch(ctx context.Context, batch *Batch) {
	_ = m.disposeBatchResources(ctx, batch)
	m.archiveBatch(batch)
}

// disposeBatchResources drops the batch's single hold on every node in reverse
// dependency order and records the disposition of each resource.
func (m *Manager) disposeBatchResources(ctx context.Context, batch *Batch) error {
	nodes := batch.nodes
	order := batch.order
	modules := batch.modules
	var errs []error
	records := make([]ResourceRecord, 0, len(order))
	for i := len(order) - 1; i >= 0; i-- {
		name := order[i]
		node := nodes[name]
		if node == nil {
			continue
		}
		isNew := false
		if prep, ok := modules[name]; ok && prep.status == "ready" {
			isNew = !prep.inherited
		}
		closed, closeErr := node.release(ctx)
		users, _, _ := node.state()
		record := ResourceRecord{
			Module: name,
			New:    isNew,
			InUse:  users > 0,
			Time:   time.Now().UTC(),
		}
		switch {
		case closeErr != nil && isNew:
			record.Kind = ResourceCleanupFailed
			record.Err = closeErr.Error()
			errs = append(errs, fmt.Errorf("close module %q: %w", name, closeErr))
		case isNew:
			record.Kind = ResourceNew
		default:
			record.Kind = ResourceKept
		}
		_ = closed
		records = append(records, record)
	}
	batch.mu.Lock()
	batch.records = append(batch.records, records...)
	batch.mu.Unlock()
	return errors.Join(errs...)
}

// Batches returns joint batches that are still open (prepared or blocked).
func (m *Manager) Batches() []BatchInfo {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	infos := make([]BatchInfo, 0, len(m.batches))
	for _, batch := range m.batches {
		infos = append(infos, batch.info())
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].ID < infos[j].ID })
	return infos
}

// BatchRecords returns every batch the manager has seen, including batches
// whose preparation failed, abandoned batches and confirmed batches, oldest
// first. Each batch keeps its readiness view and resource disposition records.
func (m *Manager) BatchRecords() []BatchInfo {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	ids := make(map[string]bool, len(m.batchArchive))
	infos := make([]BatchInfo, 0, len(m.batchArchive)+len(m.batches))
	for _, batch := range m.batchArchive {
		infos = append(infos, batch.info())
		ids[batch.id] = true
	}
	for _, batch := range m.batches {
		if !ids[batch.id] {
			infos = append(infos, batch.info())
		}
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].ID < infos[j].ID })
	return infos
}

// BatchInfo is a snapshot of a joint batch.
type BatchInfo struct {
	ID        string
	Baseline  int64
	Status    string
	Reason    string
	CreatedAt time.Time
	Modules   []ModuleState
	Resources []ResourceRecord
}

func (b *Batch) info() BatchInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	states := make([]ModuleState, 0, len(b.order))
	m := b.manager
	m.stateMu.RLock()
	order := append([]string(nil), m.order...)
	active := m.active.Load()
	m.stateMu.RUnlock()
	for _, name := range order {
		state := ModuleState{Name: name, Status: "unchanged", Inherited: true}
		if module, ok := b.modules[name]; ok {
			state.Status = module.status
			state.Required = module.spec.Required
			state.Inherited = module.inherited
			state.Reason = module.reason
			if module.node != nil {
				state.Resource = module.node.resource
			}
		} else if active != nil && active.nodes[name] != nil {
			state.Resource = active.nodes[name].resource
		}
		states = append(states, state)
	}
	return BatchInfo{
		ID:        b.id,
		Baseline:  b.baseline,
		Status:    b.state,
		Reason:    b.reason,
		CreatedAt: b.created,
		Modules:   states,
		Resources: append([]ResourceRecord(nil), b.records...),
	}
}

func (m *Manager) forgetBatch(batch *Batch) {
	m.stateMu.Lock()
	delete(m.batches, batch.id)
	m.stateMu.Unlock()
}

func (m *Manager) archiveBatch(batch *Batch) {
	m.stateMu.Lock()
	for _, existing := range m.batchArchive {
		if existing.id == batch.id {
			m.stateMu.Unlock()
			return
		}
	}
	m.batchArchive = append(m.batchArchive, batch)
	m.stateMu.Unlock()
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

func (m *Manager) recordAttempt(kind, candidateID, batchID string, baseline, fromRevision, toRevision int64, switched bool, result string, err error) {
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

// Rollback prepares and publishes the configuration of a past revision, pinned
// to the currently active revision. It performs a real switch and therefore
// consumes a new revision number; the old revision stays in history. A
// rollback is itself a joint switch: readers move atomically from one complete
// configuration set to the other and old resources survive in-flight leases.
func (m *Manager) Rollback(ctx context.Context, revision int64) error {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	info, ok := m.releaseInfo(revision)
	if !ok {
		err := fmt.Errorf("%w: %d", ErrRevisionMissing, revision)
		m.recordAttempt("rollback", "", "", m.currentRevision(), 0, 0, false, "failed", err)
		return err
	}
	if m.closed {
		err := errors.New("runtime manager is closed")
		m.recordAttempt("rollback", "", "", m.currentRevision(), 0, 0, false, "failed", err)
		return err
	}
	baseline := m.currentRevision()
	if baseline == 0 {
		err := errors.New("runtime is not started")
		m.recordAttempt("rollback", "", "", 0, 0, 0, false, "failed", err)
		return err
	}
	newVersion, err := m.buildVersion(ctx, info.Config)
	if err != nil {
		m.recordAttempt("rollback", "", "", baseline, 0, 0, false, "failed", err)
		return err
	}
	if err := persistConfig(m.configPath, newVersion.config); err != nil {
		_ = m.releaseVersion(context.WithoutCancel(ctx), newVersion)
		m.recordAttempt("rollback", "", "", baseline, 0, 0, false, "failed", err)
		return err
	}
	m.stateMu.Lock()
	if m.closed {
		m.stateMu.Unlock()
		_ = m.releaseVersion(context.WithoutCancel(ctx), newVersion)
		err := errors.New("runtime manager is closed")
		m.recordAttempt("rollback", "", "", baseline, 0, 0, false, "failed", err)
		return err
	}
	current := m.active.Load()
	if current == nil || current.revision != baseline {
		m.stateMu.Unlock()
		_ = m.releaseVersion(context.WithoutCancel(ctx), newVersion)
		err := ErrBaselineChanged
		m.recordAttempt("rollback", "", "", baseline, m.currentRevision(), 0, false, "conflict", err)
		return err
	}
	m.revisionSeq++
	newRevision := m.revisionSeq
	newVersion.revision = newRevision
	m.active.Store(newVersion)
	m.retireVersion(current)
	m.appendHistory(ReleaseInfo{
		Revision:    newRevision,
		Config:      cloneConfig(newVersion.config),
		PublishedAt: time.Now().UTC(),
		Source:      fmt.Sprintf("rollback:r%d", revision),
	})
	m.stateMu.Unlock()
	m.recordAttempt("rollback", "", "", baseline, baseline, newRevision, true, "switched", nil)
	return nil
}

// buildVersion validates and prepares every module in dependency order and
// returns a version holding one reference to each resource. Failed builds
// release the resources they created.
func (m *Manager) buildVersion(ctx context.Context, cfg Config) (*version, error) {
	order, err := m.startupOrder()
	if err != nil {
		return nil, err
	}
	v := &version{
		config: cloneConfig(cfg),
		nodes:  make(map[string]*resourceNode, len(order)),
		order:  order,
	}
	for _, name := range order {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, m.releaseVersion(context.WithoutCancel(ctx), v))
		}
		if err := m.modules[name].Validate(ctx, ModuleConfig{Name: name, Raw: cfg[name]}); err != nil {
			return nil, errors.Join(fmt.Errorf("validate module %q: %w", name, err), m.releaseVersion(context.WithoutCancel(ctx), v))
		}
	}
	for _, name := range order {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, m.releaseVersion(context.WithoutCancel(ctx), v))
		}
		resource, err := m.modules[name].Prepare(ctx, ModuleConfig{Name: name, Raw: cfg[name]})
		if err != nil {
			return nil, errors.Join(fmt.Errorf("prepare module %q: %w", name, err), m.releaseVersion(context.WithoutCancel(ctx), v))
		}
		if resource == nil {
			resource = noopResource{}
		}
		v.nodes[name] = newResourceNode(name, resource, 1)
	}
	return v, nil
}

// releaseVersion drops the version's single hold on every node, in reverse
// dependency order. Shared nodes stay open for their remaining holders.
func (m *Manager) releaseVersion(ctx context.Context, v *version) error {
	return m.releaseNodes(ctx, v.nodes, v.order)
}

func (m *Manager) releaseNodes(ctx context.Context, nodes map[string]*resourceNode, order []string) error {
	var errs []error
	for i := len(order) - 1; i >= 0; i-- {
		name := order[i]
		node := nodes[name]
		if node == nil {
			continue
		}
		if _, err := node.release(ctx); err != nil {
			errs = append(errs, fmt.Errorf("close module %q: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) retireVersion(v *version) {
	item := &retiredVersion{version: v, done: make(chan error, 1)}
	m.retired = append(m.retired, item)
	go func() {
		// Reads started against this version have a definite batch/revision
		// ownership: they keep the lease (and therefore every shared node)
		// alive until Release, even though the version is no longer active.
		v.leases.Wait()
		err := m.releaseVersion(context.Background(), item.version)
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

	for _, candidate := range candidates {
		candidate.mu.Lock()
		if candidate.state == candidatePrepared {
			candidate.state = candidateAbandoned
			candidate.mu.Unlock()
			_ = m.releaseVersion(ctx, candidate.version)
		} else {
			candidate.mu.Unlock()
		}
	}
	for _, batch := range batches {
		batch.mu.Lock()
		if batch.state == batchPrepared || batch.state == batchIncompatible || batch.state == batchPreparing {
			batch.state = batchAbandoned
			batch.mu.Unlock()
			_ = m.disposeBatchResources(ctx, batch)
			m.archiveBatch(batch)
		} else {
			batch.mu.Unlock()
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
		errCh <- m.releaseVersion(ctx, active)
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

func loadConfig(path string) (Config, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, false, fmt.Errorf("load configuration: %w", err)
	}
	return cfg, true, nil
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
