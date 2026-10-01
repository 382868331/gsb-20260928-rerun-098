package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testConfig struct {
	Generation   int  `json:"generation"`
	FailValidate bool `json:"failValidate"`
	FailPrepare  bool `json:"failPrepare"`
}

type testResource struct {
	module     string
	generation int
	closes     *closeLog
}

func (r *testResource) Close(context.Context) error {
	r.closes.record(r.module, r.generation)
	return nil
}

type testModule struct {
	name   string
	deps   []string
	closes *closeLog
}

func (m *testModule) Name() string { return m.name }

func (m *testModule) Dependencies() []string { return m.deps }

func (m *testModule) Validate(_ context.Context, config ModuleConfig) error {
	var cfg testConfig
	if err := config.Decode(&cfg); err != nil {
		return err
	}
	if cfg.FailValidate {
		return errors.New("forced validation failure")
	}
	return nil
}

func (m *testModule) Prepare(_ context.Context, config ModuleConfig) (Resource, error) {
	var cfg testConfig
	if err := config.Decode(&cfg); err != nil {
		return nil, err
	}
	if cfg.FailPrepare {
		return nil, errors.New("forced preparation failure")
	}
	return &testResource{module: m.name, generation: cfg.Generation, closes: m.closes}, nil
}

type closeLog struct {
	mu     sync.Mutex
	events []closeEvent
}

type closeEvent struct {
	module     string
	generation int
}

func (l *closeLog) record(module string, generation int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, closeEvent{module: module, generation: generation})
}

func (l *closeLog) snapshot() []closeEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]closeEvent(nil), l.events...)
}

func TestReloadFailureKeepsCompleteOldVersionAndPersistsIt(t *testing.T) {
	ctx := context.Background()
	closes := &closeLog{}
	db := &testModule{name: "db", closes: closes}
	service := &testModule{name: "service", deps: []string{"db"}, closes: closes}
	api := &testModule{name: "api", deps: []string{"service"}, closes: closes}

	manager, err := NewManager(filepath.Join(t.TempDir(), "config.json"), api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(ctx, configFor(1, map[string]bool{})); err != nil {
		t.Fatal(err)
	}

	oldLease, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, oldLease); generation != 1 {
		t.Fatalf("old generation = %d, want 1", generation)
	}

	if err := manager.Reload(ctx, configFor(2, map[string]bool{})); err != nil {
		t.Fatal(err)
	}
	newLease, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, newLease); generation != 2 {
		t.Fatalf("new generation = %d, want 2", generation)
	}
	newLease.Release()

	if generation := leaseGeneration(t, oldLease); generation != 1 {
		t.Fatalf("held old lease changed to generation %d", generation)
	}
	if err := manager.Reload(ctx, configFor(3, map[string]bool{"service": true})); err == nil {
		t.Fatal("forced preparation failure unexpectedly succeeded")
	}
	if generation := leaseGeneration(t, newLease); generation != 2 {
		t.Fatalf("generation after failed reload = %d, want 2", generation)
	}

	if err := manager.Reload(ctx, configFor(4, map[string]bool{"api": true, "validate": true})); err == nil {
		t.Fatal("forced validation failure unexpectedly succeeded")
	}
	if generation := leaseGeneration(t, newLease); generation != 2 {
		t.Fatalf("generation after failed validation = %d, want 2", generation)
	}

	for _, event := range closes.snapshot() {
		if event.generation == 2 {
			t.Fatalf("generation 2 resource closed while its lease was active: %+v", event)
		}
	}
	oldLease.Release()
	newLease.Release()

	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(ctx, manager.configPath, api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	restartLease, err := restarted.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, restartLease); generation != 2 {
		t.Fatalf("restart generation = %d, want 2", generation)
	}
	restartLease.Release()
	if err := restarted.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentLeasesAlwaysObserveOneCompleteVersion(t *testing.T) {
	ctx := context.Background()
	closes := &closeLog{}
	db := &testModule{name: "db", closes: closes}
	service := &testModule{name: "service", deps: []string{"db"}, closes: closes}
	api := &testModule{name: "api", deps: []string{"service"}, closes: closes}
	manager, err := NewManager(filepath.Join(t.TempDir(), "config.json"), api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}

	var readers sync.WaitGroup
	var mixed atomic.Bool
	stop := make(chan struct{})
	for i := 0; i < 16; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					lease, err := manager.Lease()
					if err != nil {
						return
					}
					if generation := leaseGeneration(t, lease); generation != 1 && generation != 2 {
						mixed.Store(true)
					}
					lease.Release()
				}
			}
		}()
	}

	if err := manager.Reload(ctx, configFor(2, nil)); err != nil {
		t.Fatal(err)
	}
	close(stop)
	readers.Wait()
	if mixed.Load() {
		t.Fatal("reader observed resources from different configuration versions")
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOldResourcesWaitForInflightLeaseThenCloseInReverseOrder(t *testing.T) {
	ctx := context.Background()
	closes := &closeLog{}
	db := &testModule{name: "db", closes: closes}
	service := &testModule{name: "service", deps: []string{"db"}, closes: closes}
	api := &testModule{name: "api", deps: []string{"service"}, closes: closes}
	manager, err := NewManager(filepath.Join(t.TempDir(), "config.json"), api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}
	oldLease, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Reload(ctx, configFor(2, nil)); err != nil {
		t.Fatal(err)
	}

	for _, event := range closes.snapshot() {
		if event.generation == 1 {
			t.Fatalf("old resource closed before inflight work finished: %+v", event)
		}
	}
	oldLease.Release()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var oldCount int
		for _, event := range closes.snapshot() {
			if event.generation == 1 {
				oldCount++
			}
		}
		if oldCount >= 3 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	wantEvents := []closeEvent{
		{module: "api", generation: 1},
		{module: "service", generation: 1},
		{module: "db", generation: 1},
	}
	var oldEvents []closeEvent
	for _, event := range closes.snapshot() {
		if event.generation == 1 {
			oldEvents = append(oldEvents, event)
		}
	}
	if len(oldEvents) != len(wantEvents) {
		t.Fatalf("old close events = %v, want %v", oldEvents, wantEvents)
	}
	for i, wantEvent := range wantEvents {
		if oldEvents[i] != wantEvent {
			t.Fatalf("old close events = %v, want %v", oldEvents, wantEvents)
		}
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOnlyPublishedConfigurationIsPersistedAndRecovered(t *testing.T) {
	ctx := context.Background()
	closes := &closeLog{}
	db := &testModule{name: "db", closes: closes}
	service := &testModule{name: "service", deps: []string{"db"}, closes: closes}
	api := &testModule{name: "api", deps: []string{"service"}, closes: closes}
	configPath := filepath.Join(t.TempDir(), "config.json")

	manager, err := NewManager(configPath, api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if generation := persistedGeneration(t, configPath); generation != 1 {
		t.Fatalf("persisted generation after initial publish = %d, want 1", generation)
	}

	restarted, err := Open(ctx, configPath, api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Start(ctx, configFor(2, nil)); err == nil {
		t.Fatal("duplicate start unexpectedly succeeded")
	}
	if generation := persistedGeneration(t, configPath); generation != 1 {
		t.Fatalf("persisted generation after duplicate start = %d, want 1", generation)
	}
	closesBeforeFailedReload := len(closes.snapshot())
	if err := restarted.Reload(ctx, configFor(3, map[string]bool{"service": true})); err == nil {
		t.Fatal("failed preparation unexpectedly succeeded")
	}
	if generation := persistedGeneration(t, configPath); generation != 1 {
		t.Fatalf("persisted generation after failed preparation = %d, want 1", generation)
	}
	var preparedGenerationThreeClosed bool
	for _, event := range closes.snapshot()[closesBeforeFailedReload:] {
		if event.generation == 3 {
			preparedGenerationThreeClosed = true
		}
		if event.generation == 1 {
			t.Fatalf("active generation 1 resource closed during failed candidate: %+v", event)
		}
	}
	if !preparedGenerationThreeClosed {
		t.Fatal("prepared generation 3 resource was not abandoned")
	}
	if err := restarted.Reload(ctx, configFor(4, map[string]bool{"validate": true})); err == nil {
		t.Fatal("failed validation unexpectedly succeeded")
	}
	if generation := persistedGeneration(t, configPath); generation != 1 {
		t.Fatalf("persisted generation after failed validation = %d, want 1", generation)
	}

	if err := restarted.Reload(ctx, configFor(2, nil)); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if generation := persistedGeneration(t, configPath); generation != 2 {
		t.Fatalf("persisted generation after publish and close = %d, want 2", generation)
	}
	var activeGenerationTwoClosed int
	for _, event := range closes.snapshot() {
		if event.generation == 2 {
			activeGenerationTwoClosed++
		}
	}
	if activeGenerationTwoClosed != 3 {
		t.Fatalf("closed generation 2 resources = %d, want 3", activeGenerationTwoClosed)
	}

	reopened, err := Open(ctx, configPath, api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := reopened.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, lease); generation != 2 {
		t.Fatalf("recovered generation = %d, want 2", generation)
	}
	lease.Release()
	if err := reopened.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStartupAndShutdownFollowDependencyOrder(t *testing.T) {
	order, err := (&Manager{modules: map[string]Module{
		"db":      &testModule{name: "db"},
		"service": &testModule{name: "service", deps: []string{"db"}},
		"api":     &testModule{name: "api", deps: []string{"service"}},
	}, order: []string{"api", "service", "db"}}).startupOrder()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"db", "service", "api"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("startup order = %v, want %v", order, want)
		}
	}
}

func persistedGeneration(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	var moduleCfg testConfig
	if err := json.Unmarshal(cfg["api"], &moduleCfg); err != nil {
		t.Fatal(err)
	}
	return moduleCfg.Generation
}

func configFor(generation int, failures map[string]bool) Config {
	makeConfig := func(failValidate, failPrepare bool) json.RawMessage {
		data, _ := json.Marshal(testConfig{
			Generation:   generation,
			FailValidate: failValidate,
			FailPrepare:  failPrepare,
		})
		return data
	}
	validate := failures["validate"]
	return Config{
		"db":      makeConfig(validate, failures["db"]),
		"service": makeConfig(validate, failures["service"]),
		"api":     makeConfig(validate, failures["api"]),
	}
}

func leaseGeneration(t *testing.T, lease *Lease) int {
	t.Helper()
	db := lease.Resource("db").(*testResource)
	service := lease.Resource("service").(*testResource)
	api := lease.Resource("api").(*testResource)
	if db.generation != service.generation || service.generation != api.generation {
		t.Fatalf("mixed version: db=%d service=%d api=%d", db.generation, service.generation, api.generation)
	}
	return api.generation
}

func newTestManager(t *testing.T) (*Manager, *closeLog, *testModule, *testModule, *testModule) {
	t.Helper()
	closes := &closeLog{}
	db := &testModule{name: "db", closes: closes}
	service := &testModule{name: "service", deps: []string{"db"}, closes: closes}
	api := &testModule{name: "api", deps: []string{"service"}, closes: closes}
	manager, err := NewManager(filepath.Join(t.TempDir(), "config.json"), api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	return manager, closes, db, service, api
}

func TestPreparedCandidateIsNotActiveUntilPublished(t *testing.T) {
	ctx := context.Background()
	manager, closes, _, _, _ := newTestManager(t)
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}

	candidate, err := manager.Prepare(ctx, configFor(2, nil))
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Baseline() != 1 {
		t.Fatalf("baseline = %d, want 1", candidate.Baseline())
	}
	if candidate.Status() != "prepared" {
		t.Fatalf("status = %q, want prepared", candidate.Status())
	}
	infos := manager.Candidates()
	if len(infos) != 1 || infos[0].ID != candidate.ID() || infos[0].Baseline != 1 {
		t.Fatalf("candidates = %+v", infos)
	}
	lease, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, lease); generation != 1 {
		t.Fatalf("active generation = %d, want 1 after prepare", generation)
	}
	lease.Release()
	if generation := persistedGeneration(t, manager.configPath); generation != 1 {
		t.Fatalf("persisted generation = %d, want 1 after prepare", generation)
	}

	if err := candidate.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	if candidate.Status() != "published" {
		t.Fatalf("status = %q, want published", candidate.Status())
	}
	if infos := manager.Candidates(); len(infos) != 0 {
		t.Fatalf("published candidate still listed: %+v", infos)
	}
	lease, err = manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, lease); generation != 2 {
		t.Fatalf("active generation = %d, want 2 after publish", generation)
	}
	lease.Release()
	if generation := persistedGeneration(t, manager.configPath); generation != 2 {
		t.Fatalf("persisted generation = %d, want 2", generation)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Candidate 2 resources must be closed exactly once on close.
	var gen2 int
	for _, event := range closes.snapshot() {
		if event.generation == 2 {
			gen2++
		}
	}
	if gen2 != 3 {
		t.Fatalf("generation 2 close events = %d, want 3", gen2)
	}
}

func TestPublishConflictWhenBaselineMoved(t *testing.T) {
	ctx := context.Background()
	manager, closes, _, _, _ := newTestManager(t)
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}
	candidateA, err := manager.Prepare(ctx, configFor(2, nil))
	if err != nil {
		t.Fatal(err)
	}
	candidateB, err := manager.Prepare(ctx, configFor(3, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := candidateA.Publish(ctx); err != nil {
		t.Fatal(err)
	}

	err = candidateB.Publish(ctx)
	if !errors.Is(err, ErrBaselineChanged) {
		t.Fatalf("err = %v, want ErrBaselineChanged", err)
	}
	if candidateB.Status() != "prepared" {
		t.Fatalf("conflicting candidate status = %q, want prepared", candidateB.Status())
	}
	lease, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, lease); generation != 2 {
		t.Fatalf("active generation = %d, want 2", generation)
	}
	lease.Release()
	if generation := persistedGeneration(t, manager.configPath); generation != 2 {
		t.Fatalf("persisted generation = %d, want 2", generation)
	}
	if infos := manager.Candidates(); len(infos) != 1 || infos[0].ID != candidateB.ID() {
		t.Fatalf("candidates after conflict = %+v", infos)
	}
	// Stale candidate's resources are still alive; abandoning releases them once.
	before := len(closes.snapshot())
	if err := candidateB.Abandon(ctx); err != nil {
		t.Fatal(err)
	}
	var gen3AfterAbandon int
	for _, event := range closes.snapshot()[before:] {
		if event.generation == 3 {
			gen3AfterAbandon++
		}
		if event.generation == 2 {
			t.Fatalf("active generation 2 closed by abandon: %+v", event)
		}
	}
	if gen3AfterAbandon != 3 {
		t.Fatalf("generation 3 close events after abandon = %d, want 3", gen3AfterAbandon)
	}
	if err := candidateB.Abandon(ctx); err != nil {
		t.Fatalf("idempotent abandon failed: %v", err)
	}
	var gen3Total int
	for _, event := range closes.snapshot() {
		if event.generation == 3 {
			gen3Total++
		}
	}
	if gen3Total != 3 {
		t.Fatalf("generation 3 close events = %d, want 3 (leak or double close)", gen3Total)
	}
	if infos := manager.Candidates(); len(infos) != 0 {
		t.Fatalf("abandoned candidate still listed: %+v", infos)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFailedPrepareReleasesCandidateResourcesAndKeepsService(t *testing.T) {
	ctx := context.Background()
	manager, closes, _, _, _ := newTestManager(t)
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}
	hold, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	before := len(closes.snapshot())
	if _, err := manager.Prepare(ctx, configFor(9, map[string]bool{"service": true})); err == nil {
		t.Fatal("prepare failure unexpectedly succeeded")
	}
	var gen9Closed, gen1Closed int
	for _, event := range closes.snapshot()[before:] {
		switch event.generation {
		case 9:
			gen9Closed++
		case 1:
			gen1Closed++
		}
	}
	if gen9Closed != 1 {
		t.Fatalf("prepared generation 9 resources closed = %d, want 1 (only db)", gen9Closed)
	}
	if gen1Closed != 0 {
		t.Fatalf("active generation 1 resources closed = %d, want 0", gen1Closed)
	}
	if infos := manager.Candidates(); len(infos) != 0 {
		t.Fatalf("failed candidate leaked: %+v", infos)
	}
	hold.Release()
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackRestoresHistoricalRevisionAsNewRevision(t *testing.T) {
	ctx := context.Background()
	manager, _, db, service, api := newTestManager(t)
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}
	c2, err := manager.Prepare(ctx, configFor(2, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := c2.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	c3, err := manager.Prepare(ctx, configFor(3, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := c3.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	if err := manager.Rollback(ctx, 2); err != nil {
		t.Fatal(err)
	}
	lease, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, lease); generation != 2 {
		t.Fatalf("generation after rollback = %d, want 2 config", generation)
	}
	if v := lease.version; v.revision != 4 {
		t.Fatalf("revision after rollback = %d, want 4", v.revision)
	}
	lease.Release()
	history := manager.History()
	if len(history) != 4 {
		t.Fatalf("history len = %d, want 4: %+v", len(history), history)
	}
	if history[3].Revision != 4 || history[1].Revision != 2 || history[3].Source != "rollback:r2" {
		t.Fatalf("rollback history entry = %+v", history[3])
	}
	if generation := persistedGeneration(t, manager.configPath); generation != 2 {
		t.Fatalf("persisted generation = %d, want 2", generation)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, manager.configPath, api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	lease, err = reopened.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, lease); generation != 2 {
		t.Fatalf("recovered generation = %d, want 2", generation)
	}
	lease.Release()
	if err := reopened.Rollback(ctx, 99); !errors.Is(err, ErrRevisionMissing) {
		t.Fatalf("rollback missing revision err = %v, want ErrRevisionMissing", err)
	}
	if err := reopened.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAttemptTrailRecordsSwitchesAndIneffectiveAttempts(t *testing.T) {
	ctx := context.Background()
	manager, _, _, _, _ := newTestManager(t)
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}
	a, err := manager.Prepare(ctx, configFor(2, nil))
	if err != nil {
		t.Fatal(err)
	}
	b, err := manager.Prepare(ctx, configFor(3, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx); !errors.Is(err, ErrBaselineChanged) {
		t.Fatalf("err = %v, want conflict", err)
	}
	if err := b.Abandon(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Prepare(ctx, configFor(9, map[string]bool{"db": true})); err == nil {
		t.Fatal("failed prepare unexpectedly succeeded")
	}
	if err := manager.Rollback(ctx, 1); err != nil {
		t.Fatal(err)
	}

	attempts := manager.Attempts()
	var switched, conflicts, abandoned, failedPrepares, rollbacks int
	for _, record := range attempts {
		switch record.Kind {
		case "prepare":
			switch record.Result {
			case "prepared":
			case "failed":
				failedPrepares++
			}
		case "publish":
			if record.Switched {
				switched++
			}
			if record.Result == "conflict" {
				conflicts++
			}
		case "abandon":
			abandoned++
		case "rollback":
			rollbacks++
			if !record.Switched {
				t.Fatalf("rollback record not marked switched: %+v", record)
			}
		}
	}
	if switched != 2 || conflicts != 1 || abandoned != 1 || failedPrepares != 1 || rollbacks != 1 {
		t.Fatalf("counts switched=%d conflict=%d abandon=%d failedPrepare=%d rollback=%d; attempts=%+v",
			switched, conflicts, abandoned, failedPrepares, rollbacks, attempts)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedCandidateReleasedAndForgottenOnClose(t *testing.T) {
	ctx := context.Background()
	manager, closes, _, _, _ := newTestManager(t)
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Prepare(ctx, configFor(2, nil)); err != nil {
		t.Fatal(err)
	}
	if infos := manager.Candidates(); len(infos) != 1 {
		t.Fatalf("candidates = %+v, want 1", infos)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	var gen2Closes int
	for _, event := range closes.snapshot() {
		if event.generation == 2 {
			gen2Closes++
		}
	}
	if gen2Closes != 3 {
		t.Fatalf("prepared candidate close events = %d, want 3", gen2Closes)
	}
	if infos := manager.Candidates(); len(infos) != 0 {
		t.Fatalf("candidates leaked across close: %+v", infos)
	}
	if _, err := manager.Lease(); err == nil {
		t.Fatal("lease unexpectedly succeeded after close")
	}
}

func TestReopenDoesNotLeakCandidatesAndConflictRequiresRebase(t *testing.T) {
	ctx := context.Background()
	manager, _, db, service, api := newTestManager(t)
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Prepare(ctx, configFor(2, nil)); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, manager.configPath, api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	if infos := reopened.Candidates(); len(infos) != 0 {
		t.Fatalf("candidate survived reopen: %+v", infos)
	}
	a, err := reopened.Prepare(ctx, configFor(2, nil))
	if err != nil {
		t.Fatal(err)
	}
	b, err := reopened.Prepare(ctx, configFor(3, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx); !errors.Is(err, ErrBaselineChanged) {
		t.Fatalf("err = %v, want ErrBaselineChanged", err)
	}
	lease, err := reopened.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, lease); generation != 2 {
		t.Fatalf("generation = %d, want 2", generation)
	}
	lease.Release()
	d, err := reopened.Prepare(ctx, configFor(3, nil))
	if err != nil {
		t.Fatal(err)
	}
	if d.Baseline() != a.version.revision {
		t.Fatalf("baseline = %d, want %d", d.Baseline(), a.version.revision)
	}
	if err := d.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Abandon(ctx); err != nil {
		t.Fatal(err)
	}
	if infos := reopened.Candidates(); len(infos) != 0 {
		t.Fatalf("candidates left behind: %+v", infos)
	}
	if err := reopened.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFailedInitialStartReleasesResourcesAndCanBeRetried(t *testing.T) {
	ctx := context.Background()
	manager, closes, _, _, _ := newTestManager(t)
	if err := manager.Start(ctx, configFor(7, map[string]bool{"service": true})); err == nil {
		t.Fatal("failed start unexpectedly succeeded")
	}
	if _, ok := manager.ActiveConfig(); ok {
		t.Fatal("active config exists after failed initial start")
	}
	if infos := manager.Candidates(); len(infos) != 0 {
		t.Fatalf("failed start leaked candidate: %+v", infos)
	}
	var gen7 int
	for _, event := range closes.snapshot() {
		if event.generation == 7 {
			gen7++
		}
	}
	if gen7 != 1 {
		t.Fatalf("prepared gen-7 resources closed = %d, want 1 (db only)", gen7)
	}
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatalf("retry start failed: %v", err)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyConfigPathRejectsPrepareAndPublishesNothing(t *testing.T) {
	ctx := context.Background()
	manager, _, _, _, _ := newTestManager(t)
	manager.configPath = ""
	if err := manager.Start(ctx, configFor(1, nil)); err == nil {
		t.Fatal("start with empty path unexpectedly succeeded")
	}
	if _, err := manager.Prepare(ctx, configFor(1, nil)); err == nil {
		t.Fatal("prepare with empty path unexpectedly succeeded")
	}
	if records := manager.Attempts(); len(records) == 0 {
		t.Fatal("failed attempts were not recorded")
	}
}
