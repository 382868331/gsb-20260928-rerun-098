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

func persistedGeneration(t *testing.T, path string) (int, bool) {
	t.Helper()
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	var moduleConfig testConfig
	if err := json.Unmarshal(cfg["api"], &moduleConfig); err != nil {
		t.Fatal(err)
	}
	return moduleConfig.Generation, true
}

func newTestManager(t *testing.T) (*Manager, *closeLog) {
	t.Helper()
	closes := &closeLog{}
	db := &testModule{name: "db", closes: closes}
	service := &testModule{name: "service", deps: []string{"db"}, closes: closes}
	api := &testModule{name: "api", deps: []string{"service"}, closes: closes}
	manager, err := NewManager(filepath.Join(t.TempDir(), "config.json"), api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	return manager, closes
}

// Failed operations must never affect the running version or the on-disk
// record: only a configuration that was actually published can be restored.
func TestFailedStartAndReloadDoNotPersistOrMutateActive(t *testing.T) {
	ctx := context.Background()
	manager, closes := newTestManager(t)

	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}

	// A second start fails and must leave the gen-1 record on disk.
	if err := manager.Start(ctx, configFor(2, nil)); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start error = %v, want ErrAlreadyStarted", err)
	}
	if gen, ok := persistedGeneration(t, manager.configPath); !ok || gen != 1 {
		t.Fatalf("disk generation after duplicate start = %d (exists=%v), want 1", gen, ok)
	}
	for _, event := range closes.snapshot() {
		if event.generation == 1 {
			t.Fatalf("active resources closed by rejected duplicate start: %+v", event)
		}
	}

	// A reload whose preparation fails must persist nothing new.
	if err := manager.Reload(ctx, configFor(3, map[string]bool{"service": true})); err == nil {
		t.Fatal("forced preparation failure unexpectedly succeeded")
	}
	if gen, _ := persistedGeneration(t, manager.configPath); gen != 1 {
		t.Fatalf("disk generation after failed prepare = %d, want 1", gen)
	}
	lease, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, lease); generation != 1 {
		t.Fatalf("active generation after failed reload = %d, want 1", generation)
	}
	lease.Release()

	// Successful reload publishes gen 4 on disk and in memory atomically.
	if err := manager.Reload(ctx, configFor(4, nil)); err != nil {
		t.Fatal(err)
	}
	if gen, _ := persistedGeneration(t, manager.configPath); gen != 4 {
		t.Fatalf("disk generation after reload = %d, want 4", gen)
	}

	// Exiting immediately after the publish must restore exactly gen 4.
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	db := &testModule{name: "db", closes: closes}
	service := &testModule{name: "service", deps: []string{"db"}, closes: closes}
	api := &testModule{name: "api", deps: []string{"service"}, closes: closes}
	restarted, err := Open(ctx, manager.configPath, api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	restartLease, err := restarted.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, restartLease); generation != 4 {
		t.Fatalf("restored generation = %d, want 4", generation)
	}
	restartLease.Release()
	if err := restarted.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// Prepared candidates can be abandoned: their resources close in reverse
// order and nothing is published or persisted.
func TestCandidatePrepareAndAbort(t *testing.T) {
	ctx := context.Background()
	manager, closes := newTestManager(t)
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}

	candidate, err := manager.Prepare(ctx, configFor(2, nil))
	if err != nil {
		t.Fatal(err)
	}
	if gen, ok := persistedGeneration(t, manager.configPath); !ok || gen != 1 {
		t.Fatalf("disk generation after Prepare = %d (exists=%v), want 1", gen, ok)
	}
	if err := candidate.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	for _, event := range closes.snapshot() {
		if event.generation == 1 {
			t.Fatalf("active resources closed on candidate abort: %+v", event)
		}
	}
	want := []closeEvent{
		{module: "api", generation: 2},
		{module: "service", generation: 2},
		{module: "db", generation: 2},
	}
	var got []closeEvent
	for _, event := range closes.snapshot() {
		if event.generation == 2 {
			got = append(got, event)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("aborted candidate closes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("aborted candidate closes = %v, want %v", got, want)
		}
	}

	lease, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, lease); generation != 1 {
		t.Fatalf("active generation after abort = %d, want 1", generation)
	}
	lease.Release()
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// A prepared candidate can be committed explicitly, and Rollback switches to a
// prepared configuration without changing the persisted record. Retired
// resources outlive in-flight users until their leases are released.
func TestCandidateCommitRollbackAndInflightLease(t *testing.T) {
	ctx := context.Background()
	manager, closes := newTestManager(t)
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}

	candidate, err := manager.Prepare(ctx, configFor(2, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := candidate.Commit(ctx, true); err != nil {
		t.Fatal(err)
	}
	if gen, _ := persistedGeneration(t, manager.configPath); gen != 2 {
		t.Fatalf("disk generation after commit = %d, want 2", gen)
	}

	// Roll back to gen 1 (already durably published earlier): disk keeps gen 2
	// untouched while active resources become gen 1.
	held, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, held); generation != 2 {
		t.Fatalf("active generation = %d, want 2", generation)
	}
	if err := manager.Rollback(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}
	if gen, _ := persistedGeneration(t, manager.configPath); gen != 2 {
		t.Fatalf("disk generation changed by rollback = %d, want 2", gen)
	}
	for _, event := range closes.snapshot() {
		if event.generation == 2 {
			t.Fatalf("retired resources closed before in-flight lease release: %+v", event)
		}
	}
	current, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := leaseGeneration(t, current); generation != 1 {
		t.Fatalf("active generation after rollback = %d, want 1", generation)
	}
	current.Release()
	held.Release()

	deadline := time.Now().Add(time.Second)
	var retiredCloses []closeEvent
	for time.Now().Before(deadline) {
		retiredCloses = nil
		for _, event := range closes.snapshot() {
			if event.generation == 2 {
				retiredCloses = append(retiredCloses, event)
			}
		}
		if len(retiredCloses) == 3 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	want := []closeEvent{
		{module: "api", generation: 2},
		{module: "service", generation: 2},
		{module: "db", generation: 2},
	}
	if len(retiredCloses) != len(want) {
		t.Fatalf("retired closes = %v, want %v", retiredCloses, want)
	}
	for i := range want {
		if retiredCloses[i] != want[i] {
			t.Fatalf("retired closes = %v, want %v", retiredCloses, want)
		}
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// Starting from a failed preparation must leave no file behind, so restart
// cannot restore a configuration that was never published.
func TestFailedStartLeavesNoPersistedRecord(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManager(t)
	if err := manager.Start(ctx, configFor(9, map[string]bool{"db": true})); err == nil {
		t.Fatal("forced preparation failure unexpectedly succeeded")
	}
	if gen, ok := persistedGeneration(t, manager.configPath); ok {
		t.Fatalf("failed start persisted generation %d", gen)
	}
	if _, err := manager.Lease(); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Lease after failed start error = %v, want ErrNotStarted", err)
	}
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}
	if gen, ok := persistedGeneration(t, manager.configPath); !ok || gen != 1 {
		t.Fatalf("disk generation after successful start = %d (exists=%v), want 1", gen, ok)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
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
