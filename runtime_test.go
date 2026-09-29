package runtime

import (
	"context"
	"encoding/json"
	"errors"
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
