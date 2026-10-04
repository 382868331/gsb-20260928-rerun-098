package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func batchConfigFor(generation int) json.RawMessage {
	data, _ := json.Marshal(testConfig{Generation: generation})
	return data
}

func rawFailPrepare(generation int) json.RawMessage {
	data, _ := json.Marshal(testConfig{Generation: generation, FailPrepare: true})
	return data
}

func newBatchTestManager(t *testing.T) (*Manager, *closeLog) {
	t.Helper()
	closes := &closeLog{}
	db := &testModule{name: "db", closes: closes}
	service := &testModule{name: "service", deps: []string{"db"}, closes: closes}
	api := &testModule{name: "api", deps: []string{"service"}, closes: closes}
	manager, err := NewManager(testConfigPath(t), api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), configFor(1, nil)); err != nil {
		t.Fatal(err)
	}
	return manager, closes
}

func testConfigPath(t *testing.T) string {
	t.Helper()
	return t.TempDir() + "/config.json"
}

func stateByName(states []ModuleState) map[string]ModuleState {
	out := make(map[string]ModuleState, len(states))
	for _, state := range states {
		out[state.Name] = state
	}
	return out
}

func countCloses(events []closeEvent, generation int) int {
	var count int
	for _, event := range events {
		if event.generation == generation {
			count++
		}
	}
	return count
}

// apiGeneration is the batch generation carried by service+api; db may stay
// on an older unchanged generation in joint releases.
func apiGeneration(t *testing.T, lease *Lease) int {
	t.Helper()
	service := lease.Resource("service").(*testResource).generation
	api := lease.Resource("api").(*testResource).generation
	if service != api {
		t.Fatalf("mixed service/api versions: %d vs %d", service, api)
	}
	return api
}

func TestBatchConfirmRequiresAllRequiredModulesReady(t *testing.T) {
	ctx := context.Background()
	manager, closes := newBatchTestManager(t)

	batch, err := manager.PrepareBatch(ctx, BatchSpec{
		Modules: []BatchModule{
			{Name: "service", Raw: batchConfigFor(2), Required: true},
			{Name: "api", Raw: rawFailPrepare(9), Required: true},
		},
	})
	if err == nil {
		t.Fatal("batch preparation with failing required module unexpectedly succeeded")
	}
	if batch.Status() != batchFailed {
		t.Fatalf("status = %q, want failed", batch.Status())
	}
	states := stateByName(batch.Modules())
	if states["service"].Status != "ready" || !states["service"].Required {
		t.Fatalf("service state = %+v", states["service"])
	}
	if states["api"].Status != "failed" || states["api"].Reason == "" {
		t.Fatalf("api state = %+v", states["api"])
	}
	if states["db"].Status != "unchanged" || !states["db"].Inherited {
		t.Fatalf("db state = %+v", states["db"])
	}
	// Newly prepared service resource was torn down; baseline generation 1
	// resources were never closed.
	if count := countCloses(closes.snapshot(), 2); count != 1 {
		t.Fatalf("generation 2 closes = %d, want 1", count)
	}
	if count := countCloses(closes.snapshot(), 1); count != 0 {
		t.Fatalf("baseline closes during failed batch = %d, want 0", count)
	}
	records := batch.Resources()
	kinds := map[string]int{}
	for _, record := range records {
		kinds[record.Kind]++
	}
	if kinds[ResourceNew] != 1 || kinds[ResourceKept] != 2 {
		t.Fatalf("resource records = %+v", records)
	}

	// Failed batch can never be confirmed.
	if err := batch.Confirm(ctx); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("confirm failed batch err = %v, want ErrBatchClosed", err)
	}

	// A healthy batch for the same modules confirms only when ready.
	good, err := manager.PrepareBatch(ctx, BatchSpec{
		Modules: []BatchModule{
			{Name: "service", Raw: batchConfigFor(3), Required: true},
			{Name: "api", Raw: batchConfigFor(3), Required: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !good.Ready() {
		t.Fatal("healthy batch not ready")
	}
	if err := good.Confirm(ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := apiGeneration(t, lease); generation != 3 {
		t.Fatalf("active generation = %d, want 3", generation)
	}
	dbResource, ok := lease.Resource("db").(*testResource)
	if !ok || dbResource.generation != 1 {
		t.Fatalf("db resource = %#v, want unchanged generation 1", lease.Resource("db"))
	}
	lease.Release()
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBatchCompatibilityConditionGatesConfirm(t *testing.T) {
	ctx := context.Background()
	manager, _ := newBatchTestManager(t)

	compat := func(view map[string]ModuleConfig) error {
		var serviceCfg, apiCfg testConfig
		if err := view["service"].Decode(&serviceCfg); err != nil {
			return err
		}
		if err := view["api"].Decode(&apiCfg); err != nil {
			return err
		}
		if serviceCfg.Generation != apiCfg.Generation {
			return fmt.Errorf("service gen %d incompatible with api gen %d", serviceCfg.Generation, apiCfg.Generation)
		}
		return nil
	}

	incompatible, err := manager.PrepareBatch(ctx, BatchSpec{
		Modules: []BatchModule{
			{Name: "service", Raw: batchConfigFor(2), Required: true},
			{Name: "api", Raw: batchConfigFor(5), Required: true},
		},
		Compatible: compat,
	})
	if err == nil {
		t.Fatal("incompatible batch unexpectedly prepared")
	}
	if incompatible.Status() != batchIncompatible || incompatible.Reason() == "" {
		t.Fatalf("status = %q reason = %q", incompatible.Status(), incompatible.Reason())
	}

	good, err := manager.PrepareBatch(ctx, BatchSpec{
		Modules: []BatchModule{
			{Name: "service", Raw: batchConfigFor(4), Required: true},
			{Name: "api", Raw: batchConfigFor(4), Required: true},
		},
		Compatible: compat,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := good.Confirm(ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := apiGeneration(t, lease); generation != 4 {
		t.Fatalf("generation = %d, want 4", generation)
	}
	lease.Release()
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTwoBatchesInterleaveAndBaselineMovesBeforeConfirm(t *testing.T) {
	ctx := context.Background()
	manager, closes := newBatchTestManager(t)

	batchA, err := manager.PrepareBatch(ctx, BatchSpec{Modules: []BatchModule{
		{Name: "api", Raw: batchConfigFor(2), Required: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	batchB, err := manager.PrepareBatch(ctx, BatchSpec{Modules: []BatchModule{
		{Name: "api", Raw: batchConfigFor(3), Required: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(manager.Batches()) != 2 {
		t.Fatalf("open batches = %d, want 2", len(manager.Batches()))
	}
	if err := batchA.Confirm(ctx); err != nil {
		t.Fatal(err)
	}
	err = batchB.Confirm(ctx)
	if !errors.Is(err, ErrBaselineChanged) {
		t.Fatalf("confirm after baseline moved err = %v, want ErrBaselineChanged", err)
	}
	if batchB.Status() != batchPrepared {
		t.Fatalf("batch B status = %q, want prepared for inspection", batchB.Status())
	}
	// Abandoning stale B must not close resources serving revision 2.
	before := len(closes.snapshot())
	if err := batchB.Abandon(ctx); err != nil {
		t.Fatal(err)
	}
	for _, event := range closes.snapshot()[before:] {
		if event.generation == 2 {
			t.Fatalf("active revision 2 resource closed by stale batch abandon: %+v", event)
		}
	}
	if count := countCloses(closes.snapshot(), 3); count != 1 {
		t.Fatalf("gen 3 closes = %d, want 1 (only api was new)", count)
	}
	// Abandon is idempotent and recorded once.
	if err := batchB.Abandon(ctx); err != nil {
		t.Fatalf("idempotent abandon: %v", err)
	}
	if count := countCloses(closes.snapshot(), 3); count != 1 {
		t.Fatalf("gen 3 closes after re-abandon = %d, want 1", count)
	}
	records := manager.BatchRecords()
	byID := map[string]BatchInfo{}
	for _, info := range records {
		byID[info.ID] = info
	}
	if byID[batchA.ID()].Status != batchConfirmed || byID[batchB.ID()].Status != batchAbandoned {
		t.Fatalf("batch records = %+v", records)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestJointRollbackPublishesCompatibleSetAndKeepsReadsPinned(t *testing.T) {
	ctx := context.Background()
	manager, closes := newBatchTestManager(t)

	batch2, err := manager.PrepareBatch(ctx, BatchSpec{Modules: []BatchModule{
		{Name: "service", Raw: batchConfigFor(2), Required: true},
		{Name: "api", Raw: batchConfigFor(2), Required: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := batch2.Confirm(ctx); err != nil {
		t.Fatal(err)
	}
	lease2, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := apiGeneration(t, lease2); generation != 2 {
		t.Fatalf("generation = %d, want 2", generation)
	}

	batch3, err := manager.PrepareBatch(ctx, BatchSpec{Modules: []BatchModule{
		{Name: "service", Raw: batchConfigFor(3), Required: true},
		{Name: "api", Raw: batchConfigFor(3), Required: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := batch3.Confirm(ctx); err != nil {
		t.Fatal(err)
	}
	if generation := apiGeneration(t, lease2); generation != 2 {
		t.Fatalf("in-flight lease changed to %d", generation)
	}
	if lease2.Revision() != 2 {
		t.Fatalf("lease revision = %d, want 2", lease2.Revision())
	}

	if err := manager.Rollback(ctx, 2); err != nil {
		t.Fatal(err)
	}
	leaseBack, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := apiGeneration(t, leaseBack); generation != 2 {
		t.Fatalf("generation after rollback = %d, want 2", generation)
	}
	if revision := leaseBack.Revision(); revision != 4 {
		t.Fatalf("revision after rollback = %d, want 4", revision)
	}
	leaseBack.Release()

	// Generation 2 resources of the in-flight lease must stay open until it
	// releases, then close in reverse order.
	for _, event := range closes.snapshot() {
		if event.generation == 2 {
			t.Fatalf("revision 2 resource closed while lease active: %+v", event)
		}
	}
	lease2.Release()
	deadline := time.Now().Add(2 * time.Second)
	var oldEvents []closeEvent
	for time.Now().Before(deadline) {
		oldEvents = nil
		for _, event := range closes.snapshot() {
			if event.generation == 2 {
				oldEvents = append(oldEvents, event)
			}
		}
		if len(oldEvents) >= 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	want := []closeEvent{{module: "service", generation: 2}, {module: "api", generation: 2}}
	if len(oldEvents) != 2 || oldEvents[0] != want[0] || oldEvents[1] != want[1] {
		t.Fatalf("revision 2 closes = %v, want %v", oldEvents, want)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentReadsNeverSeeMixedBatchModules(t *testing.T) {
	ctx := context.Background()
	manager, _ := newBatchTestManager(t)

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
					db := lease.Resource("db").(*testResource).generation
					service := lease.Resource("service").(*testResource).generation
					api := lease.Resource("api").(*testResource).generation
					if db != 1 || service != api {
						mixed.Store(true)
					}
					lease.Release()
				}
			}
		}()
	}

	for gen := 2; gen <= 4; gen++ {
		batch, err := manager.PrepareBatch(ctx, BatchSpec{Modules: []BatchModule{
			{Name: "service", Raw: batchConfigFor(gen), Required: true},
			{Name: "api", Raw: batchConfigFor(gen), Required: true},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Confirm(ctx); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	readers.Wait()
	if mixed.Load() {
		t.Fatal("reader observed a mix of old and new batch modules")
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

type failingCloseResource struct {
	*testResource
	failOnce *atomic.Bool
}

func (r *failingCloseResource) Close(context.Context) error {
	if r.failOnce.CompareAndSwap(true, false) {
		return errors.New("forced cleanup failure")
	}
	return r.testResource.Close(context.Background())
}

type failingCloseModule struct {
	testModule
	failOnce *atomic.Bool
}

func (m *failingCloseModule) Prepare(ctx context.Context, config ModuleConfig) (Resource, error) {
	resource, err := m.testModule.Prepare(ctx, config)
	if err != nil {
		return nil, err
	}
	return &failingCloseResource{testResource: resource.(*testResource), failOnce: m.failOnce}, nil
}

func TestBatchAbandonRecordsCleanupFailure(t *testing.T) {
	ctx := context.Background()
	closes := &closeLog{}
	db := &testModule{name: "db", closes: closes}
	service := &failingCloseModule{
		testModule: testModule{name: "service", deps: []string{"db"}, closes: closes},
		failOnce:   &atomic.Bool{},
	}
	service.failOnce.Store(true)
	api := &testModule{name: "api", deps: []string{"service"}, closes: closes}
	manager, err := NewManager(t.TempDir()+"/config.json", api, service, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(ctx, configFor(1, nil)); err != nil {
		t.Fatal(err)
	}
	batch, err := manager.PrepareBatch(ctx, BatchSpec{Modules: []BatchModule{
		{Name: "service", Raw: batchConfigFor(2), Required: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Abandon(ctx); err == nil {
		t.Fatal("abandon with failing Close unexpectedly succeeded")
	}
	var cleanupFailures, kept int
	for _, record := range batch.Resources() {
		switch record.Kind {
		case ResourceCleanupFailed:
			cleanupFailures++
			if record.Module != "service" || record.Err == "" || !record.New {
				t.Fatalf("bad cleanup record = %+v", record)
			}
		case ResourceKept:
			kept++
		}
	}
	if cleanupFailures != 1 {
		t.Fatalf("cleanup failures = %d, want 1: %+v", cleanupFailures, batch.Resources())
	}
	if kept != 2 {
		t.Fatalf("kept records = %d, want 2", kept)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestManagerCloseAbandonsPreparedBatchesAndPreservesRecords(t *testing.T) {
	ctx := context.Background()
	manager, closes := newBatchTestManager(t)
	batch, err := manager.PrepareBatch(ctx, BatchSpec{Modules: []BatchModule{
		{Name: "api", Raw: batchConfigFor(2), Required: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if batch.Status() != batchAbandoned {
		t.Fatalf("batch status = %q, want abandoned", batch.Status())
	}
	if count := countCloses(closes.snapshot(), 2); count != 1 {
		t.Fatalf("gen 2 closes after manager close = %d, want 1", count)
	}
	found := false
	for _, info := range manager.BatchRecords() {
		if info.ID == batch.ID() && info.Status == batchAbandoned {
			found = true
		}
	}
	if !found {
		t.Fatalf("abandoned batch missing from records: %+v", manager.BatchRecords())
	}
}

func TestOptionalModuleFailureSkipsModuleButKeepsBatchReady(t *testing.T) {
	ctx := context.Background()
	manager, closes := newBatchTestManager(t)
	batch, err := manager.PrepareBatch(ctx, BatchSpec{Modules: []BatchModule{
		{Name: "api", Raw: rawFailPrepare(2), Required: false},
	}})
	if err != nil {
		t.Fatalf("optional failure returned error: %v", err)
	}
	if !batch.Ready() || batch.Status() != batchPrepared {
		t.Fatalf("status = %q ready = %v", batch.Status(), batch.Ready())
	}
	states := stateByName(batch.Modules())
	if states["api"].Status != "skipped" || states["api"].Required {
		t.Fatalf("api state = %+v", states["api"])
	}
	if err := batch.Confirm(ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := manager.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if generation := apiGeneration(t, lease); generation != 1 {
		t.Fatalf("api generation = %d, want baseline 1 (skipped optional)", generation)
	}
	lease.Release()
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if count := countCloses(closes.snapshot(), 2); count != 0 {
		t.Fatalf("gen 2 closes = %d, want 0 (nothing prepared)", count)
	}
}
