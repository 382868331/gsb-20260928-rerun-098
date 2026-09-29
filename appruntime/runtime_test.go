package appruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type moduleSettings struct {
	Value        int  "json:\"value\""
	FailValidate bool "json:\"fail_validate\""
	FailPrepare  bool "json:\"fail_prepare\""
}

type testModule struct {
	name      string
	dependsOn []string
}

func (m testModule) Name() string { return m.name }

func (m testModule) DependsOn() []string { return append([]string(nil), m.dependsOn...) }

func (m testModule) ValidateConfig(raw json.RawMessage) error {
	var settings moduleSettings
	if err := json.Unmarshal(raw, &settings); err != nil {
		return err
	}
	if settings.FailValidate {
		return errors.New("forced validation failure")
	}
	return nil
}

func (m testModule) PrepareResource(_ context.Context, raw json.RawMessage) (Resource, error) {
	var settings moduleSettings
	if err := json.Unmarshal(raw, &settings); err != nil {
		return nil, err
	}
	if settings.FailPrepare {
		return nil, errors.New("forced preparation failure")
	}
	return &testResource{module: m.name, value: settings.Value}, nil
}

type testResource struct {
	module string
	value  int
}

func (r *testResource) Close(context.Context) error { return nil }

type closeRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *closeRecorder) record(module string, value int) {
	r.mu.Lock()
	r.calls = append(r.calls, fmt.Sprintf("%s:%d", module, value))
	r.mu.Unlock()
}

func (r *closeRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

type recordedResource struct {
	module   string
	value    int
	recorder *closeRecorder
}

func (r *recordedResource) Close(context.Context) error {
	r.recorder.record(r.module, r.value)
	return nil
}

type recordedModule struct {
	testModule
	recorder *closeRecorder
}

func (m recordedModule) PrepareResource(ctx context.Context, raw json.RawMessage) (Resource, error) {
	var settings moduleSettings
	if err := json.Unmarshal(raw, &settings); err != nil {
		return nil, err
	}
	if settings.FailPrepare {
		return nil, errors.New("forced preparation failure")
	}
	return &recordedResource{module: m.name, value: settings.Value, recorder: m.recorder}, nil
}

func configFor(values map[string]int, fail map[string]string) Config {
	config := make(Config, len(values))
	for name, value := range values {
		settings := moduleSettings{Value: value}
		if fail[name] == "validate" {
			settings.FailValidate = true
		}
		if fail[name] == "prepare" {
			settings.FailPrepare = true
		}
		data, _ := json.Marshal(settings)
		config[name] = data
	}
	return config
}

func layeredModules(recorder *closeRecorder) []Module {
	base := recordedModule{testModule: testModule{name: "base"}, recorder: recorder}
	service := recordedModule{testModule: testModule{name: "service", dependsOn: []string{"base"}}, recorder: recorder}
	app := recordedModule{testModule: testModule{name: "app", dependsOn: []string{"service"}}, recorder: recorder}
	return []Module{app, base, service}
}

func resourceValue(t *testing.T, snapshot *Snapshot, name string) int {
	t.Helper()
	resource, ok := snapshot.Resource(name)
	if !ok {
		t.Fatalf("missing resource %s", name)
	}
	recorded, ok := resource.(*recordedResource)
	if !ok {
		t.Fatalf("resource %s has unexpected type %T", name, resource)
	}
	return recorded.value
}

func TestTwoLayerDependencyOrderAndShutdown(t *testing.T) {
	recorder := &closeRecorder{}
	framework, err := New(layeredModules(recorder))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := framework.Reload(ctx, configFor(map[string]int{"base": 1, "service": 1, "app": 1}, nil), ""); err != nil {
		t.Fatal(err)
	}
	if err := framework.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{"app:1", "service:1", "base:1"}
	if got := recorder.snapshot(); !equalStrings(got, want) {
		t.Fatalf("close order = %v, want %v", got, want)
	}
}

func TestSnapshotDoneIsIdempotent(t *testing.T) {
	recorder := &closeRecorder{}
	framework, err := New(layeredModules(recorder))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := framework.Reload(ctx, configFor(map[string]int{"base": 1, "service": 1, "app": 1}, nil), ""); err != nil {
		t.Fatal(err)
	}
	snapshot := framework.Current()
	snapshot.Done()
	snapshot.Done()
	if err := framework.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFailedPreparationKeepsCompleteOldVersion(t *testing.T) {
	recorder := &closeRecorder{}
	framework, err := New(layeredModules(recorder))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := framework.Reload(ctx, configFor(map[string]int{"base": 1, "service": 1, "app": 1}, nil), ""); err != nil {
		t.Fatal(err)
	}

	oldSnapshot := framework.Current()

	bad := configFor(map[string]int{"base": 2, "service": 2, "app": 2}, map[string]string{"service": "prepare"})
	if err := framework.Reload(ctx, bad, ""); err == nil {
		t.Fatal("expected preparation failure")
	}

	current := framework.Current()
	if current.Version() != 1 {
		t.Fatalf("active version = %d, want 1", current.Version())
	}
	for _, name := range []string{"base", "service", "app"} {
		if value := resourceValue(t, current, name); value != 1 {
			t.Fatalf("%s = %d, want old version 1", name, value)
		}
	}
	if got := recorder.snapshot(); !equalStrings(got, []string{"base:2"}) {
		t.Fatalf("new resources closed = %v, want only [base:2]", got)
	}

	oldSnapshot.Done()
	current.Done()
	if err := framework.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := recorder.snapshot(); !equalStrings(got, []string{"base:2", "app:1", "service:1", "base:1"}) {
		t.Fatalf("close sequence = %v", got)
	}
}

func TestValidationFailurePreparesNoResource(t *testing.T) {
	recorder := &closeRecorder{}
	framework, err := New(layeredModules(recorder))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := framework.Reload(ctx, configFor(map[string]int{"base": 1, "service": 1, "app": 1}, nil), ""); err != nil {
		t.Fatal(err)
	}
	bad := configFor(map[string]int{"base": 2, "service": 2, "app": 2}, map[string]string{"app": "validate"})
	if err := framework.Reload(ctx, bad, ""); err == nil {
		t.Fatal("expected validation failure")
	}
	if got := recorder.snapshot(); len(got) != 0 {
		t.Fatalf("resources closed during failed validation: %v", got)
	}
	if err := framework.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestInFlightWorkKeepsOldResourcesUntilDone(t *testing.T) {
	recorder := &closeRecorder{}
	framework, err := New(layeredModules(recorder))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := framework.Reload(ctx, configFor(map[string]int{"base": 1, "service": 1, "app": 1}, nil), ""); err != nil {
		t.Fatal(err)
	}
	inFlight := framework.Current()

	if err := framework.Reload(ctx, configFor(map[string]int{"base": 2, "service": 2, "app": 2}, nil), ""); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if got := recorder.snapshot(); len(got) != 0 {
		t.Fatalf("old resources released before in-flight work finished: %v", got)
	}

	current := framework.Current()
	if current.Version() != 2 {
		t.Fatalf("active version = %d, want 2", current.Version())
	}
	inFlight.Done()
	current.Done()

	waitFor(t, time.Second, func() bool {
		return equalStrings(recorder.snapshot(), []string{"app:1", "service:1", "base:1"})
	})
	if err := framework.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentReadersAlwaysSeeCompleteVersion(t *testing.T) {
	recorder := &closeRecorder{}
	framework, err := New(layeredModules(recorder))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := framework.Reload(ctx, configFor(map[string]int{"base": 1, "service": 1, "app": 1}, nil), ""); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					snapshot := framework.Current()
					base := resourceValue(t, snapshot, "base")
					service := resourceValue(t, snapshot, "service")
					app := resourceValue(t, snapshot, "app")
					if base != service || service != app {
						t.Errorf("mixed versions: base=%d service=%d app=%d", base, service, app)
					}
					snapshot.Done()
				}
			}
		}()
	}

	for version := 2; version <= 5; version++ {
		values := map[string]int{"base": version, "service": version, "app": version}
		if err := framework.Reload(ctx, configFor(values, nil), ""); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	readers.Wait()
	if err := framework.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRestartLoadsLastSuccessfulConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime-config.json")
	ctx := context.Background()

	first, err := New(layeredModules(&closeRecorder{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Reload(ctx, configFor(map[string]int{"base": 7, "service": 7, "app": 7}, nil), path); err != nil {
		t.Fatal(err)
	}
	bad := configFor(map[string]int{"base": 8, "service": 8, "app": 8}, map[string]string{"service": "prepare"})
	if err := first.Reload(ctx, bad, path); err == nil {
		t.Fatal("expected failed reload")
	}
	if err := first.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(layeredModules(&closeRecorder{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Load(ctx, path); err != nil {
		t.Fatal(err)
	}
	snapshot := restarted.Current()
	for _, name := range []string{"base", "service", "app"} {
		if value := resourceValue(t, snapshot, name); value != 7 {
			t.Fatalf("%s loaded value = %d, want 7", name, value)
		}
	}
	snapshot.Done()
	if err := restarted.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDependencyValidation(t *testing.T) {
	missing := testModule{name: "app", dependsOn: []string{"missing"}}
	framework, err := New([]Module{missing})
	if err != nil {
		t.Fatal(err)
	}
	err = framework.Reload(context.Background(), Config{"app": json.RawMessage("{}\n")}, "")
	if err == nil {
		t.Fatal("expected missing dependency error")
	}

	a := testModule{name: "a", dependsOn: []string{"b"}}
	b := testModule{name: "b", dependsOn: []string{"a"}}
	cyclic, err := New([]Module{a, b})
	if err != nil {
		t.Fatal(err)
	}
	err = cyclic.Reload(context.Background(), Config{
		"a": json.RawMessage("{}\n"),
		"b": json.RawMessage("{}\n"),
	}, "")
	if err == nil {
		t.Fatal("expected dependency cycle error")
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
