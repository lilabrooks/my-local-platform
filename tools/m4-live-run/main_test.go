package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	testRunID   = "20260908T210000Z"
	testCommit  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testAccount = "123456789012"
)

var testStarted = time.Date(2026, 9, 8, 21, 0, 0, 0, time.UTC)

type fakeExecutor struct {
	calls        [][]string
	environments [][]string
	statuses     map[string][]int
}

func (f *fakeExecutor) Run(arguments []string, environment []string, output io.Writer) int {
	f.calls = append(f.calls, append([]string(nil), arguments...))
	f.environments = append(f.environments, append([]string(nil), environment...))
	target := ""
	if len(arguments) > 1 {
		target = arguments[1]
	}
	values := f.statuses[target]
	status := 0
	if len(values) > 0 {
		status = values[0]
		if len(values) > 1 {
			f.statuses[target] = values[1:]
		}
	}
	if output != nil {
		_, _ = io.WriteString(output, "fake "+target+"\n")
	}
	return status
}

func (f *fakeExecutor) Signal(os.Signal) {}

func (f *fakeExecutor) ClearPending() {}

type blockingExecutor struct {
	released chan struct{}
	once     sync.Once
	signal   os.Signal
}

func (b *blockingExecutor) Run([]string, []string, io.Writer) int {
	<-b.released
	return 130
}

func (b *blockingExecutor) Signal(value os.Signal) {
	b.signal = value
	b.once.Do(func() { close(b.released) })
}

func (b *blockingExecutor) ClearPending() {}

type stubbornExecutor struct {
	mu       sync.Mutex
	released chan struct{}
	signals  []os.Signal
	cleared  bool
}

func (s *stubbornExecutor) Run([]string, []string, io.Writer) int {
	<-s.released
	return 137
}

func (s *stubbornExecutor) Signal(value os.Signal) {
	s.mu.Lock()
	s.signals = append(s.signals, value)
	s.mu.Unlock()
}

func (s *stubbornExecutor) ClearPending() {
	s.mu.Lock()
	s.cleared = true
	s.mu.Unlock()
}

type fakeAWS struct {
	account         string
	accountFailures int
	accountCalls    int
	logsPassed      bool
}

func (f *fakeAWS) Account() (string, error) {
	f.accountCalls++
	if f.accountCalls <= f.accountFailures {
		return "", errors.New("transient identity failure")
	}
	return f.account, nil
}

func (f *fakeAWS) DeleteRuntimeLogs(output io.Writer) bool {
	_, _ = io.WriteString(output, "fake log cleanup\n")
	return f.logsPassed
}

type mutableClock struct {
	value time.Time
}

func (c *mutableClock) now() time.Time { return c.value }

func (c *mutableClock) sleep(value time.Duration) { c.value = c.value.Add(value) }

func testSession() sessionReceipt {
	return sessionReceipt{
		SchemaVersion:     1,
		RunID:             testRunID,
		Commit:            testCommit,
		Region:            "us-east-1",
		Operator:          "test-owner",
		BillableStartedAt: utcText(testStarted),
		DestroyDeadline:   utcText(testStarted.Add(destroyAfter)),
		HardDeadline:      utcText(testStarted.Add(hardAfter)),
		Limits: map[string]float64{
			"maximum_hourly_usd": 1.25,
			"maximum_total_usd":  5,
		},
	}
}

func writeTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

func prepareRun(t *testing.T, root string) string {
	t.Helper()
	raw := filepath.Join(root, ".evidence", "m4", testRunID)
	writeTestJSON(t, filepath.Join(raw, "00-preflight.json"), preflightReceipt{
		SchemaVersion: 1,
		RunID:         testRunID,
		Commit:        testCommit,
		Result:        "passed",
	})
	writeTestJSON(t, filepath.Join(raw, "capture-plan.json"), map[string]any{
		"schema_version": 1,
		"run_id":         testRunID,
		"commit":         testCommit,
		"paid_window": map[string]any{
			"evidence_deadline_minutes": 150,
			"hard_deadline_minutes":     180,
		},
	})
	writeTestJSON(t, filepath.Join(raw, "06-go-no-go.json"), map[string]any{
		"schema_version": 1,
		"run_id":         testRunID,
		"source_commit":  testCommit,
		"decision":       "go",
		"abort_command":  "make aws-down",
		"gate":           map[string]any{"passed": true},
	})
	return raw
}

func TestValidateSessionRequiresFixedDeadlines(t *testing.T) {
	session := testSession()
	started, destroy, hard, err := validateSession(session, testRunID, testCommit)
	if err != nil {
		t.Fatal(err)
	}
	if !started.Equal(testStarted) || !destroy.Equal(testStarted.Add(destroyAfter)) || !hard.Equal(testStarted.Add(hardAfter)) {
		t.Fatalf("unexpected deadlines: %s %s %s", started, destroy, hard)
	}

	session.DestroyDeadline = utcText(testStarted.Add(149 * time.Minute))
	if _, _, _, err := validateSession(session, testRunID, testCommit); err == nil || !strings.Contains(err.Error(), "fixed deadlines") {
		t.Fatalf("got %v, want fixed deadline error", err)
	}
}

func TestDestroyDeadlineStartsCleanupAtBoundary(t *testing.T) {
	deadline := testStarted.Add(destroyAfter)
	if destroyDeadlineReached(deadline.Add(-time.Nanosecond), deadline) {
		t.Fatal("cleanup started before the destroy deadline")
	}
	if !destroyDeadlineReached(deadline, deadline) {
		t.Fatal("cleanup did not start at the destroy deadline")
	}
}

func TestApplyIsInterruptedAtDestroyDeadline(t *testing.T) {
	deadline := testStarted.Add(destroyAfter)
	runner := &blockingExecutor{released: make(chan struct{})}
	current := &controller{
		runID:    testRunID,
		commit:   testCommit,
		profile:  "test-profile",
		region:   "us-east-1",
		executor: runner,
		now:      func() time.Time { return deadline },
	}

	status := current.runApply(deadline, make(chan os.Signal))

	if status != 130 || current.stopReason != "destroy_deadline" {
		t.Fatalf("status=%d reason=%q", status, current.stopReason)
	}
	if runner.signal != os.Interrupt {
		t.Fatalf("got signal %v, want interrupt", runner.signal)
	}
}

func TestApplyEscalatesAndReturnsWhenChildIgnoresSignals(t *testing.T) {
	deadline := testStarted.Add(destroyAfter)
	runner := &stubbornExecutor{released: make(chan struct{})}
	immediate := func(time.Duration) <-chan time.Time {
		result := make(chan time.Time, 1)
		result <- deadline
		return result
	}
	current := &controller{
		runID:    testRunID,
		commit:   testCommit,
		profile:  "test-profile",
		region:   "us-east-1",
		executor: runner,
		now:      func() time.Time { return deadline },
		after:    immediate,
	}

	status := current.runApply(deadline, make(chan os.Signal))
	close(runner.released)

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if status != 124 {
		t.Fatalf("status=%d, want 124", status)
	}
	want := []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL}
	if len(runner.signals) != len(want) {
		t.Fatalf("signals=%v, want %v", runner.signals, want)
	}
	for index := range want {
		if runner.signals[index] != want[index] {
			t.Fatalf("signals=%v, want %v", runner.signals, want)
		}
	}
	if !runner.cleared {
		t.Fatal("apply did not clear a pending signal before cleanup")
	}
}

func TestRepeatedOperatorSignalsEscalateApplyImmediately(t *testing.T) {
	runner := &stubbornExecutor{released: make(chan struct{})}
	signals := make(chan os.Signal, 3)
	signals <- syscall.SIGINT
	signals <- syscall.SIGINT
	signals <- syscall.SIGINT
	current := &controller{
		executor: runner,
		after: func(time.Duration) <-chan time.Time {
			return nil
		},
	}

	status := current.stopApply(make(chan int), signals, syscall.SIGINT)

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if status != 124 {
		t.Fatalf("status=%d, want 124", status)
	}
	want := []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL}
	if !slices.Equal(runner.signals, want) {
		t.Fatalf("signals=%v, want %v", runner.signals, want)
	}
}

func TestApplyPassesControllerPIDAsMakeArgumentAndEnvironment(t *testing.T) {
	runner := &fakeExecutor{statuses: map[string][]int{"aws-up": {0}}}
	current := &controller{
		runID:    testRunID,
		commit:   testCommit,
		profile:  "test-profile",
		region:   "us-east-1",
		executor: runner,
		now:      func() time.Time { return testStarted },
	}

	if status := current.runApply(testStarted.Add(destroyAfter), make(chan os.Signal)); status != 0 {
		t.Fatalf("apply returned %d", status)
	}
	pid := "MLP_AWS_LIVE_CONTROLLER_PID=" + strconv.Itoa(os.Getpid())
	if len(runner.calls) != 1 || !slices.Contains(runner.calls[0], pid) {
		t.Fatalf("apply arguments do not contain %q: %v", pid, runner.calls)
	}
	if len(runner.environments) != 1 || !slices.Contains(runner.environments[0], pid) {
		t.Fatalf("apply environment does not contain %q", pid)
	}
}

func TestRequirePreparedRunBindsEveryReceipt(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)

	got, err := requirePreparedRun(root, testRunID, testCommit)
	if err != nil {
		t.Fatal(err)
	}
	if got != raw {
		t.Fatalf("got %s, want %s", got, raw)
	}

	writeTestJSON(t, filepath.Join(raw, "00-session.json"), testSession())
	if _, err := requirePreparedRun(root, testRunID, testCommit); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("got %v, want existing session error", err)
	}
}

func TestRequestStopWritesOneBoundRequest(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "controller-state.json"), controllerState{
		SchemaVersion: 1,
		RunID:         testRunID,
		Commit:        testCommit,
		ControllerPID: os.Getpid(),
		Phase:         "live",
		UpdatedAt:     utcText(testStarted),
	})

	path, err := requestStop(root, testRunID, "evidence_complete", testStarted)
	if err != nil {
		t.Fatal(err)
	}
	var request stopRequest
	if err := readJSON(path, &request, "stop request"); err != nil {
		t.Fatal(err)
	}
	if request.RunID != testRunID || request.Reason != "evidence_complete" {
		t.Fatalf("unexpected stop request: %+v", request)
	}
	if _, err := requestStop(root, testRunID, "again", testStarted); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("got %v, want duplicate request error", err)
	}
}

func TestStopRejectsADeadController(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "controller-state.json"), controllerState{
		SchemaVersion: 1,
		RunID:         testRunID,
		Commit:        testCommit,
		ControllerPID: 99999999,
		Phase:         "live",
		UpdatedAt:     utcText(testStarted),
	})

	if _, err := requestStop(root, testRunID, "evidence_complete", testStarted); err == nil || !strings.Contains(err.Error(), "no fresh heartbeat") {
		t.Fatalf("got %v, want dead-controller error", err)
	}
}

func TestLiveStopPreservesReasonWithoutSignallingController(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "controller-state.json"), controllerState{
		SchemaVersion: 1,
		RunID:         testRunID,
		Commit:        testCommit,
		ControllerPID: os.Getpid(),
		Phase:         "live",
		UpdatedAt:     utcText(testStarted),
	})
	called := false
	previous := signalProcess
	signalProcess = func(int, syscall.Signal) error {
		called = true
		return nil
	}
	defer func() { signalProcess = previous }()

	if _, err := requestStop(root, testRunID, "evidence_complete", testStarted); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("live stop signalled the controller instead of letting it read the request")
	}
}

func TestWaitForStopHandlesDeadlineSignalAndStopFile(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		current := &controller{now: func() time.Time { return testStarted.Add(destroyAfter) }}
		if err := current.waitForStop(testStarted.Add(destroyAfter), make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		if current.stopReason != "destroy_deadline" {
			t.Fatalf("reason=%q", current.stopReason)
		}
	})

	t.Run("signal", func(t *testing.T) {
		signals := make(chan os.Signal, 1)
		signals <- syscall.SIGTERM
		current := &controller{now: func() time.Time { return testStarted }}
		if err := current.waitForStop(testStarted.Add(destroyAfter), signals); err != nil {
			t.Fatal(err)
		}
		if current.stopReason != syscall.SIGTERM.String() {
			t.Fatalf("reason=%q", current.stopReason)
		}
	})

	t.Run("stop file", func(t *testing.T) {
		root := t.TempDir()
		raw := filepath.Join(root, ".evidence", "m4", testRunID)
		writeTestJSON(t, filepath.Join(raw, "stop-request.json"), stopRequest{
			SchemaVersion: 1,
			RunID:         testRunID,
			Reason:        "evidence_complete",
			RequestedAt:   utcText(testStarted),
		})
		current := &controller{
			runID: testRunID,
			raw:   raw,
			now:   func() time.Time { return testStarted },
		}
		if err := current.waitForStop(testStarted.Add(destroyAfter), make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		if current.stopReason != "evidence_complete" {
			t.Fatalf("reason=%q", current.stopReason)
		}
	})
}

func TestCleanupRetriesDestroyAndRequiresEmptyInventory(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "01-identity.txt"), accountReceipt{
		AWS: struct {
			AccountID string `json:"account_id"`
		}{AccountID: testAccount},
	})
	clock := &mutableClock{value: testStarted.Add(destroyAfter)}
	runner := &fakeExecutor{statuses: map[string][]int{
		"aws-down":            {9, 0},
		"aws-inventory-empty": {5, 0},
		"aws-cost":            {0},
	}}
	current := &controller{
		root:       root,
		runID:      testRunID,
		commit:     testCommit,
		operator:   "test-owner",
		profile:    "test-profile",
		region:     "us-east-1",
		raw:        raw,
		executor:   runner,
		aws:        &fakeAWS{account: testAccount, logsPassed: true},
		now:        clock.now,
		sleep:      clock.sleep,
		stopReason: "destroy_deadline",
		state: controllerState{
			SchemaVersion: 1,
			RunID:         testRunID,
			Commit:        testCommit,
			ControllerPID: 1234,
		},
	}

	if status := current.cleanup(testSession(), 0); status != 0 {
		t.Fatalf("cleanup returned %d", status)
	}
	counts := map[string]int{}
	for _, call := range runner.calls {
		if len(call) > 1 {
			counts[call[1]]++
		}
	}
	if counts["aws-down"] != 2 || counts["aws-state-empty"] != 1 ||
		counts["aws-inventory-empty"] != 2 || counts["aws-cost"] != 1 {
		t.Fatalf("unexpected calls: %+v", counts)
	}
	var state controllerState
	if err := readJSON(filepath.Join(raw, "controller-state.json"), &state, "controller state"); err != nil {
		t.Fatal(err)
	}
	if state.Phase != "complete" || state.CleanupVerified == nil || !*state.CleanupVerified {
		t.Fatalf("unexpected final state: %+v", state)
	}
}

func TestCleanupFailureIsRecordedAndCostStillRuns(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "01-identity.txt"), accountReceipt{
		AWS: struct {
			AccountID string `json:"account_id"`
		}{AccountID: testAccount},
	})
	clock := &mutableClock{value: testStarted.Add(destroyAfter)}
	runner := &fakeExecutor{statuses: map[string][]int{
		"aws-down":            {0},
		"aws-inventory-empty": {4},
		"aws-cost":            {0},
	}}
	current := &controller{
		root:       root,
		runID:      testRunID,
		commit:     testCommit,
		operator:   "test-owner",
		profile:    "test-profile",
		region:     "us-east-1",
		raw:        raw,
		executor:   runner,
		aws:        &fakeAWS{account: testAccount, logsPassed: true},
		now:        clock.now,
		sleep:      clock.sleep,
		stopReason: "operator_stop",
		state: controllerState{
			SchemaVersion: 1,
			RunID:         testRunID,
			Commit:        testCommit,
			ControllerPID: 1234,
		},
	}

	if status := current.cleanup(testSession(), 0); status != 1 {
		t.Fatalf("cleanup returned %d", status)
	}
	var state controllerState
	if err := readJSON(filepath.Join(raw, "controller-state.json"), &state, "controller state"); err != nil {
		t.Fatal(err)
	}
	if state.Phase != "cleanup_failed" || state.CostExit == nil || *state.CostExit != 0 {
		t.Fatalf("unexpected failure state: %+v", state)
	}
}

func TestCleanupRetriesTransientIdentityFailuresBeforeDestroy(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "01-identity.txt"), accountReceipt{
		AWS: struct {
			AccountID string `json:"account_id"`
		}{AccountID: testAccount},
	})
	clock := &mutableClock{value: testStarted.Add(destroyAfter)}
	runner := &fakeExecutor{statuses: map[string][]int{
		"aws-down":            {0},
		"aws-state-empty":     {0},
		"aws-inventory-empty": {0},
		"aws-cost":            {0},
	}}
	aws := &fakeAWS{
		account:         testAccount,
		accountFailures: 2,
		logsPassed:      true,
	}
	current := &controller{
		root:     root,
		runID:    testRunID,
		commit:   testCommit,
		profile:  "test-profile",
		region:   "us-east-1",
		raw:      raw,
		executor: runner,
		aws:      aws,
		now:      clock.now,
		sleep:    clock.sleep,
		state: controllerState{
			SchemaVersion: 1,
			RunID:         testRunID,
			Commit:        testCommit,
			ControllerPID: 1234,
		},
	}

	if status := current.cleanup(testSession(), 0); status != 0 {
		t.Fatalf("cleanup returned %d", status)
	}
	if aws.accountCalls != 3 {
		t.Fatalf("identity calls=%d, want 3", aws.accountCalls)
	}
	if !clock.value.Equal(testStarted.Add(destroyAfter + 2*identityRetryDelay)) {
		t.Fatalf("clock=%s", clock.value)
	}
}

func TestCleanupPersistentIdentityFailureWritesRecoveryAndSkipsDestroy(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "01-identity.txt"), accountReceipt{
		AWS: struct {
			AccountID string `json:"account_id"`
		}{AccountID: testAccount},
	})
	clock := &mutableClock{value: testStarted.Add(destroyAfter)}
	runner := &fakeExecutor{statuses: map[string][]int{}}
	aws := &fakeAWS{
		account:         testAccount,
		accountFailures: identityAttempts,
		logsPassed:      true,
	}
	current := &controller{
		root:     root,
		runID:    testRunID,
		commit:   testCommit,
		profile:  "test-profile",
		region:   "us-east-1",
		raw:      raw,
		executor: runner,
		aws:      aws,
		now:      clock.now,
		sleep:    clock.sleep,
		state: controllerState{
			SchemaVersion: 1,
			RunID:         testRunID,
			Commit:        testCommit,
			ControllerPID: 1234,
		},
	}

	if status := current.cleanup(testSession(), 0); status != 1 {
		t.Fatalf("cleanup returned %d", status)
	}
	if aws.accountCalls != identityAttempts || len(runner.calls) != 0 {
		t.Fatalf("identity calls=%d executor calls=%v", aws.accountCalls, runner.calls)
	}
	transcript, err := os.ReadFile(filepath.Join(raw, "20-destroy.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(transcript), "make aws-down AWS_PROFILE_NAME=test-profile") {
		t.Fatalf("missing recovery instruction: %s", transcript)
	}
}

func TestCostFailureDoesNotClaimResourcesRemain(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "01-identity.txt"), accountReceipt{
		AWS: struct {
			AccountID string `json:"account_id"`
		}{AccountID: testAccount},
	})
	runner := &fakeExecutor{statuses: map[string][]int{
		"aws-down":            {0},
		"aws-state-empty":     {0},
		"aws-inventory-empty": {0},
		"aws-cost":            {7},
	}}
	current := &controller{
		root:     root,
		runID:    testRunID,
		commit:   testCommit,
		profile:  "test-profile",
		region:   "us-east-1",
		raw:      raw,
		executor: runner,
		aws:      &fakeAWS{account: testAccount, logsPassed: true},
		now:      func() time.Time { return testStarted.Add(destroyAfter) },
		state: controllerState{
			SchemaVersion: 1,
			RunID:         testRunID,
			Commit:        testCommit,
			ControllerPID: 1234,
		},
	}

	if status := current.cleanup(testSession(), 0); status != 1 {
		t.Fatalf("cleanup returned %d", status)
	}
	var state controllerState
	if err := readJSON(filepath.Join(raw, "controller-state.json"), &state, "controller state"); err != nil {
		t.Fatal(err)
	}
	if state.Phase != "complete" || state.Result != "cleanup_complete_with_errors" ||
		state.CleanupVerified == nil || !*state.CleanupVerified || state.CostExit == nil || *state.CostExit != 7 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestNonemptyTerraformStateFailsCleanupEvenWithEmptyInventory(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "01-identity.txt"), accountReceipt{
		AWS: struct {
			AccountID string `json:"account_id"`
		}{AccountID: testAccount},
	})
	runner := &fakeExecutor{statuses: map[string][]int{
		"aws-down":            {0},
		"aws-state-empty":     {4},
		"aws-inventory-empty": {0},
		"aws-cost":            {0},
	}}
	current := &controller{
		root:     root,
		runID:    testRunID,
		commit:   testCommit,
		profile:  "test-profile",
		region:   "us-east-1",
		raw:      raw,
		executor: runner,
		aws:      &fakeAWS{account: testAccount, logsPassed: true},
		now:      func() time.Time { return testStarted.Add(destroyAfter) },
		state: controllerState{
			SchemaVersion: 1,
			RunID:         testRunID,
			Commit:        testCommit,
			ControllerPID: 1234,
		},
	}

	if status := current.cleanup(testSession(), 0); status != 1 {
		t.Fatalf("cleanup returned %d", status)
	}
	var state controllerState
	if err := readJSON(filepath.Join(raw, "controller-state.json"), &state, "controller state"); err != nil {
		t.Fatal(err)
	}
	if state.Phase != "cleanup_failed" || state.CleanupVerified == nil ||
		*state.CleanupVerified || state.TerraformStateExit == nil || *state.TerraformStateExit != 4 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestTranscriptOpenFailureDoesNotBlockCleanup(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "01-identity.txt"), accountReceipt{
		AWS: struct {
			AccountID string `json:"account_id"`
		}{AccountID: testAccount},
	})
	if err := os.Mkdir(filepath.Join(raw, "20-destroy.txt"), 0o700); err != nil {
		t.Fatal(err)
	}
	runner := &fakeExecutor{statuses: map[string][]int{
		"aws-down":            {0},
		"aws-state-empty":     {0},
		"aws-inventory-empty": {0},
		"aws-cost":            {0},
	}}
	current := &controller{
		root:     root,
		runID:    testRunID,
		commit:   testCommit,
		profile:  "test-profile",
		region:   "us-east-1",
		raw:      raw,
		executor: runner,
		aws:      &fakeAWS{account: testAccount, logsPassed: true},
		now:      func() time.Time { return testStarted.Add(destroyAfter) },
		state: controllerState{
			SchemaVersion: 1,
			RunID:         testRunID,
			Commit:        testCommit,
			ControllerPID: 1234,
		},
	}

	if status := current.cleanup(testSession(), 0); status != 1 {
		t.Fatalf("cleanup returned %d", status)
	}
	if len(runner.calls) == 0 || runner.calls[0][1] != "aws-down" {
		t.Fatalf("cleanup did not run destroy: %v", runner.calls)
	}
	var state controllerState
	if err := readJSON(filepath.Join(raw, "controller-state.json"), &state, "controller state"); err != nil {
		t.Fatal(err)
	}
	if state.CleanupVerified == nil || !*state.CleanupVerified ||
		state.Result != "cleanup_complete_with_errors" {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestCleanupPastHardDeadlineIsVerifiedButFailsTheRun(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "01-identity.txt"), accountReceipt{
		AWS: struct {
			AccountID string `json:"account_id"`
		}{AccountID: testAccount},
	})
	clock := &mutableClock{value: testStarted.Add(hardAfter + time.Second)}
	runner := &fakeExecutor{statuses: map[string][]int{
		"aws-down":            {0},
		"aws-inventory-empty": {0},
		"aws-cost":            {0},
	}}
	current := &controller{
		root:       root,
		runID:      testRunID,
		commit:     testCommit,
		operator:   "test-owner",
		profile:    "test-profile",
		region:     "us-east-1",
		raw:        raw,
		executor:   runner,
		aws:        &fakeAWS{account: testAccount, logsPassed: true},
		now:        clock.now,
		sleep:      clock.sleep,
		stopReason: "destroy_deadline",
		state: controllerState{
			SchemaVersion: 1,
			RunID:         testRunID,
			Commit:        testCommit,
			ControllerPID: 1234,
		},
	}

	if status := current.cleanup(testSession(), 0); status != 1 {
		t.Fatalf("cleanup returned %d", status)
	}
	var state controllerState
	if err := readJSON(filepath.Join(raw, "controller-state.json"), &state, "controller state"); err != nil {
		t.Fatal(err)
	}
	if state.Phase != "complete" || state.Result != "cleanup_overdue" || !state.CleanupOverdue || state.CleanupVerified == nil || !*state.CleanupVerified {
		t.Fatalf("unexpected overdue state: %+v", state)
	}
}

func TestControllerLivenessRequiresFreshActiveState(t *testing.T) {
	state := controllerState{
		ControllerPID: os.Getpid(),
		Phase:         "live",
		UpdatedAt:     utcText(testStarted),
	}
	if !controllerStateRunning(state, testStarted.Add(heartbeatMaxAge)) {
		t.Fatal("fresh controller state was reported dead")
	}
	if controllerStateRunning(state, testStarted.Add(heartbeatMaxAge+time.Second)) {
		t.Fatal("stale controller state was reported live")
	}
	state.Phase = "complete"
	state.UpdatedAt = utcText(testStarted)
	if controllerStateRunning(state, testStarted) {
		t.Fatal("completed controller state was reported live")
	}
}

func TestCurrentStatusUsesTheControllerHeartbeat(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "00-session.json"), testSession())
	writeTestJSON(t, filepath.Join(raw, "controller-state.json"), controllerState{
		SchemaVersion: 1,
		RunID:         testRunID,
		Commit:        testCommit,
		ControllerPID: os.Getpid(),
		Phase:         "live",
		UpdatedAt:     utcText(testStarted),
	})

	status, err := currentStatus(root, testRunID, testStarted)
	if err != nil {
		t.Fatal(err)
	}
	if running, ok := status["controller_running"].(bool); !ok || !running {
		t.Fatalf("fresh status=%v", status)
	}
	status, err = currentStatus(root, testRunID, testStarted.Add(heartbeatMaxAge+time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if running, ok := status["controller_running"].(bool); !ok || running {
		t.Fatalf("stale status=%v", status)
	}
}

func TestControllerLockIsExclusive(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".controller.lock")
	first, err := acquireControllerLock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	if _, err := acquireControllerLock(path); err == nil {
		t.Fatal("second controller acquired the same lock")
	}
}

func TestStopRejectsStaleHeartbeatWithoutSignallingPID(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "controller-state.json"), controllerState{
		SchemaVersion: 1,
		RunID:         testRunID,
		Commit:        testCommit,
		ControllerPID: os.Getpid(),
		Phase:         "applying",
		UpdatedAt:     utcText(testStarted),
	})
	called := false
	previous := signalProcess
	signalProcess = func(int, syscall.Signal) error {
		called = true
		return nil
	}
	defer func() { signalProcess = previous }()

	_, err := requestStop(
		root,
		testRunID,
		"evidence_complete",
		testStarted.Add(heartbeatMaxAge+time.Second),
	)
	if err == nil || !strings.Contains(err.Error(), "no fresh heartbeat") {
		t.Fatalf("got %v, want stale-heartbeat error", err)
	}
	if called {
		t.Fatal("stale controller state caused a signal")
	}
}

func TestProcessExecutorClearsPendingSignalAndMapsExitStatus(t *testing.T) {
	runner := &processExecutor{root: t.TempDir()}
	runner.Signal(syscall.SIGTERM)
	runner.ClearPending()

	if status := runner.Run([]string{"/bin/sh", "-c", "exit 7"}, nil, io.Discard); status != 7 {
		t.Fatalf("status=%d, want 7", status)
	}
}

func TestProcessExecutorStartsAProcessGroupAndSignalsIt(t *testing.T) {
	root := t.TempDir()
	ready := filepath.Join(root, "ready")
	runner := &processExecutor{root: root}
	result := make(chan int, 1)
	go func() {
		result <- runner.Run(
			[]string{
				"/bin/sh",
				"-c",
				"trap 'exit 42' TERM; touch \"$1\"; while :; do sleep 1; done",
				"sh",
				ready,
			},
			nil,
			io.Discard,
		)
	}()
	t.Cleanup(func() { runner.Signal(syscall.SIGKILL) })

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	runner.mu.Lock()
	child := runner.child
	runner.mu.Unlock()
	if child == nil || child.Process == nil {
		t.Fatal("executor has no active child")
	}
	pgid, err := syscall.Getpgid(child.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if pgid != child.Process.Pid {
		t.Fatalf("pgid=%d pid=%d", pgid, child.Process.Pid)
	}
	runner.Signal(syscall.SIGTERM)
	select {
	case status := <-result:
		if status != 42 {
			t.Fatalf("status=%d, want 42", status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("signalled process group did not exit")
	}
}

func TestRepositoryRootIsResolvedFromTheModuleDirectory(t *testing.T) {
	root := t.TempDir()
	if err := exec.Command("git", "init", "-q", root).Run(); err != nil {
		t.Fatal(err)
	}
	module := filepath.Join(root, "tools", "m4-live-run")
	if err := os.MkdirAll(module, 0o700); err != nil {
		t.Fatal(err)
	}
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(module); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(previous) }()

	got, err := repositoryRoot()
	if err != nil {
		t.Fatal(err)
	}
	gotInfo, gotErr := os.Stat(got)
	wantInfo, wantErr := os.Stat(root)
	if gotErr != nil || wantErr != nil || !os.SameFile(gotInfo, wantInfo) {
		t.Fatalf("root=%s, want %s", got, root)
	}
}

func TestRestrictedAWSEnvironmentDropsEndpointOverridesAndBoundsRetries(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
	environment := restrictedAWSEnvironment("test-profile", "us-east-1")

	if slices.Contains(environment, "AWS_ENDPOINT_URL=http://127.0.0.1:4566") {
		t.Fatal("restricted environment retained AWS_ENDPOINT_URL")
	}
	if !slices.Contains(environment, "AWS_MAX_ATTEMPTS=1") ||
		!slices.Contains(environment, "AWS_RETRY_MODE=standard") {
		t.Fatalf("restricted environment has no retry bound: %v", environment)
	}
}

func TestAWSCLIAddsSocketTimeouts(t *testing.T) {
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	awsPath := filepath.Join(bin, "aws")
	if err := os.WriteFile(
		awsPath,
		[]byte("#!/bin/sh\nprintf '%s\\n' \"$*\"\n"),
		0o700,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	client := &awsCLI{
		root:        root,
		environment: []string{"PATH=" + bin},
	}

	output, err := client.output("sts", "get-caller-identity")
	if err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(string(output))
	want := "--cli-connect-timeout 10 --cli-read-timeout 30 sts get-caller-identity"
	if got != want {
		t.Fatalf("arguments=%q, want %q", got, want)
	}
}
