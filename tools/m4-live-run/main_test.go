package main

import (
	"bytes"
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
	failures     map[string]error
}

func (f *fakeExecutor) Run(arguments []string, environment []string, output io.Writer) (int, error) {
	f.calls = append(f.calls, append([]string(nil), arguments...))
	f.environments = append(f.environments, append([]string(nil), environment...))
	target := ""
	if len(arguments) > 1 {
		target = arguments[1]
	}
	if len(arguments) > 2 && arguments[0] == "python3" {
		target = arguments[2]
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
	return status, f.failures[target]
}

func (f *fakeExecutor) Signal(os.Signal) {}

func (f *fakeExecutor) ClearPending() {}

type blockingExecutor struct {
	released chan struct{}
	once     sync.Once
	signal   os.Signal
}

func (b *blockingExecutor) Run([]string, []string, io.Writer) (int, error) {
	<-b.released
	return 130, nil
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

func (s *stubbornExecutor) Run([]string, []string, io.Writer) (int, error) {
	<-s.released
	return 137, nil
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

// groupedExecutor is a stubborn apply whose process group is known.
type groupedExecutor struct {
	stubbornExecutor
	group int
}

func (g *groupedExecutor) ActiveGroup() int { return g.group }

// killDeliveryExecutor reports apply's exit as SIGKILL is sent, so the exit
// result and the final deadline are both ready when stopApply next selects.
type killDeliveryExecutor struct {
	result  chan<- executionResult
	signals []os.Signal
}

func (k *killDeliveryExecutor) Run([]string, []string, io.Writer) (int, error) { return 0, nil }

func (k *killDeliveryExecutor) Signal(value os.Signal) {
	k.signals = append(k.signals, value)
	if value == syscall.SIGKILL {
		k.result <- executionResult{status: 137}
	}
}

func (k *killDeliveryExecutor) ClearPending() {}

// firedTimer stands in for a grace period that has already expired.
func firedTimer(time.Duration) <-chan time.Time {
	fired := make(chan time.Time, 1)
	fired <- time.Now()
	return fired
}

type fakeAWS struct {
	account         string
	accountFailures int
	accountCalls    int
	logsPassed      bool
	logCalls        int
}

func (f *fakeAWS) Account() (string, error) {
	f.accountCalls++
	if f.accountCalls <= f.accountFailures {
		return "", errors.New("transient identity failure")
	}
	return f.account, nil
}

func (f *fakeAWS) DeleteRuntimeLogs(output io.Writer) bool {
	f.logCalls++
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

func prepareCommittedRun(t *testing.T) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte(".evidence/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", ".gitignore"}, {"-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git fixture: %v %s", err, out)
		}
	}
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = root
	head, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	commit := strings.TrimSpace(string(head))
	raw := prepareRun(t, root)
	for _, name := range []string{"00-preflight.json", "capture-plan.json", "06-go-no-go.json"} {
		path := filepath.Join(raw, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(strings.ReplaceAll(string(data), testCommit, commit)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root, raw, commit
}

func TestPreSessionRefusalDoesNotSpendRunOrDestroy(t *testing.T) {
	root, raw, commit := prepareCommittedRun(t)
	executor := &fakeExecutor{statuses: map[string][]int{"verify-go-no-go": {2}}}
	c := &controller{root: root, runID: testRunID, commit: commit, profile: "fixture", region: "us-east-1", executor: executor}
	if status := c.run(make(chan os.Signal)); status != 2 {
		t.Fatalf("status = %d", status)
	}
	if len(executor.calls) != 1 || !slices.Contains(executor.calls[0], "--before-session") {
		t.Fatalf("unexpected commands: %v", executor.calls)
	}
	for _, name := range []string{"00-session.json", "controller-state.json", ".controller.lock"} {
		if _, err := os.Stat(filepath.Join(raw, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("refusal left %s: %v", name, err)
		}
	}
}

type sessionExecutor struct {
	fakeExecutor
	raw    string
	commit string
}

func (s *sessionExecutor) Run(arguments []string, environment []string, output io.Writer) (int, error) {
	status, err := s.fakeExecutor.Run(arguments, environment, output)
	if err == nil && status == 0 && len(arguments) > 2 && arguments[2] == "start-session" {
		session := testSession()
		session.Commit = s.commit
		if writeErr := writeJSONAtomic(filepath.Join(s.raw, "00-session.json"), session); writeErr != nil {
			return 1, nil
		}
	}
	return status, err
}

func TestControllerRequiresConfirmedApplyExitBeforeCleanup(t *testing.T) {
	for _, unconfirmed := range []bool{false, true} {
		t.Run(strconv.FormatBool(unconfirmed), func(t *testing.T) {
			root, raw, commit := prepareCommittedRun(t)
			writeTestJSON(t, filepath.Join(raw, "01-identity.txt"), map[string]any{
				"aws": map[string]string{"account_id": testAccount},
			})
			runner := &sessionExecutor{
				fakeExecutor: fakeExecutor{statuses: map[string][]int{"aws-up": {137}}},
				raw:          raw, commit: commit,
			}
			if unconfirmed {
				runner.failures = map[string]error{"aws-up": errProcessExitUnconfirmed}
			}
			aws := &fakeAWS{account: testAccount, logsPassed: true}
			current := &controller{
				root: root, raw: raw, runID: testRunID, commit: commit,
				operator: "fixture", profile: "fixture", region: "us-east-1",
				executor: runner, aws: aws, now: func() time.Time { return testStarted },
			}

			if status := current.run(make(chan os.Signal)); status != 1 {
				t.Fatalf("status=%d, want failed run", status)
			}
			var state controllerState
			if err := readJSON(filepath.Join(raw, "controller-state.json"), &state, "state"); err != nil {
				t.Fatal(err)
			}
			if unconfirmed {
				if len(runner.calls) != 3 || aws.accountCalls != 0 || aws.logCalls != 0 {
					t.Fatalf("commands ran after unconfirmed apply: %v; AWS=%+v", runner.calls, aws)
				}
				if state.Phase != "cleanup_failed" || state.CleanupBlockedReason != "process_exit_unconfirmed" ||
					state.CleanupVerified == nil || *state.CleanupVerified || state.ApplyExit != nil ||
					state.DestroyStartedAt != "" || state.DestroyExit != nil || state.CleanupFinishedAt != "" {
					t.Fatalf("unconfirmed exit was not preserved: %+v", state)
				}
				status, err := currentStatus(root, testRunID, testStarted)
				if err != nil || status["cleanup_blocked_reason"] != "process_exit_unconfirmed" {
					t.Fatalf("operator status hid the blocked cleanup: %v, %v", status, err)
				}
			} else if state.Phase != "complete" || state.ApplyExit == nil || *state.ApplyExit != 137 ||
				state.Result != "apply_failed_cleanup_complete" || state.CleanupVerified == nil || !*state.CleanupVerified {
				t.Fatalf("confirmed failed apply did not clean up: %+v", state)
			}
		})
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

	status, err := current.runApply(deadline, make(chan os.Signal))

	if err != nil || status != 130 || current.stopReason != "destroy_deadline" {
		t.Fatalf("status=%d reason=%q error=%v", status, current.stopReason, err)
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

	status, err := current.runApply(deadline, make(chan os.Signal))
	close(runner.released)

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if status != 124 || !errors.Is(err, errProcessExitUnconfirmed) {
		t.Fatalf("status=%d error=%v, want unconfirmed exit", status, err)
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
		after: func(delay time.Duration) <-chan time.Time {
			if delay == applyKillGrace {
				return time.After(time.Millisecond)
			}
			return nil
		},
	}

	status, err := current.stopApply(make(chan executionResult), signals, syscall.SIGINT)

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if status != 124 || !errors.Is(err, errProcessExitUnconfirmed) {
		t.Fatalf("status=%d error=%v, want unconfirmed exit", status, err)
	}
	want := []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL}
	if !slices.Equal(runner.signals, want) {
		t.Fatalf("signals=%v, want %v", runner.signals, want)
	}
}

func TestApplyEscalationSurvivesHeartbeats(t *testing.T) {
	runner := &stubbornExecutor{released: make(chan struct{})}
	var delays []time.Duration
	current := &controller{
		raw: t.TempDir(), runID: testRunID, executor: runner, now: time.Now,
		after: func(delay time.Duration) <-chan time.Time {
			delays = append(delays, delay)
			// A real heartbeat must arrive before each shortened grace expires.
			return time.After(1500 * time.Millisecond)
		},
	}
	applyResult := make(chan executionResult, 1)
	done := make(chan executionResult, 1)
	go func() {
		status, err := current.stopApply(applyResult, make(chan os.Signal), syscall.SIGINT)
		done <- executionResult{status: status, err: err}
	}()
	select {
	case outcome := <-done:
		if outcome.status != 124 || !errors.Is(outcome.err, errProcessExitUnconfirmed) {
			t.Fatalf("result=%+v, want unconfirmed exit", outcome)
		}
	case <-time.After(7 * time.Second):
		applyResult <- executionResult{status: 130}
		<-done
		t.Fatal("heartbeats prevented escalation from completing")
	}
	if !slices.Equal(delays, []time.Duration{applyInterruptGrace, applyTerminateGrace, applyKillGrace}) {
		t.Fatalf("grace periods restarted: %v", delays)
	}
	if !slices.Equal(runner.signals, []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL}) {
		t.Fatalf("signals=%v", runner.signals)
	}
	if _, err := os.Stat(filepath.Join(current.raw, "controller-state.json")); err != nil {
		t.Fatalf("heartbeat did not write state during escalation: %v", err)
	}
}

func TestRepeatedSignalsDoNotSkipExitConfirmation(t *testing.T) {
	runner := &stubbornExecutor{released: make(chan struct{})}
	killed := make(chan struct{})
	current := &controller{
		raw: t.TempDir(), executor: runner, now: time.Now,
		after: func(delay time.Duration) <-chan time.Time {
			if delay == applyKillGrace {
				close(killed)
			}
			return nil
		},
	}
	signals := make(chan os.Signal, 3)
	for range 3 {
		signals <- syscall.SIGINT
	}
	applyResult := make(chan executionResult, 1)
	done := make(chan executionResult, 1)
	go func() {
		status, err := current.stopApply(applyResult, signals, syscall.SIGINT)
		done <- executionResult{status: status, err: err}
	}()
	select {
	case <-killed:
	case <-time.After(2 * time.Second):
		applyResult <- executionResult{status: 137}
		<-done
		t.Fatal("operator signals did not reach SIGKILL")
	}
	select {
	case outcome := <-done:
		t.Fatalf("stop returned without an apply exit: %+v", outcome)
	case <-time.After(50 * time.Millisecond):
	}
	applyResult <- executionResult{status: 137}
	select {
	case outcome := <-done:
		if outcome.err != nil || outcome.status != 137 {
			t.Fatalf("confirmed exit result=%+v", outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("confirmed exit did not release the stop")
	}
	if !slices.Equal(runner.signals, []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL}) {
		t.Fatalf("signals=%v", runner.signals)
	}
}

func TestFinalDeadlineKeepsAnApplyExitThatArrivedWithIt(t *testing.T) {
	raw := t.TempDir()
	// Go chooses at random among ready select cases. Without the final check
	// for a result, about half of these stops would report an unconfirmed exit.
	for attempt := range 200 {
		result := make(chan executionResult, 1)
		runner := &killDeliveryExecutor{result: result}
		current := &controller{
			raw: raw, runID: testRunID, executor: runner, now: time.Now, after: firedTimer,
		}

		status, err := current.stopApply(result, make(chan os.Signal), syscall.SIGINT)

		if err != nil || status != 137 {
			t.Fatalf("attempt %d: status=%d error=%v, want the delivered exit", attempt, status, err)
		}
		if !slices.Equal(runner.signals, []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL}) {
			t.Fatalf("attempt %d: signals=%v", attempt, runner.signals)
		}
	}
}

func TestUnconfirmedApplyStopNamesItsProcessGroup(t *testing.T) {
	runner := &groupedExecutor{stubbornExecutor: stubbornExecutor{released: make(chan struct{})}, group: 4242}
	current := &controller{
		raw: t.TempDir(), runID: testRunID, executor: runner, now: time.Now, after: firedTimer,
	}

	status, err := current.stopApply(make(chan executionResult), make(chan os.Signal), syscall.SIGINT)

	if status != 124 || !errors.Is(err, errProcessExitUnconfirmed) {
		t.Fatalf("status=%d error=%v, want unconfirmed exit", status, err)
	}
	if !strings.Contains(err.Error(), "(process group 4242)") {
		t.Fatalf("error does not name the process group: %v", err)
	}
}

// releaseAfterFirstSignal lets a stubborn apply exit once it has been
// signalled, so a test sees only the signals sent before apply exits.
func releaseAfterFirstSignal(t *testing.T, runner *stubbornExecutor) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		runner.mu.Lock()
		count := len(runner.signals)
		runner.mu.Unlock()
		if count > 0 || time.Now().After(deadline) {
			if count == 0 {
				t.Error("apply received no stop signal")
			}
			close(runner.released)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestStopRequestDuringApplyStartsWithInterrupt(t *testing.T) {
	raw := t.TempDir()
	writeTestJSON(t, filepath.Join(raw, "stop-request.json"), stopRequest{
		SchemaVersion: 1,
		RunID:         testRunID,
		Reason:        "infrastructure_failed",
		RequestedAt:   utcText(testStarted),
	})
	runner := &stubbornExecutor{released: make(chan struct{})}
	signals := make(chan os.Signal, 1)
	// requestStop wakes a controller that is applying with SIGTERM.
	signals <- syscall.SIGTERM
	current := &controller{
		runID:    testRunID,
		commit:   testCommit,
		profile:  "test-profile",
		region:   "us-east-1",
		raw:      raw,
		executor: runner,
		now:      func() time.Time { return testStarted },
		after: func(time.Duration) <-chan time.Time {
			return nil
		},
	}
	go releaseAfterFirstSignal(t, runner)

	status, err := current.runApply(testStarted.Add(destroyAfter), signals)

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if err != nil || status != 137 {
		t.Fatalf("status=%d error=%v, want 137", status, err)
	}
	if !slices.Equal(runner.signals, []os.Signal{syscall.SIGINT}) {
		t.Fatalf("signals=%v, want SIGINT alone before apply exits", runner.signals)
	}
	if current.stopReason != "infrastructure_failed" {
		t.Fatalf("reason=%q, want the stop request's reason", current.stopReason)
	}
}

func TestStateWriteFailureInterruptsApplyWithSIGINT(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &stubbornExecutor{released: make(chan struct{})}
	current := &controller{
		runID:    testRunID,
		commit:   testCommit,
		profile:  "test-profile",
		region:   "us-east-1",
		raw:      filepath.Join(blocker, "raw"),
		executor: runner,
		now:      func() time.Time { return testStarted },
		after: func(time.Duration) <-chan time.Time {
			return nil
		},
	}
	go releaseAfterFirstSignal(t, runner)

	status, err := current.runApply(testStarted.Add(destroyAfter), make(chan os.Signal))

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if err != nil || status != 137 || current.stopReason != "state_write_failed" {
		t.Fatalf("status=%d reason=%q error=%v", status, current.stopReason, err)
	}
	if !slices.Equal(runner.signals, []os.Signal{syscall.SIGINT}) {
		t.Fatalf("signals=%v, want SIGINT alone before apply exits", runner.signals)
	}
}

func TestRequestStopValidatesReasonAndWakesAnApplyingController(t *testing.T) {
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
	signalled := 0
	previous := signalProcess
	signalProcess = func(pid int, _ syscall.Signal) error {
		signalled = pid
		return nil
	}
	defer func() { signalProcess = previous }()

	for _, reason := range []string{"Infrastructure failed", "capture-failed", "1_failed", strings.Repeat("a", 65)} {
		if _, err := requestStop(root, testRunID, reason, testStarted); err == nil || !strings.Contains(err.Error(), "stop reason must") {
			t.Fatalf("reason %q: got %v, want a reason error", reason, err)
		}
	}
	if signalled != 0 {
		t.Fatal("a rejected reason signalled the controller")
	}
	if _, err := os.Stat(filepath.Join(raw, "stop-request.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a rejected reason wrote a stop request: %v", err)
	}

	path, err := requestStop(root, testRunID, "infrastructure_failed", testStarted)
	if err != nil {
		t.Fatal(err)
	}
	var request stopRequest
	if err := readJSON(path, &request, "stop request"); err != nil {
		t.Fatal(err)
	}
	if request.Reason != "infrastructure_failed" || signalled != os.Getpid() {
		t.Fatalf("reason=%q signalled=%d", request.Reason, signalled)
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

	if status, err := current.runApply(testStarted.Add(destroyAfter), make(chan os.Signal)); err != nil || status != 0 {
		t.Fatalf("apply returned %d, %v", status, err)
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

func TestCleanupStopsAfterAnUnconfirmedDestroyExit(t *testing.T) {
	root := t.TempDir()
	raw := prepareRun(t, root)
	writeTestJSON(t, filepath.Join(raw, "01-identity.txt"), map[string]any{
		"aws": map[string]string{"account_id": testAccount},
	})
	runner := &fakeExecutor{failures: map[string]error{"aws-down": errProcessExitUnconfirmed}}
	aws := &fakeAWS{account: testAccount, logsPassed: true}
	current := &controller{
		root: root, raw: raw, runID: testRunID, commit: testCommit,
		executor: runner, aws: aws, now: func() time.Time { return testStarted },
	}
	if status := current.cleanup(testSession(), 137); status != 1 {
		t.Fatalf("status=%d, want failed cleanup", status)
	}
	if len(runner.calls) != 1 || runner.calls[0][1] != "aws-down" || aws.logCalls != 0 {
		t.Fatalf("commands followed the unconfirmed destroy: %v; AWS=%+v", runner.calls, aws)
	}
	var state controllerState
	if err := readJSON(filepath.Join(raw, "controller-state.json"), &state, "state"); err != nil {
		t.Fatal(err)
	}
	if state.Phase != "cleanup_failed" || state.CleanupBlockedReason != "process_exit_unconfirmed" ||
		state.CleanupVerified == nil || *state.CleanupVerified || state.DestroyExit != nil || state.CleanupFinishedAt != "" {
		t.Fatalf("unconfirmed destroy was not preserved: %+v", state)
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

	if status, err := runner.Run([]string{"/bin/sh", "-c", "exit 7"}, nil, io.Discard); err != nil || status != 7 {
		t.Fatalf("status=%d error=%v, want 7", status, err)
	}
}

func TestProcessExecutorRejectsUnconfirmedExitAndReuse(t *testing.T) {
	oldGrace, oldKillGrace, oldPoll := groupExitGrace, groupKillGrace, groupPollInterval
	groupExitGrace, groupKillGrace, groupPollInterval = 0, time.Millisecond, time.Millisecond
	defer func() { groupExitGrace, groupKillGrace, groupPollInterval = oldGrace, oldKillGrace, oldPoll }()
	root := t.TempDir()
	runner := &processExecutor{
		root:        root,
		groupSignal: func(int, syscall.Signal) error { return syscall.EPERM },
	}
	if _, err := runner.Run([]string{"/bin/sh", "-c", "exit 0"}, nil, io.Discard); !errors.Is(err, errProcessExitUnconfirmed) {
		t.Fatalf("unconfirmed group exit returned %v", err)
	}
	marker := filepath.Join(root, "second-command")
	if _, err := runner.Run([]string{"/bin/sh", "-c", `: > "$1"`, "sh", marker}, nil, io.Discard); !errors.Is(err, errProcessExitUnconfirmed) ||
		!strings.Contains(err.Error(), "process group ") {
		t.Fatalf("executor reuse returned %v", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("second command was started: %v", err)
	}
}

func TestGroupWaitRejectsFailedKillAndSurvivingMembers(t *testing.T) {
	oldGrace, oldKillGrace, oldPoll := groupExitGrace, groupKillGrace, groupPollInterval
	groupExitGrace, groupKillGrace, groupPollInterval = 0, time.Millisecond, time.Millisecond
	defer func() { groupExitGrace, groupKillGrace, groupPollInterval = oldGrace, oldKillGrace, oldPoll }()
	for _, probeErr := range []error{nil, syscall.EPERM} {
		for _, killErr := range []error{syscall.EPERM, syscall.EINVAL, nil} {
			kills := 0
			err := waitForGroupExit(42, func(_ int, value syscall.Signal) error {
				if value == syscall.SIGKILL {
					kills++
					return killErr
				}
				return probeErr // The group remains present even after the kill request.
			})
			if !errors.Is(err, errProcessExitUnconfirmed) || kills != 1 {
				t.Fatalf("probe error=%v kill error=%v: result=%v kills=%d", probeErr, killErr, err, kills)
			}
		}
	}
}

func TestGroupWaitKeepsWaitingThroughPermissionErrors(t *testing.T) {
	oldGrace, oldPoll := groupExitGrace, groupPollInterval
	groupExitGrace, groupPollInterval = time.Second, time.Millisecond
	defer func() { groupExitGrace, groupPollInterval = oldGrace, oldPoll }()
	probes, kills := 0, 0

	err := waitForGroupExit(42, func(_ int, value syscall.Signal) error {
		if value == syscall.SIGKILL {
			kills++
			return nil
		}
		probes++
		if probes < 3 {
			return syscall.EPERM // A member that has exited but awaits reaping.
		}
		return syscall.ESRCH
	})

	if err != nil || probes != 3 || kills != 0 {
		t.Fatalf("result=%v probes=%d kills=%d, want exit confirmed after EPERM", err, probes, kills)
	}
}

func TestGroupWaitOutlastsAMemberAwaitingItsReaper(t *testing.T) {
	previous := groupExitGrace
	groupExitGrace = 5 * time.Second
	defer func() { groupExitGrace = previous }()
	command := exec.Command("/bin/sh", "-c", "exit 0")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	group := command.Process.Pid
	// Leave the exited leader unreaped, so the group's only member is a
	// zombie: macOS then reports EPERM for the group, and Linux success.
	time.Sleep(200 * time.Millisecond)
	result := make(chan error, 1)
	go func() {
		result <- waitForGroupExit(group, func(target int, value syscall.Signal) error {
			return syscall.Kill(-target, value)
		})
	}()
	time.Sleep(300 * time.Millisecond)
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("group wait failed while its member awaited reaping: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("group wait did not finish after the member was reaped")
	}
}

func TestProcessExecutorStartsAProcessGroupAndSignalsIt(t *testing.T) {
	root := t.TempDir()
	ready := filepath.Join(root, "ready")
	runner := &processExecutor{root: root}
	result := make(chan executionResult, 1)
	go func() {
		status, err := runner.Run(
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
		result <- executionResult{status: status, err: err}
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
	if group := runner.ActiveGroup(); group != pgid {
		t.Fatalf("active group=%d, want %d", group, pgid)
	}
	runner.Signal(syscall.SIGTERM)
	select {
	case outcome := <-result:
		if outcome.err != nil || outcome.status != 42 {
			t.Fatalf("result=%+v, want 42", outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("signalled process group did not exit")
	}
	if group := runner.ActiveGroup(); group != 0 {
		t.Fatalf("active group=%d after confirmed exit", group)
	}
}

func TestProcessExecutorWaitsForTheWholeProcessGroup(t *testing.T) {
	root := t.TempDir()
	finished := filepath.Join(root, "finished")
	runner := &processExecutor{root: root}

	// The shell exits at once. Its background member keeps the group alive
	// without holding the output pipe, as Terraform outlived make.
	status, runErr := runner.Run(
		[]string{"/bin/sh", "-c", `(sleep 1; : > "$1") >/dev/null 2>&1 & exit 3`, "sh", finished},
		nil,
		io.Discard,
	)

	if runErr != nil || status != 3 {
		t.Fatalf("status=%d error=%v, want 3", status, runErr)
	}
	if _, err := os.Stat(finished); err != nil {
		t.Fatalf("Run returned before its process group exited: %v", err)
	}
}

func TestProcessExecutorKillsGroupMembersThatOutliveTheGrace(t *testing.T) {
	previous := groupExitGrace
	groupExitGrace = 200 * time.Millisecond
	defer func() { groupExitGrace = previous }()
	root := t.TempDir()
	leader := filepath.Join(root, "leader")
	runner := &processExecutor{root: root}
	started := time.Now()

	status, runErr := runner.Run(
		[]string{
			"/bin/sh",
			"-c",
			`echo $$ > "$1"; (trap '' INT TERM; sleep 30) >/dev/null 2>&1 & exit 0`,
			"sh",
			leader,
		},
		nil,
		io.Discard,
	)

	if runErr != nil || status != 0 {
		t.Fatalf("status=%d error=%v, want 0", status, runErr)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("Run waited %s for a member that ignored signals", elapsed)
	}
	requireEmptyProcessGroup(t, leader)
}

func TestProcessExecutorBoundsAnOutputPipeHeldByAGroupMember(t *testing.T) {
	previous := groupExitGrace
	groupExitGrace = 200 * time.Millisecond
	defer func() { groupExitGrace = previous }()
	root := t.TempDir()
	leader := filepath.Join(root, "leader")
	runner := &processExecutor{root: root}
	var output bytes.Buffer // Not a file, so exec copies it through a pipe.
	started := time.Now()

	// The shell exits at once. Its background member keeps the output pipe
	// open, as a descendant of an interrupted make could.
	status, runErr := runner.Run(
		[]string{"/bin/sh", "-c", `echo $$ > "$1"; echo started; sleep 30 & exit 0`, "sh", leader},
		nil,
		&output,
	)

	if runErr != nil || status != 0 {
		t.Fatalf("status=%d error=%v, want 0", status, runErr)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("Run waited %s for a member holding its output pipe", elapsed)
	}
	if !strings.Contains(output.String(), "started") {
		t.Fatalf("output=%q, want the command's output", output.String())
	}
	requireEmptyProcessGroup(t, leader)
}

// requireEmptyProcessGroup checks the group whose leader wrote its PID to path.
func requireEmptyProcessGroup(t *testing.T, path string) {
	t.Helper()
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	group, err := strconv.Atoi(strings.TrimSpace(string(value)))
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(-group, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("process group %d still has members: %v", group, err)
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
