package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	destroyAfter        = 150 * time.Minute
	hardAfter           = 180 * time.Minute
	pollInterval        = time.Second
	destroyRetryDelay   = 30 * time.Second
	destroyAttempts     = 3
	identityRetryDelay  = 30 * time.Second
	identityAttempts    = 3
	inventoryRetryDelay = 30 * time.Second
	inventoryAttempts   = 3
	applyInterruptGrace = 30 * time.Second
	applyTerminateGrace = 10 * time.Second
	applyKillGrace      = 5 * time.Second
	heartbeatMaxAge     = 5 * time.Second
	heartbeatFutureSkew = 30 * time.Second
	awsCommandTimeout   = 45 * time.Second
)

var (
	runIDPattern   = regexp.MustCompile(`^[0-9]{8}T[0-9]{6}Z$`)
	commitPattern  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	accountPattern = regexp.MustCompile(`^[0-9]{12}$`)
	logPrefixes    = []string{"/aws/eks/mlp-", "/aws/msk/mlp-"}
)

type liveRunError struct {
	message string
}

func (e *liveRunError) Error() string { return e.message }

func fail(format string, values ...any) error {
	return &liveRunError{message: fmt.Sprintf(format, values...)}
}

type sessionReceipt struct {
	SchemaVersion     int                `json:"schema_version"`
	RunID             string             `json:"run_id"`
	Commit            string             `json:"commit"`
	Region            string             `json:"region"`
	Operator          string             `json:"operator"`
	BillableStartedAt string             `json:"billable_started_at"`
	DestroyDeadline   string             `json:"destroy_deadline"`
	HardDeadline      string             `json:"hard_deadline"`
	Limits            map[string]float64 `json:"limits"`
}

type preflightReceipt struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	Commit        string `json:"commit"`
	Result        string `json:"result"`
}

type capturePlan struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	Commit        string `json:"commit"`
	PaidWindow    struct {
		EvidenceDeadlineMinutes int `json:"evidence_deadline_minutes"`
		HardDeadlineMinutes     int `json:"hard_deadline_minutes"`
	} `json:"paid_window"`
}

type goPacket struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	SourceCommit  string `json:"source_commit"`
	Decision      string `json:"decision"`
	AbortCommand  string `json:"abort_command"`
	Gate          struct {
		Passed bool `json:"passed"`
	} `json:"gate"`
}

type accountReceipt struct {
	AWS struct {
		AccountID string `json:"account_id"`
	} `json:"aws"`
}

type controllerState struct {
	SchemaVersion      int    `json:"schema_version"`
	RunID              string `json:"run_id"`
	Commit             string `json:"commit"`
	Operator           string `json:"operator"`
	Region             string `json:"region"`
	ControllerPID      int    `json:"controller_pid"`
	CreatedAt          string `json:"created_at"`
	UpdatedAt          string `json:"updated_at"`
	Phase              string `json:"phase"`
	Result             string `json:"result,omitempty"`
	StopReason         string `json:"stop_reason,omitempty"`
	ApplyExit          *int   `json:"apply_exit,omitempty"`
	DestroyStartedAt   string `json:"destroy_started_at,omitempty"`
	DestroyExit        *int   `json:"destroy_exit,omitempty"`
	LogCleanupPassed   *bool  `json:"log_cleanup_passed,omitempty"`
	TerraformStateExit *int   `json:"terraform_state_exit,omitempty"`
	TranscriptPassed   *bool  `json:"transcript_passed,omitempty"`
	InventoryExit      *int   `json:"inventory_exit,omitempty"`
	CostExit           *int   `json:"cost_exit,omitempty"`
	CleanupFinishedAt  string `json:"cleanup_finished_at,omitempty"`
	CleanupOverdue     bool   `json:"cleanup_overdue"`
	CleanupVerified    *bool  `json:"cleanup_verified,omitempty"`
	Error              string `json:"error,omitempty"`
}

type stopRequest struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	Reason        string `json:"reason"`
	RequestedAt   string `json:"requested_at"`
}

type executor interface {
	Run(arguments []string, environment []string, output io.Writer) int
	Signal(os.Signal)
	ClearPending()
}

type processExecutor struct {
	root    string
	mu      sync.Mutex
	child   *exec.Cmd
	pending syscall.Signal
}

func (e *processExecutor) Run(arguments []string, environment []string, output io.Writer) int {
	if len(arguments) == 0 {
		return 127
	}
	command := exec.Command(arguments[0], arguments[1:]...)
	command.Dir = e.root
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if environment != nil {
		command.Env = environment
	}
	if output == nil {
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		command.Stdin = os.Stdin
	} else {
		command.Stdout = output
		command.Stderr = output
	}
	if err := command.Start(); err != nil {
		e.mu.Lock()
		e.pending = 0
		e.mu.Unlock()
		fmt.Fprintf(os.Stderr, "%s: %v\n", arguments[0], err)
		return 127
	}
	e.mu.Lock()
	e.child = command
	pending := e.pending
	e.pending = 0
	e.mu.Unlock()
	if pending != 0 {
		_ = syscall.Kill(-command.Process.Pid, pending)
	}
	err := command.Wait()
	e.mu.Lock()
	if e.child == command {
		e.child = nil
	}
	e.mu.Unlock()
	if err == nil {
		return 0
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return exitError.ExitCode()
	}
	fmt.Fprintf(os.Stderr, "%s: %v\n", arguments[0], err)
	return 127
}

func (e *processExecutor) Signal(value os.Signal) {
	signalValue, ok := value.(syscall.Signal)
	if !ok {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.child == nil || e.child.Process == nil {
		e.pending = signalValue
		return
	}
	_ = syscall.Kill(-e.child.Process.Pid, signalValue)
}

func (e *processExecutor) ClearPending() {
	e.mu.Lock()
	e.pending = 0
	e.mu.Unlock()
}

type awsClient interface {
	Account() (string, error)
	DeleteRuntimeLogs(io.Writer) bool
}

type awsCLI struct {
	root        string
	environment []string
}

func writeLine(output io.Writer, values ...any) bool {
	_, err := fmt.Fprintln(output, values...)
	return err == nil
}

func writeFormatted(output io.Writer, format string, values ...any) bool {
	_, err := fmt.Fprintf(output, format, values...)
	return err == nil
}

type writeNopCloser struct {
	io.Writer
}

func (writeNopCloser) Close() error { return nil }

func (a *awsCLI) output(arguments ...string) ([]byte, error) {
	commandArguments := append(
		[]string{"--cli-connect-timeout", "10", "--cli-read-timeout", "30"},
		arguments...,
	)
	ctx, cancel := context.WithTimeout(context.Background(), awsCommandTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, "aws", commandArguments...)
	command.Dir = a.root
	command.Env = a.environment
	var stderr bytes.Buffer
	command.Stderr = &stderr
	value, err := command.Output()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf(
				"aws %s timed out after %s",
				strings.Join(arguments, " "),
				awsCommandTimeout,
			)
		}
		return nil, fmt.Errorf("aws %s: %s", strings.Join(arguments, " "), strings.TrimSpace(stderr.String()))
	}
	return value, nil
}

func (a *awsCLI) Account() (string, error) {
	value, err := a.output("sts", "get-caller-identity", "--output", "json")
	if err != nil {
		return "", err
	}
	var identity struct {
		Account string `json:"Account"`
	}
	if err := json.Unmarshal(value, &identity); err != nil || !accountPattern.MatchString(identity.Account) {
		return "", fail("current AWS identity response is invalid")
	}
	return identity.Account, nil
}

func (a *awsCLI) DeleteRuntimeLogs(transcript io.Writer) bool {
	passed := writeLine(transcript, "\n==> delete service-created M4 log groups")
	value, err := a.output("logs", "describe-log-groups", "--output", "json")
	if err != nil {
		_ = writeLine(transcript, err)
		return false
	}
	var response struct {
		LogGroups []struct {
			Name string `json:"logGroupName"`
		} `json:"logGroups"`
	}
	if err := json.Unmarshal(value, &response); err != nil {
		_ = writeFormatted(transcript, "invalid log-group response: %v\n", err)
		return false
	}
	names := make([]string, 0)
	for _, group := range response.LogGroups {
		for _, prefix := range logPrefixes {
			if strings.HasPrefix(group.Name, prefix) {
				names = append(names, group.Name)
				break
			}
		}
	}
	sort.Strings(names)
	for _, name := range names {
		_, err := a.output("logs", "delete-log-group", "--log-group-name", name)
		if err != nil {
			_ = writeFormatted(transcript, "delete %s: %v\n", name, err)
			passed = false
			continue
		}
		if !writeFormatted(transcript, "delete %s: exit 0\n", name) {
			passed = false
		}
	}
	if len(names) == 0 {
		if !writeLine(transcript, "no matching log groups") {
			passed = false
		}
	}
	return passed
}

type controller struct {
	root       string
	runID      string
	commit     string
	operator   string
	profile    string
	region     string
	raw        string
	executor   executor
	aws        awsClient
	now        func() time.Time
	sleep      func(time.Duration)
	after      func(time.Duration) <-chan time.Time
	state      controllerState
	stopReason string
}

func utcText(value time.Time) string {
	return value.UTC().Truncate(time.Second).Format(time.RFC3339)
}

func parseUTC(value, field string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil || parsed.Format(time.RFC3339) != value || parsed.Location() != time.UTC {
		return time.Time{}, fail("%s must use UTC YYYY-MM-DDTHH:MM:SSZ", field)
	}
	return parsed, nil
}

func validateIdentifiers(runID, commit string) error {
	if !runIDPattern.MatchString(runID) {
		return fail("AWS_RUN_ID must use UTC YYYYMMDDTHHMMSSZ")
	}
	if !commitPattern.MatchString(commit) {
		return fail("AWS_APPROVED_COMMIT must be a full lowercase git SHA")
	}
	return nil
}

func rawDirectory(root, runID string) (string, error) {
	if !runIDPattern.MatchString(runID) {
		return "", fail("AWS_RUN_ID must use UTC YYYYMMDDTHHMMSSZ")
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	raw := filepath.Join(absoluteRoot, ".evidence", "m4", runID)
	relative, err := filepath.Rel(absoluteRoot, raw)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fail("evidence path escapes the repository: %s", raw)
	}
	current := absoluteRoot
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if statErr != nil {
			return "", statErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fail("evidence path contains a symlink: %s", current)
		}
	}
	return raw, nil
}

func readJSON(path string, destination any, description string) error {
	value, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return fail("%s is missing: %s", description, path)
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(value, destination); err != nil {
		return fail("%s is invalid JSON: %v", description, err)
	}
	return nil
}

func writeJSONAtomic(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return err
	}
	return os.Chmod(path, 0o600)
}

func validateSession(session sessionReceipt, runID, commit string) (time.Time, time.Time, time.Time, error) {
	if session.SchemaVersion != 1 || session.RunID != runID || session.Commit != commit || session.Region != "us-east-1" {
		return time.Time{}, time.Time{}, time.Time{}, fail("session receipt is not bound to this run and commit")
	}
	started, err := parseUTC(session.BillableStartedAt, "billable start")
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	destroy, err := parseUTC(session.DestroyDeadline, "destroy deadline")
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	hard, err := parseUTC(session.HardDeadline, "hard deadline")
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	if destroy.Sub(started) != destroyAfter || hard.Sub(started) != hardAfter {
		return time.Time{}, time.Time{}, time.Time{}, fail("session receipt does not contain the fixed deadlines")
	}
	return started, destroy, hard, nil
}

func requirePreparedRun(root, runID, commit string) (string, error) {
	if err := validateIdentifiers(runID, commit); err != nil {
		return "", err
	}
	raw, err := rawDirectory(root, runID)
	if err != nil {
		return "", err
	}
	var preflight preflightReceipt
	if err := readJSON(filepath.Join(raw, "00-preflight.json"), &preflight, "preflight receipt"); err != nil {
		return "", err
	}
	if preflight.SchemaVersion != 1 || preflight.RunID != runID || preflight.Commit != commit || preflight.Result != "passed" {
		return "", fail("preflight receipt is not a pass for this run and commit")
	}
	var plan capturePlan
	if err := readJSON(filepath.Join(raw, "capture-plan.json"), &plan, "capture plan"); err != nil {
		return "", err
	}
	if plan.SchemaVersion != 1 || plan.RunID != runID || plan.Commit != commit || plan.PaidWindow.EvidenceDeadlineMinutes != 150 || plan.PaidWindow.HardDeadlineMinutes != 180 {
		return "", fail("capture plan does not contain the fixed paid window")
	}
	var decision goPacket
	if err := readJSON(filepath.Join(raw, "06-go-no-go.json"), &decision, "GO packet"); err != nil {
		return "", err
	}
	if decision.SchemaVersion != 1 || decision.RunID != runID || decision.SourceCommit != commit || decision.Decision != "go" || !decision.Gate.Passed || decision.AbortCommand != "make aws-down" {
		return "", fail("GO packet is not a pass for this run and commit")
	}
	if _, err := os.Stat(filepath.Join(raw, "00-session.json")); err == nil {
		return "", fail("session receipt already exists for this run")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return raw, nil
}

func restrictedAWSEnvironment(profile, region string) []string {
	values := []string{
		"HOME=" + os.Getenv("HOME"),
		"PATH=" + os.Getenv("PATH"),
		"AWS_PROFILE=" + profile,
		"AWS_REGION=" + region,
		"AWS_DEFAULT_REGION=" + region,
		"AWS_MAX_ATTEMPTS=1",
		"AWS_RETRY_MODE=standard",
		"AWS_PAGER=",
	}
	if temporary := os.Getenv("TMPDIR"); temporary != "" {
		values = append(values, "TMPDIR="+temporary)
	}
	return values
}

func newController(root, runID, commit, operator, profile, region string) (*controller, error) {
	if err := validateIdentifiers(runID, commit); err != nil {
		return nil, err
	}
	operator = strings.TrimSpace(operator)
	if operator == "" || strings.ContainsAny(operator, "\r\n") {
		return nil, fail("M4_OPERATOR must name the executing operator")
	}
	if strings.TrimSpace(profile) == "" {
		return nil, fail("AWS profile is required")
	}
	if region != "us-east-1" {
		return nil, fail("M4 uses the fixed us-east-1 region")
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	execRunner := &processExecutor{root: absoluteRoot}
	return &controller{
		root:     absoluteRoot,
		runID:    runID,
		commit:   commit,
		operator: operator,
		profile:  profile,
		region:   region,
		executor: execRunner,
		aws: &awsCLI{
			root:        absoluteRoot,
			environment: restrictedAWSEnvironment(profile, region),
		},
		now:   time.Now,
		sleep: time.Sleep,
		after: time.After,
	}, nil
}

func (c *controller) updateState(phase string) error {
	c.state.Phase = phase
	c.state.UpdatedAt = utcText(c.now())
	return writeJSONAtomic(filepath.Join(c.raw, "controller-state.json"), c.state)
}

func (c *controller) verifySource() error {
	command := exec.Command("git", "rev-parse", "HEAD")
	command.Dir = c.root
	value, err := command.Output()
	if err != nil {
		return fmt.Errorf("git rev-parse HEAD: %w", err)
	}
	head := strings.TrimSpace(string(value))
	if head != c.commit {
		return fail("HEAD %s does not match approved commit %s", head, c.commit)
	}
	command = exec.Command("git", "status", "--porcelain", "--untracked-files=all")
	command.Dir = c.root
	value, err = command.Output()
	if err != nil {
		return fmt.Errorf("git status: %w", err)
	}
	if strings.TrimSpace(string(value)) != "" {
		return fail("worktree must be clean before a live AWS run")
	}
	return nil
}

func (c *controller) makeArguments(target string, extra ...string) []string {
	arguments := []string{
		"make",
		target,
		"AWS_RUN_ID=" + c.runID,
		"AWS_APPROVED_COMMIT=" + c.commit,
		"AWS_PROFILE_NAME=" + c.profile,
		"AWS_REAL_REGION=" + c.region,
	}
	return append(arguments, extra...)
}

func (c *controller) createSession() (sessionReceipt, error) {
	started := utcText(c.now())
	status := c.executor.Run([]string{
		"python3",
		filepath.Join(c.root, "scripts", "m4-evidence.py"),
		"start-session",
		"--run-id", c.runID,
		"--started-at", started,
		"--operator", c.operator,
		"--region", c.region,
	}, nil, nil)
	if status != 0 {
		return sessionReceipt{}, fail("failed to create the live session receipt")
	}
	var session sessionReceipt
	if err := readJSON(filepath.Join(c.raw, "00-session.json"), &session, "session receipt"); err != nil {
		return sessionReceipt{}, err
	}
	if _, _, _, err := validateSession(session, c.runID, c.commit); err != nil {
		return sessionReceipt{}, err
	}
	return session, nil
}

func (c *controller) sleepFor(value time.Duration) {
	if c.sleep != nil {
		c.sleep(value)
		return
	}
	time.Sleep(value)
}

func (c *controller) afterDelay(value time.Duration) <-chan time.Time {
	if c.after != nil {
		return c.after(value)
	}
	return time.After(value)
}

func (c *controller) retryPause(value time.Duration, phase string) bool {
	if c.after == nil {
		c.sleepFor(value)
		return c.updateState(phase) == nil
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	done := c.afterDelay(value)
	for {
		select {
		case <-done:
			return true
		case <-ticker.C:
			if err := c.updateState(phase); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return false
			}
		}
	}
}

type accountLookup struct {
	account string
	err     error
}

func (c *controller) accountWithHeartbeat() (string, error, bool) {
	result := make(chan accountLookup, 1)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	go func() {
		account, err := c.aws.Account()
		result <- accountLookup{account: account, err: err}
	}()
	heartbeatPassed := true
	for {
		select {
		case lookup := <-result:
			return lookup.account, lookup.err, heartbeatPassed
		case <-ticker.C:
			if err := c.updateState("destroying"); err != nil {
				fmt.Fprintln(os.Stderr, err)
				heartbeatPassed = false
			}
		}
	}
}

func (c *controller) deleteLogsWithHeartbeat(output io.Writer) (bool, bool) {
	result := make(chan bool, 1)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	go func() {
		result <- c.aws.DeleteRuntimeLogs(output)
	}()
	heartbeatPassed := true
	for {
		select {
		case passed := <-result:
			return passed, heartbeatPassed
		case <-ticker.C:
			if err := c.updateState("destroying"); err != nil {
				fmt.Fprintln(os.Stderr, err)
				heartbeatPassed = false
			}
		}
	}
}

func (c *controller) verifyAccount(output io.Writer) (bool, error) {
	var receipt accountReceipt
	if err := readJSON(filepath.Join(c.raw, "01-identity.txt"), &receipt, "account receipt"); err != nil {
		return true, err
	}
	if !accountPattern.MatchString(receipt.AWS.AccountID) {
		return true, fail("account receipt has no valid account id")
	}
	var lastError error
	heartbeatPassed := true
	for attempt := 1; attempt <= identityAttempts; attempt++ {
		current, err, heartbeatOK := c.accountWithHeartbeat()
		heartbeatPassed = heartbeatPassed && heartbeatOK
		if err == nil {
			if current != receipt.AWS.AccountID {
				return heartbeatPassed, fail("current AWS account does not match the staged account")
			}
			if output != nil {
				_ = writeFormatted(output, "identity verification attempt %d: account matched\n", attempt)
			}
			return heartbeatPassed, nil
		}
		lastError = err
		if output != nil {
			_ = writeFormatted(output, "identity verification attempt %d failed: %v\n", attempt, err)
		}
		if attempt < identityAttempts {
			heartbeatPassed = c.retryPause(identityRetryDelay, "destroying") && heartbeatPassed
		}
	}
	return heartbeatPassed, fail(
		"current AWS identity could not be verified after %d attempts: %v",
		identityAttempts,
		lastError,
	)
}

func escalationAfter(value syscall.Signal) (syscall.Signal, time.Duration, bool) {
	switch value {
	case syscall.SIGINT:
		return syscall.SIGTERM, applyInterruptGrace, true
	case syscall.SIGTERM:
		return syscall.SIGKILL, applyTerminateGrace, true
	case syscall.SIGKILL:
		return 0, applyKillGrace, false
	default:
		return syscall.SIGTERM, applyInterruptGrace, true
	}
}

func (c *controller) stopRequestReason(fallback string) string {
	var request stopRequest
	if err := readJSON(filepath.Join(c.raw, "stop-request.json"), &request, "stop request"); err == nil &&
		request.SchemaVersion == 1 && request.RunID == c.runID && strings.TrimSpace(request.Reason) != "" {
		return request.Reason
	}
	return fallback
}

func (c *controller) stopApply(
	result <-chan int,
	signalChannel <-chan os.Signal,
	initial syscall.Signal,
) int {
	currentSignal := initial
	c.executor.Signal(currentSignal)
	heartbeat := time.NewTicker(pollInterval)
	defer heartbeat.Stop()
	for {
		nextSignal, wait, canEscalate := escalationAfter(currentSignal)
		select {
		case status := <-result:
			return status
		case <-c.afterDelay(wait):
			if !canEscalate {
				fmt.Fprintln(os.Stderr, "apply process did not exit after SIGKILL; starting cleanup")
				return 124
			}
			currentSignal = nextSignal
			c.executor.Signal(currentSignal)
		case <-signalChannel:
			if canEscalate {
				currentSignal = nextSignal
				c.executor.Signal(currentSignal)
			} else {
				return 124
			}
		case <-heartbeat.C:
			if err := c.updateState("applying"); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}
	}
}

func (c *controller) runApply(destroyDeadline time.Time, signalChannel <-chan os.Signal) int {
	result := make(chan int, 1)
	environment := append(os.Environ(), "MLP_AWS_LIVE_CONTROLLER_PID="+strconv.Itoa(os.Getpid()))
	deadlinePoll := time.NewTicker(pollInterval)
	defer deadlinePoll.Stop()
	defer c.executor.ClearPending()
	go func() {
		result <- c.executor.Run(
			c.makeArguments("aws-up", "MLP_AWS_LIVE_CONTROLLER_PID="+strconv.Itoa(os.Getpid())),
			environment,
			nil,
		)
	}()
	for {
		if destroyDeadlineReached(c.now(), destroyDeadline) {
			c.stopReason = "destroy_deadline"
			return c.stopApply(result, signalChannel, syscall.SIGINT)
		}
		select {
		case status := <-result:
			return status
		case received := <-signalChannel:
			c.stopReason = c.stopRequestReason(received.String())
			initial, ok := received.(syscall.Signal)
			if !ok {
				initial = syscall.SIGTERM
			}
			return c.stopApply(result, signalChannel, initial)
		case <-deadlinePoll.C:
			if err := c.updateState("applying"); err != nil {
				fmt.Fprintln(os.Stderr, err)
				c.stopReason = "state_write_failed"
				return c.stopApply(result, signalChannel, syscall.SIGTERM)
			}
		}
	}
}

func (c *controller) waitForStop(destroyDeadline time.Time, signalChannel <-chan os.Signal) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	stopPath := filepath.Join(c.raw, "stop-request.json")
	for {
		if destroyDeadlineReached(c.now(), destroyDeadline) {
			c.stopReason = "destroy_deadline"
			return nil
		}
		select {
		case received := <-signalChannel:
			c.stopReason = received.String()
			return nil
		case <-ticker.C:
			var request stopRequest
			err := readJSON(stopPath, &request, "stop request")
			if err == nil {
				if request.SchemaVersion != 1 || request.RunID != c.runID {
					return fail("stop request is not bound to this run")
				}
				c.stopReason = request.Reason
				if c.stopReason == "" {
					c.stopReason = "operator_stop"
				}
				return nil
			}
			var liveError *liveRunError
			if !errors.As(err, &liveError) || !strings.Contains(err.Error(), "is missing") {
				return err
			}
			if err := c.updateState("live"); err != nil {
				return err
			}
		}
	}
}

func destroyDeadlineReached(now, deadline time.Time) bool {
	return !now.Before(deadline)
}

func intPointer(value int) *int    { return &value }
func boolPointer(value bool) *bool { return &value }

func (c *controller) runWithHeartbeat(
	arguments []string,
	environment []string,
	output io.Writer,
	phase string,
) (int, bool) {
	result := make(chan int, 1)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	go func() {
		result <- c.executor.Run(arguments, environment, output)
	}()
	heartbeatPassed := true
	for {
		select {
		case status := <-result:
			return status, heartbeatPassed
		case <-ticker.C:
			if err := c.updateState(phase); err != nil {
				fmt.Fprintln(os.Stderr, err)
				heartbeatPassed = false
			}
		}
	}
}

func (c *controller) cleanup(session sessionReceipt, applyStatus int) int {
	_, _, hardDeadline, err := validateSession(session, c.runID, c.commit)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if c.stopReason == "" {
		if applyStatus != 0 {
			c.stopReason = "apply_failed"
		} else {
			c.stopReason = "operator_stop"
		}
	}
	c.state.StopReason = c.stopReason
	c.state.DestroyStartedAt = utcText(c.now())
	stateEvidencePassed := true
	if err := c.updateState("destroying"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		stateEvidencePassed = false
	}
	c.executor.ClearPending()

	transcriptPath := filepath.Join(c.raw, "20-destroy.txt")
	transcriptFile, transcriptOpenErr := os.OpenFile(
		transcriptPath,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0o600,
	)
	transcriptPassed := transcriptOpenErr == nil
	var transcript io.WriteCloser
	if transcriptOpenErr != nil {
		fmt.Fprintln(os.Stderr, transcriptOpenErr)
		transcript = writeNopCloser{Writer: os.Stderr}
	} else {
		transcript = transcriptFile
	}
	identityHeartbeatPassed, identityErr := c.verifyAccount(transcript)
	heartbeatPassed := stateEvidencePassed && identityHeartbeatPassed
	if identityErr != nil {
		recovery := fmt.Sprintf(
			"hourly EKS, MSK, RDS, network, or log resources may still be running; "+
				"verify AWS_PROFILE_NAME=%s against 01-identity.txt, then run "+
				"make aws-down AWS_PROFILE_NAME=%s AWS_REAL_REGION=%s",
			c.profile,
			c.profile,
			c.region,
		)
		transcriptPassed = writeFormatted(transcript, "%v\n%s\n", identityErr, recovery) &&
			transcriptPassed
		if closeErr := transcript.Close(); closeErr != nil {
			transcriptPassed = false
		}
		c.state.Error = identityErr.Error()
		c.state.TranscriptPassed = boolPointer(transcriptPassed)
		c.state.CleanupVerified = boolPointer(false)
		_ = c.updateState("cleanup_failed")
		fmt.Fprintln(os.Stderr, identityErr)
		fmt.Fprintln(os.Stderr, recovery)
		return 1
	}

	destroyStatus := 1
	for attempt := 1; attempt <= destroyAttempts; attempt++ {
		if !writeFormatted(transcript, "==> destroy attempt %d at %s\n", attempt, utcText(c.now())) {
			transcriptPassed = false
		}
		var heartbeatOK bool
		destroyStatus, heartbeatOK = c.runWithHeartbeat(
			c.makeArguments("aws-down", "AWS_DESTROY_ARGS=-auto-approve"),
			nil,
			transcript,
			"destroying",
		)
		heartbeatPassed = heartbeatPassed && heartbeatOK
		if !writeFormatted(transcript, "destroy exit: %d\n", destroyStatus) {
			transcriptPassed = false
		}
		if destroyStatus == 0 {
			break
		}
		if attempt < destroyAttempts && c.now().Before(hardDeadline) {
			heartbeatPassed = c.retryPause(destroyRetryDelay, "destroying") && heartbeatPassed
		}
	}
	logsPassed, heartbeatOK := c.deleteLogsWithHeartbeat(transcript)
	heartbeatPassed = heartbeatPassed && heartbeatOK
	stateStatus, heartbeatOK := c.runWithHeartbeat(
		c.makeArguments("aws-state-empty"),
		nil,
		transcript,
		"destroying",
	)
	heartbeatPassed = heartbeatPassed && heartbeatOK
	if !writeFormatted(transcript, "terraform state check exit: %d\n", stateStatus) {
		transcriptPassed = false
	}
	if closeErr := transcript.Close(); closeErr != nil {
		transcriptPassed = false
	}
	c.state.DestroyExit = intPointer(destroyStatus)
	c.state.LogCleanupPassed = boolPointer(logsPassed)
	c.state.TerraformStateExit = intPointer(stateStatus)
	c.state.TranscriptPassed = boolPointer(transcriptPassed)
	_ = c.updateState("verifying")

	inventoryStatus := 1
	for attempt := 1; attempt <= inventoryAttempts; attempt++ {
		inventoryStatus, heartbeatOK = c.runWithHeartbeat(
			c.makeArguments(
				"aws-inventory-empty",
				"AWS_INVENTORY_FILE="+filepath.Join(c.raw, "21-inventory-after.json"),
			),
			nil,
			nil,
			"verifying",
		)
		heartbeatPassed = heartbeatPassed && heartbeatOK
		if inventoryStatus == 0 {
			break
		}
		if attempt < inventoryAttempts && c.now().Before(hardDeadline) {
			heartbeatPassed = c.retryPause(inventoryRetryDelay, "verifying") && heartbeatPassed
		}
	}
	costPath := filepath.Join(c.raw, "22-cost-immediate.txt")
	costOutput, costOpenErr := os.OpenFile(costPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	costStatus := 1
	if costOpenErr == nil {
		costStatus, heartbeatOK = c.runWithHeartbeat(
			c.makeArguments("aws-cost"),
			nil,
			costOutput,
			"verifying",
		)
		heartbeatPassed = heartbeatPassed && heartbeatOK
		if closeErr := costOutput.Close(); closeErr != nil {
			costStatus = 1
		}
	}

	finished := c.now()
	overdue := finished.After(hardDeadline)
	cleanupPassed := stateStatus == 0 && inventoryStatus == 0
	evidencePassed := destroyStatus == 0 &&
		logsPassed &&
		transcriptPassed &&
		costStatus == 0 &&
		heartbeatPassed
	c.state.InventoryExit = intPointer(inventoryStatus)
	c.state.CostExit = intPointer(costStatus)
	c.state.CleanupFinishedAt = utcText(finished)
	c.state.CleanupOverdue = overdue
	c.state.CleanupVerified = boolPointer(cleanupPassed)
	if cleanupPassed {
		switch {
		case overdue:
			c.state.Result = "cleanup_overdue"
		case applyStatus != 0:
			c.state.Result = "apply_failed_cleanup_complete"
		case !evidencePassed:
			c.state.Result = "cleanup_complete_with_errors"
			c.state.Error = "cleanup was verified, but one or more cleanup or evidence commands failed"
		default:
			c.state.Result = "passed"
		}
		if err := c.updateState("complete"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if applyStatus != 0 || overdue || !evidencePassed {
			return 1
		}
		return 0
	}
	c.state.Result = "cleanup_failed"
	if err := c.updateState("cleanup_failed"); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	return 1
}

func acquireControllerLock(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
}

func (c *controller) run(signalChannel <-chan os.Signal) (exitCode int) {
	raw, err := requirePreparedRun(c.root, c.runID, c.commit)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	c.raw = raw
	if err := c.verifySource(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	lockPath := filepath.Join(c.raw, ".controller.lock")
	lock, err := acquireControllerLock(lockPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "another controller already owns this run")
		return 1
	}
	_, _ = fmt.Fprintf(lock, "%d\n", os.Getpid())
	_ = lock.Close()
	defer func() { _ = os.Remove(lockPath) }()

	c.state = controllerState{
		SchemaVersion: 1,
		RunID:         c.runID,
		Commit:        c.commit,
		Operator:      c.operator,
		Region:        c.region,
		ControllerPID: os.Getpid(),
		CreatedAt:     utcText(c.now()),
	}
	session, err := c.createSession()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := c.updateState("applying"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	_, destroyDeadline, _, err := validateSession(session, c.runID, c.commit)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	applyStatus := c.runApply(destroyDeadline, signalChannel)
	c.state.ApplyExit = intPointer(applyStatus)
	if applyStatus == 0 {
		if err := c.updateState("live"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			c.stopReason = "state_write_failed"
			return c.cleanup(session, applyStatus)
		}
		fmt.Printf("live AWS session active; run make aws-live-stop AWS_RUN_ID=%s when capture is complete\n", c.runID)
		fmt.Printf("destroy starts no later than %s\n", utcText(destroyDeadline))
		if err := c.waitForStop(destroyDeadline, signalChannel); err != nil {
			fmt.Fprintln(os.Stderr, err)
			c.stopReason = "controller_error"
		}
	} else if c.stopReason == "" {
		c.stopReason = "apply_failed"
	}
	return c.cleanup(session, applyStatus)
}

func processRunning(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func controllerStateRunning(state controllerState, now time.Time) bool {
	if state.Phase != "applying" && state.Phase != "live" &&
		state.Phase != "destroying" && state.Phase != "verifying" {
		return false
	}
	updated, err := parseUTC(state.UpdatedAt, "controller heartbeat")
	if err != nil {
		return false
	}
	age := now.UTC().Sub(updated)
	if age < -heartbeatFutureSkew || age > heartbeatMaxAge {
		return false
	}
	return processRunning(state.ControllerPID)
}

var signalProcess = func(pid int, value syscall.Signal) error {
	return syscall.Kill(pid, value)
}

func requestStop(root, runID, reason string, now time.Time) (string, error) {
	raw, err := rawDirectory(root, runID)
	if err != nil {
		return "", err
	}
	var state controllerState
	if err := readJSON(filepath.Join(raw, "controller-state.json"), &state, "controller state"); err != nil {
		return "", err
	}
	if state.SchemaVersion != 1 || state.RunID != runID || (state.Phase != "applying" && state.Phase != "live") {
		return "", fail("the run has no active apply or live phase")
	}
	if !controllerStateRunning(state, now) {
		return "", fail("the live-run controller has no fresh heartbeat; use the manual recovery path")
	}
	path := filepath.Join(raw, "stop-request.json")
	if _, err := os.Stat(path); err == nil {
		return "", fail("a stop request already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "operator_stop"
	}
	if err := writeJSONAtomic(path, stopRequest{
		SchemaVersion: 1,
		RunID:         runID,
		Reason:        reason,
		RequestedAt:   utcText(now),
	}); err != nil {
		return "", err
	}
	// During apply, wake the controller immediately so it can forward SIGTERM
	// to Terraform. During the live phase the controller polls this file once a
	// second; leaving it unsignalled preserves the operator's recorded reason.
	if state.Phase == "applying" {
		_ = signalProcess(state.ControllerPID, syscall.SIGTERM)
	}
	return path, nil
}

func currentStatus(root, runID string, now time.Time) (map[string]any, error) {
	raw, err := rawDirectory(root, runID)
	if err != nil {
		return nil, err
	}
	var state controllerState
	if err := readJSON(filepath.Join(raw, "controller-state.json"), &state, "controller state"); err != nil {
		return nil, err
	}
	if state.SchemaVersion != 1 || state.RunID != runID || !commitPattern.MatchString(state.Commit) {
		return nil, fail("controller state is not bound to this run")
	}
	var session sessionReceipt
	if err := readJSON(filepath.Join(raw, "00-session.json"), &session, "session receipt"); err != nil {
		return nil, err
	}
	_, destroy, hard, err := validateSession(session, runID, state.Commit)
	if err != nil {
		return nil, err
	}
	seconds := int64(destroy.Sub(now).Seconds())
	if seconds < 0 {
		seconds = 0
	}
	return map[string]any{
		"run_id":                runID,
		"phase":                 state.Phase,
		"result":                state.Result,
		"controller_running":    controllerStateRunning(state, now),
		"controller_updated_at": state.UpdatedAt,
		"destroy_deadline":      utcText(destroy),
		"hard_deadline":         utcText(hard),
		"seconds_until_destroy": seconds,
		"cleanup_verified":      state.CleanupVerified,
	}, nil
}

func repositoryRoot() (string, error) {
	current, err := os.Getwd()
	if err != nil {
		return "", err
	}
	command := exec.Command("git", "-C", current, "rev-parse", "--show-toplevel")
	value, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("locate repository root: %w", err)
	}
	root := strings.TrimSpace(string(value))
	if root == "" {
		return "", fail("git returned an empty repository root")
	}
	return filepath.Abs(root)
}

func runCommand(arguments []string) int {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	runID := flags.String("run-id", os.Getenv("AWS_RUN_ID"), "UTC run id")
	commit := flags.String("commit", os.Getenv("AWS_APPROVED_COMMIT"), "approved commit")
	operator := flags.String("operator", os.Getenv("M4_OPERATOR"), "cleanup owner")
	profile := flags.String("profile", os.Getenv("AWS_PROFILE_NAME"), "AWS profile")
	region := flags.String("region", envDefault("AWS_REAL_REGION", "us-east-1"), "AWS region")
	if err := flags.Parse(arguments); err != nil {
		return 2
	}
	root, err := repositoryRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	current, err := newController(root, *runID, *commit, *operator, *profile, *region)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	signalChannel := make(chan os.Signal, 2)
	signalNotify(signalChannel)
	defer signalStop(signalChannel)
	return current.run(signalChannel)
}

var signalNotify = func(channel chan<- os.Signal) {
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
}

var signalStop = func(channel chan<- os.Signal) {
	signal.Stop(channel)
}

func stopCommand(arguments []string) int {
	flags := flag.NewFlagSet("stop", flag.ContinueOnError)
	runID := flags.String("run-id", os.Getenv("AWS_RUN_ID"), "UTC run id")
	reason := flags.String("reason", "evidence_complete", "stop reason")
	if err := flags.Parse(arguments); err != nil {
		return 2
	}
	root, err := repositoryRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	path, err := requestStop(root, *runID, *reason, time.Now())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("stop requested: %s\n", path)
	return 0
}

func statusCommand(arguments []string) int {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	runID := flags.String("run-id", os.Getenv("AWS_RUN_ID"), "UTC run id")
	if err := flags.Parse(arguments); err != nil {
		return 2
	}
	root, err := repositoryRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	value, err := currentStatus(root, *runID, time.Now())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	encoded, _ := json.MarshalIndent(value, "", "  ")
	fmt.Println(string(encoded))
	return 0
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: m4-live-run run|status|stop [options]")
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var exitCode int
	switch os.Args[1] {
	case "run":
		exitCode = runCommand(os.Args[2:])
	case "status":
		exitCode = statusCommand(os.Args[2:])
	case "stop":
		exitCode = stopCommand(os.Args[2:])
	default:
		usage()
		exitCode = 2
	}
	os.Exit(exitCode)
}
