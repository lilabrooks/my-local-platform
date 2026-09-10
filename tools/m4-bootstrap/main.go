package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

const (
	heartbeatMaxAge        = 5 * time.Second
	operatorTimeout        = 5 * time.Minute
	jobActiveDeadline      = 210
	jobWaitTimeout         = 4 * time.Minute
	jobPollInterval        = 2 * time.Second
	rdsRootCertificatePath = "/etc/ssl/certs/rds-us-east-1-bundle.pem"
)

var (
	runIDPattern  = regexp.MustCompile(`^[0-9]{8}T[0-9]{6}Z$`)
	commitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
	imagePattern  = regexp.MustCompile(`^[0-9]{12}\.dkr\.ecr\.us-east-1\.amazonaws\.com/mlp-dev/relay@sha256:[0-9a-f]{64}$`)
	jobPattern    = regexp.MustCompile(`^job\.batch/relay-bootstrap-[a-z0-9-]+$`)
)

type commandError struct {
	arguments []string
	stderr    string
	err       error
}

func (e *commandError) Error() string {
	detail := strings.TrimSpace(e.stderr)
	if detail == "" {
		detail = e.err.Error()
	}
	return fmt.Sprintf("%s: %s", strings.Join(e.arguments, " "), detail)
}

func (e *commandError) Unwrap() error { return e.err }

type commandRunner interface {
	Run(context.Context, []byte, ...string) ([]byte, error)
}

type processRunner struct {
	directory string
}

func (r processRunner) Run(ctx context.Context, stdin []byte, arguments ...string) ([]byte, error) {
	if len(arguments) == 0 {
		return nil, errors.New("empty command")
	}
	command := exec.CommandContext(ctx, arguments[0], arguments[1:]...)
	command.Dir = r.directory
	command.Env = os.Environ()
	if stdin != nil {
		command.Stdin = bytes.NewReader(stdin)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return nil, &commandError{arguments: append([]string(nil), arguments...), stderr: stderr.String(), err: err}
	}
	return stdout.Bytes(), nil
}

type goPacket struct {
	SchemaVersion   int               `json:"schema_version"`
	RunID           string            `json:"run_id"`
	SourceCommit    string            `json:"source_commit"`
	Decision        string            `json:"decision"`
	ImageReferences map[string]string `json:"image_references"`
	Gate            struct {
		Passed bool `json:"passed"`
	} `json:"gate"`
}

type controllerState struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	Commit        string `json:"commit"`
	Region        string `json:"region"`
	ControllerPID int    `json:"controller_pid"`
	UpdatedAt     string `json:"updated_at"`
	Phase         string `json:"phase"`
	ApplyExit     *int   `json:"apply_exit"`
}

type sessionReceipt struct {
	SchemaVersion   int    `json:"schema_version"`
	RunID           string `json:"run_id"`
	Commit          string `json:"commit"`
	Region          string `json:"region"`
	DestroyDeadline string `json:"destroy_deadline"`
}

type accountReceipt struct {
	AWS struct {
		AccountID string `json:"account_id"`
	} `json:"aws"`
}

type outputValue struct {
	Value json.RawMessage `json:"value"`
}

type terraformOutputs map[string]outputValue

func (o terraformOutputs) text(name string) (string, error) {
	entry, found := o[name]
	if !found {
		return "", fmt.Errorf("terraform output %s is missing", name)
	}
	var value string
	if err := json.Unmarshal(entry.Value, &value); err != nil || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("terraform output %s is empty or invalid", name)
	}
	return value, nil
}

type secretResponse struct {
	SecretString *string `json:"SecretString"`
}

type rdsSecret struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type runApproval struct {
	packet          goPacket
	destroyDeadline time.Time
}

type jobStatus struct {
	Status struct {
		Conditions []struct {
			Type    string `json:"type"`
			Status  string `json:"status"`
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"conditions"`
	} `json:"status"`
}

type application struct {
	root           string
	runner         commandRunner
	random         io.Reader
	now            func() time.Time
	processRunning func(int) bool
	output         io.Writer
	after          func(time.Duration) <-chan time.Time
}

func readJSON(path string, destination any, description string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("%s: %w", description, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("%s must be a regular file", description)
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("%s: %w", description, err)
	}
	if err := json.Unmarshal(value, destination); err != nil {
		return fmt.Errorf("%s is invalid JSON: %w", description, err)
	}
	return nil
}

func (a *application) evidenceDirectory(runID string) (string, error) {
	if !runIDPattern.MatchString(runID) {
		return "", errors.New("AWS_RUN_ID must use UTC YYYYMMDDTHHMMSSZ")
	}
	raw := filepath.Join(a.root, ".evidence", "m4", runID)
	relative, err := filepath.Rel(a.root, raw)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("evidence directory escapes the repository")
	}
	current := a.root
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
			return "", fmt.Errorf("evidence path contains a symlink: %s", current)
		}
	}
	return raw, nil
}

func parseUTC(value, name string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil || parsed.Location() != time.UTC || parsed.Format(time.RFC3339) != value {
		return time.Time{}, fmt.Errorf("%s must use UTC YYYY-MM-DDTHH:MM:SSZ", name)
	}
	return parsed, nil
}

func (a *application) activeController(runID, commit, region string) (time.Time, error) {
	raw, err := a.evidenceDirectory(runID)
	if err != nil {
		return time.Time{}, err
	}
	var state controllerState
	if err := readJSON(filepath.Join(raw, "controller-state.json"), &state, "controller state"); err != nil {
		return time.Time{}, err
	}
	updated, err := parseUTC(state.UpdatedAt, "controller heartbeat")
	if err != nil {
		return time.Time{}, err
	}
	age := a.now().UTC().Sub(updated)
	if state.SchemaVersion != 1 || state.RunID != runID || state.Commit != commit || state.Region != region || state.Phase != "live" || state.ApplyExit == nil || *state.ApplyExit != 0 || age < -30*time.Second || age > heartbeatMaxAge || !a.processRunning(state.ControllerPID) {
		return time.Time{}, errors.New("live controller is not active after a successful apply for this run and commit")
	}
	var session sessionReceipt
	if err := readJSON(filepath.Join(raw, "00-session.json"), &session, "session receipt"); err != nil {
		return time.Time{}, err
	}
	deadline, err := parseUTC(session.DestroyDeadline, "destroy deadline")
	if err != nil {
		return time.Time{}, err
	}
	if session.SchemaVersion != 1 || session.RunID != runID || session.Commit != commit || session.Region != region || !a.now().UTC().Before(deadline) {
		return time.Time{}, errors.New("session receipt is not active for this run and commit")
	}
	return deadline, nil
}

func (a *application) validateRun(ctx context.Context, runID, commit, region string) (runApproval, error) {
	var approval runApproval
	if !commitPattern.MatchString(commit) {
		return approval, errors.New("AWS_APPROVED_COMMIT must be a full lowercase git SHA")
	}
	if region != "us-east-1" {
		return approval, errors.New("M4 uses the fixed us-east-1 region")
	}
	if os.Getenv("MLP_USE_REAL_AWS") != "1" {
		return approval, errors.New("MLP_USE_REAL_AWS=1 is required")
	}
	if os.Getenv("AWS_ENDPOINT_URL") != "" {
		return approval, errors.New("AWS_ENDPOINT_URL must be unset for live AWS")
	}
	raw, err := a.evidenceDirectory(runID)
	if err != nil {
		return approval, err
	}
	if err := readJSON(filepath.Join(raw, "06-go-no-go.json"), &approval.packet, "go/no-go packet"); err != nil {
		return approval, err
	}
	relayImage := approval.packet.ImageReferences["relay"]
	if approval.packet.SchemaVersion != 1 || approval.packet.RunID != runID || approval.packet.SourceCommit != commit || approval.packet.Decision != "go" || !approval.packet.Gate.Passed || !imagePattern.MatchString(relayImage) {
		return approval, errors.New("go/no-go packet does not approve this run, commit, and relay image")
	}
	var identity accountReceipt
	if err := readJSON(filepath.Join(raw, "01-identity.txt"), &identity, "account receipt"); err != nil {
		return approval, err
	}
	imageAccount := relayImage[:12]
	if identity.AWS.AccountID != imageAccount {
		return approval, errors.New("account receipt does not match the approved relay image account")
	}
	currentAccount, err := a.runner.Run(ctx, nil, "aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text")
	if err != nil {
		return approval, err
	}
	if strings.TrimSpace(string(currentAccount)) != identity.AWS.AccountID {
		return approval, errors.New("current AWS identity does not match the approved account receipt")
	}
	approval.destroyDeadline, err = a.activeController(runID, commit, region)
	if err != nil {
		return approval, err
	}
	head, err := a.runner.Run(ctx, nil, "git", "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(string(head)) != commit {
		return approval, errors.New("worktree HEAD does not match the approved commit")
	}
	status, err := a.runner.Run(ctx, nil, "git", "status", "--porcelain")
	if err != nil {
		return approval, err
	}
	if len(bytes.TrimSpace(status)) != 0 {
		return approval, errors.New("worktree must be clean before live bootstrap")
	}
	return approval, nil
}

func (a *application) terraformOutputs(ctx context.Context) (terraformOutputs, error) {
	value, err := a.runner.Run(ctx, nil, "terraform", "-chdir=infra/terraform/envs/dev", "output", "-json")
	if err != nil {
		return nil, err
	}
	var outputs terraformOutputs
	if err := json.Unmarshal(value, &outputs); err != nil {
		return nil, fmt.Errorf("terraform outputs are invalid JSON: %w", err)
	}
	return outputs, nil
}

func parseSecret(value []byte, description string) (string, error) {
	var response secretResponse
	if err := json.Unmarshal(value, &response); err != nil || response.SecretString == nil || *response.SecretString == "" {
		return "", fmt.Errorf("%s has no SecretString", description)
	}
	return *response.SecretString, nil
}

func missingSecret(err error) bool {
	var commandFailure *commandError
	return errors.As(err, &commandFailure) && strings.Contains(commandFailure.stderr, "ResourceNotFoundException")
}

func (a *application) getSecret(ctx context.Context, arn string) (string, error) {
	value, err := a.runner.Run(ctx, nil, "aws", "secretsmanager", "get-secret-value", "--secret-id", arn, "--output", "json")
	if err != nil {
		return "", err
	}
	return parseSecret(value, "Secrets Manager response")
}

func (a *application) signingSecret(ctx context.Context, arn string, beforeStore func() error) (string, error) {
	existing, err := a.getSecret(ctx, arn)
	if err == nil {
		decoded, decodeErr := base64.RawURLEncoding.DecodeString(existing)
		if decodeErr != nil || len(decoded) != 32 {
			return "", errors.New("existing sink signing secret is not a 256-bit base64url value")
		}
		return existing, nil
	}
	if !missingSecret(err) {
		return "", fmt.Errorf("read sink signing secret: %w", err)
	}
	randomBytes := make([]byte, 32)
	if _, err := io.ReadFull(a.random, randomBytes); err != nil {
		return "", fmt.Errorf("generate sink signing secret: %w", err)
	}
	generated := base64.RawURLEncoding.EncodeToString(randomBytes)
	if beforeStore != nil {
		if err := beforeStore(); err != nil {
			return "", fmt.Errorf("controller stopped before storing sink signing secret: %w", err)
		}
	}
	if _, err := a.runner.Run(ctx, []byte(generated), "aws", "secretsmanager", "put-secret-value", "--secret-id", arn, "--secret-string", "file:///dev/stdin", "--output", "json"); err != nil {
		return "", fmt.Errorf("store sink signing secret: %w", err)
	}
	stored, err := a.getSecret(ctx, arn)
	if err != nil {
		return "", fmt.Errorf("read stored sink signing secret: %w", err)
	}
	if stored != generated {
		return "", errors.New("stored sink signing secret does not match the generated value")
	}
	return stored, nil
}

func databaseURL(endpoint, secretText string) (string, error) {
	var credential rdsSecret
	if err := json.Unmarshal([]byte(secretText), &credential); err != nil || credential.Username == "" || credential.Password == "" {
		return "", errors.New("RDS master secret must contain username and password")
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || port != "5432" || !strings.HasSuffix(host, ".rds.amazonaws.com") {
		return "", errors.New("RDS endpoint must be an AWS RDS hostname on port 5432")
	}
	value := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(credential.Username, credential.Password),
		Host:     endpoint,
		Path:     "/platform",
		RawQuery: "sslmode=verify-full&sslrootcert=" + url.QueryEscape(rdsRootCertificatePath),
	}
	return value.String(), nil
}

func encodedPassword(password string) string {
	userinfo := url.UserPassword("user", password).String()
	_, encoded, _ := strings.Cut(userinfo, ":")
	return encoded
}

func secretManifest(database, signing string) ([]byte, error) {
	document := map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      "relay-secrets",
			"namespace": "mlp",
			"labels": map[string]string{
				"app.kubernetes.io/part-of": "my-local-platform",
			},
		},
		"type": "Opaque",
		"stringData": map[string]string{
			"DATABASE_URL":         database,
			"RELAY_SIGNING_SECRET": signing,
		},
	}
	return json.Marshal(document)
}

func redacted(value []byte, sensitive ...string) []byte {
	text := string(value)
	for _, secret := range sensitive {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "[REDACTED]")
		}
	}
	return []byte(text)
}

func jobManifest(runID, commit, image string) ([]byte, error) {
	document := map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]any{
			"generateName": "relay-bootstrap-",
			"namespace":    "mlp",
			"annotations": map[string]string{
				"mlp.dev/run-id":        runID,
				"mlp.dev/source-commit": commit,
			},
			"labels": map[string]string{
				"app.kubernetes.io/name":    "relay-bootstrap",
				"app.kubernetes.io/part-of": "my-local-platform",
			},
		},
		"spec": map[string]any{
			"backoffLimit":          0,
			"activeDeadlineSeconds": jobActiveDeadline,
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]string{
						"app.kubernetes.io/name":    "relay-bootstrap",
						"app.kubernetes.io/part-of": "my-local-platform",
					},
				},
				"spec": map[string]any{
					"serviceAccountName": "relay-bootstrap",
					"restartPolicy":      "Never",
					"securityContext": map[string]any{
						"runAsNonRoot": true,
						"runAsUser":    65532,
						"seccompProfile": map[string]string{
							"type": "RuntimeDefault",
						},
					},
					"containers": []any{map[string]any{
						"name":            "bootstrap",
						"image":           image,
						"imagePullPolicy": "IfNotPresent",
						"command":         []string{"/relay-bootstrap"},
						"envFrom": []any{
							map[string]any{"configMapRef": map[string]string{"name": "relay-runtime"}},
							map[string]any{"secretRef": map[string]string{"name": "relay-secrets"}},
						},
						"securityContext": map[string]any{
							"allowPrivilegeEscalation": false,
							"readOnlyRootFilesystem":   true,
							"capabilities": map[string]any{
								"drop": []string{"ALL"},
							},
						},
						"resources": map[string]any{
							"requests": map[string]string{"cpu": "10m", "memory": "32Mi"},
							"limits":   map[string]string{"memory": "128Mi"},
						},
					}},
				},
			},
		},
	}
	return json.Marshal(document)
}

func currentClusterServer(value []byte) (string, error) {
	var config struct {
		Clusters []struct {
			Cluster struct {
				Server string `json:"server"`
			} `json:"cluster"`
		} `json:"clusters"`
	}
	if err := json.Unmarshal(value, &config); err != nil || len(config.Clusters) != 1 || config.Clusters[0].Cluster.Server == "" {
		return "", errors.New("current kubectl context has no single cluster server")
	}
	return config.Clusters[0].Cluster.Server, nil
}

func (a *application) verifyCluster(ctx context.Context, clusterName string) error {
	expected, err := a.runner.Run(ctx, nil, "aws", "eks", "describe-cluster", "--name", clusterName, "--query", "cluster.endpoint", "--output", "text")
	if err != nil {
		return err
	}
	config, err := a.runner.Run(ctx, nil, "kubectl", "config", "view", "--minify", "-o", "json")
	if err != nil {
		return err
	}
	actual, err := currentClusterServer(config)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(expected)) != actual {
		return errors.New("current kubectl context does not target the Terraform EKS cluster")
	}
	return nil
}

func runtimeBootstrapValue(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, errors.New("rendered relay runtime must be a regular file")
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var document struct {
		Kind string            `json:"kind"`
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(value, &document); err != nil || document.Kind != "ConfigMap" {
		return nil, errors.New("rendered relay runtime is not a ConfigMap")
	}
	if document.Data["KAFKA_AUTH_MODE"] != "aws_msk_iam" || document.Data["AWS_REGION"] != "us-east-1" {
		return nil, errors.New("rendered relay runtime does not require us-east-1 MSK IAM")
	}
	return value, nil
}

func (a *application) afterDelay(delay time.Duration) <-chan time.Time {
	if a.after != nil {
		return a.after(delay)
	}
	return time.After(delay)
}

func (a *application) waitForJob(ctx context.Context, jobName, runID, commit, region string) error {
	for {
		if _, err := a.activeController(runID, commit, region); err != nil {
			return fmt.Errorf("live controller stopped during runtime bootstrap: %w", err)
		}
		value, err := a.runner.Run(ctx, nil, "kubectl", "-n", "mlp", "get", jobName, "-o", "json")
		if err != nil {
			return fmt.Errorf("read relay bootstrap Job status: %w", err)
		}
		var status jobStatus
		if err := json.Unmarshal(value, &status); err != nil {
			return fmt.Errorf("relay bootstrap Job status is invalid JSON: %w", err)
		}
		for _, condition := range status.Status.Conditions {
			if condition.Status != "True" {
				continue
			}
			switch condition.Type {
			case "Complete":
				return nil
			case "Failed":
				detail := strings.TrimSpace(strings.Join([]string{condition.Reason, condition.Message}, ": "))
				return fmt.Errorf("relay bootstrap Job failed: %s", detail)
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for relay bootstrap Job: %w", ctx.Err())
		case <-a.afterDelay(jobPollInterval):
		}
	}
}

func (a *application) run(ctx context.Context, runID, commit, region string) error {
	approval, err := a.validateRun(ctx, runID, commit, region)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(ctx, approval.destroyDeadline)
	defer cancel()
	packet := approval.packet
	controllerActive := func() error {
		_, activeErr := a.activeController(runID, commit, region)
		return activeErr
	}
	outputs, err := a.terraformOutputs(ctx)
	if err != nil {
		return err
	}
	cluster, err := outputs.text("eks_cluster_name")
	if err != nil {
		return err
	}
	msk, err := outputs.text("msk_bootstrap_brokers")
	if err != nil {
		return err
	}
	rdsEndpoint, err := outputs.text("rds_endpoint")
	if err != nil {
		return err
	}
	rdsSecretARN, err := outputs.text("rds_master_secret_arn")
	if err != nil {
		return err
	}
	signingSecretARN, err := outputs.text("sink_signing_secret_arn")
	if err != nil {
		return err
	}
	account := packet.ImageReferences["relay"][:12]
	secretPrefix := "arn:aws:secretsmanager:" + region + ":" + account + ":secret:"
	if !strings.HasPrefix(rdsSecretARN, secretPrefix) || !strings.HasPrefix(signingSecretARN, secretPrefix) {
		return errors.New("terraform secret ARNs do not match the approved region and account")
	}
	if err := a.verifyCluster(ctx, cluster); err != nil {
		return err
	}
	runtimePath := filepath.Join(a.root, ".evidence", "m4", runID, "rendered", "relay-runtime.json")
	runtime, err := runtimeBootstrapValue(runtimePath)
	if err != nil {
		return fmt.Errorf("read rendered relay runtime: %w", err)
	}
	var runtimeDocument struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(runtime, &runtimeDocument); err != nil || runtimeDocument.Data["KAFKA_BOOTSTRAP"] != msk {
		return errors.New("rendered relay runtime does not match the Terraform MSK brokers")
	}

	if err := controllerActive(); err != nil {
		return err
	}
	signing, err := a.signingSecret(ctx, signingSecretARN, controllerActive)
	if err != nil {
		return err
	}
	rdsValue, err := a.getSecret(ctx, rdsSecretARN)
	if err != nil {
		return fmt.Errorf("read RDS master secret: %w", err)
	}
	database, err := databaseURL(rdsEndpoint, rdsValue)
	if err != nil {
		return err
	}
	var databaseCredential rdsSecret
	if err := json.Unmarshal([]byte(rdsValue), &databaseCredential); err != nil {
		return errors.New("RDS master secret changed after validation")
	}
	secret, err := secretManifest(database, signing)
	if err != nil {
		return err
	}
	job, err := jobManifest(runID, commit, packet.ImageReferences["relay"])
	if err != nil {
		return err
	}

	for _, arguments := range [][]string{
		{"kubectl", "apply", "-f", "k8s/manifests/namespace.yaml"},
		{"kubectl", "-n", "mlp", "apply", "-f", "k8s/aws/relay/serviceaccounts.yaml"},
	} {
		if err := controllerActive(); err != nil {
			return err
		}
		if _, err := a.runner.Run(ctx, nil, arguments...); err != nil {
			return err
		}
	}
	if err := controllerActive(); err != nil {
		return err
	}
	if _, err := a.runner.Run(ctx, runtime, "kubectl", "apply", "--server-side", "--field-manager=mlp-bootstrap", "-f", "-"); err != nil {
		return fmt.Errorf("apply relay runtime ConfigMap: %w", err)
	}
	if err := controllerActive(); err != nil {
		return err
	}
	if _, err := a.runner.Run(ctx, secret, "kubectl", "apply", "--server-side", "--field-manager=mlp-bootstrap", "-f", "-"); err != nil {
		return fmt.Errorf("apply relay runtime Secret: %w", err)
	}
	if err := controllerActive(); err != nil {
		return err
	}
	created, err := a.runner.Run(ctx, job, "kubectl", "create", "-f", "-", "-o", "name")
	if err != nil {
		return fmt.Errorf("create relay bootstrap Job: %w", err)
	}
	jobName := strings.TrimSpace(string(created))
	if !jobPattern.MatchString(jobName) {
		return fmt.Errorf("kubectl returned invalid bootstrap Job name %q", jobName)
	}
	jobContext, stopWaiting := context.WithTimeout(ctx, jobWaitTimeout)
	waitErr := a.waitForJob(jobContext, jobName, runID, commit, region)
	stopWaiting()
	if waitErr != nil {
		logContext, stopLogs := context.WithTimeout(ctx, 10*time.Second)
		logs, _ := a.runner.Run(logContext, nil, "kubectl", "-n", "mlp", "logs", jobName)
		stopLogs()
		if len(logs) != 0 {
			_, _ = a.output.Write(redacted(logs, signing, database, databaseCredential.Password, encodedPassword(databaseCredential.Password)))
		}
		return waitErr
	}
	logs, err := a.runner.Run(ctx, nil, "kubectl", "-n", "mlp", "logs", jobName)
	if err != nil {
		return fmt.Errorf("read relay bootstrap Job logs: %w", err)
	}
	if len(logs) != 0 {
		_, _ = a.output.Write(redacted(logs, signing, database, databaseCredential.Password, encodedPassword(databaseCredential.Password)))
	}
	if _, err := fmt.Fprintf(a.output, "live runtime bootstrap complete: %s\n", jobName); err != nil {
		return fmt.Errorf("write bootstrap completion: %w", err)
	}
	return nil
}

func isProcessRunning(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func main() {
	runID := flag.String("run-id", "", "approved M4 run id")
	commit := flag.String("commit", "", "approved source commit")
	region := flag.String("region", "us-east-1", "fixed M4 AWS region")
	root := flag.String("root", "../..", "repository root")
	flag.Parse()
	absoluteRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	app := &application{
		root:           absoluteRoot,
		runner:         processRunner{directory: absoluteRoot},
		random:         rand.Reader,
		now:            time.Now,
		processRunning: isProcessRunning,
		output:         os.Stdout,
	}
	rootContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(rootContext, operatorTimeout)
	defer cancel()
	if err := app.run(ctx, *runID, *commit, *region); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
