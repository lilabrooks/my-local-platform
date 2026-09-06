package validate

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

const (
	fixtureCommit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	fixtureRelay  = "123456789012.dkr.ecr.us-east-1.amazonaws.com/mlp-dev/relay@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	fixtureSink   = "123456789012.dkr.ecr.us-east-1.amazonaws.com/mlp-dev/sink@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	fixtureMSK    = "boot-example.c1.kafka-serverless.us-east-1.amazonaws.com:9098"
)

func awsDocs(t *testing.T, component string) []map[string]any {
	t.Helper()
	return render(t, filepath.Join("../aws", component))
}

func awsComponents(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir("../aws")
	if err != nil {
		t.Fatal(err)
	}
	var components []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join("../aws", entry.Name(), "kustomization.yaml")); err == nil {
			components = append(components, entry.Name())
		}
	}
	if len(components) == 0 {
		t.Fatal("no AWS Kustomize components discovered")
	}
	return components
}

func findNamed(t *testing.T, docs []map[string]any, kind, objectName string) map[string]any {
	t.Helper()
	for _, doc := range kindsOf(docs, kind) {
		if name(doc) == objectName {
			return doc
		}
	}
	t.Fatalf("no %s %s", kind, objectName)
	return nil
}

func list(value any) []any {
	out, _ := value.([]any)
	return out
}

func object(value any) map[string]any {
	out, _ := value.(map[string]any)
	return out
}

func firstContainer(t *testing.T, workload map[string]any) map[string]any {
	t.Helper()
	spec := nested(workload, "spec", "template", "spec")
	containers := list(spec["containers"])
	if len(containers) != 1 {
		t.Fatalf("%s has %d containers, want 1", name(workload), len(containers))
	}
	return object(containers[0])
}

func TestAWSWorkloadsKeepIdentityAndExposureBoundaries(t *testing.T) {
	components := awsComponents(t)
	wantAccounts := map[string]string{
		"relay-ingest":  "relay-ingest",
		"relay-deliver": "relay-deliver",
		"sink":          "sink",
	}

	for _, component := range components {
		docs := awsDocs(t, component)
		if len(kindsOf(docs, "Ingress")) != 0 {
			t.Errorf("%s renders an Ingress; AWS workloads must stay private", component)
		}
		for _, service := range kindsOf(docs, "Service") {
			if got := nested(service, "spec")["type"]; got != "ClusterIP" {
				t.Errorf("%s/%s Service type = %v, want ClusterIP", component, name(service), got)
			}
		}
		for _, deployment := range kindsOf(docs, "Deployment") {
			want, constrained := wantAccounts[name(deployment)]
			if !constrained {
				continue
			}
			got := nested(deployment, "spec", "template", "spec")["serviceAccountName"]
			if got != want {
				t.Errorf("%s serviceAccountName = %v, want %s", name(deployment), got, want)
			}
		}
	}

	relay := awsDocs(t, "relay")
	ingest := findNamed(t, relay, "Deployment", "relay-ingest")
	if nested(ingest, "spec")["replicas"] != float64(2) {
		t.Errorf("relay-ingest replicas = %v, want 2", nested(ingest, "spec")["replicas"])
	}
	deliver := findNamed(t, relay, "Deployment", "relay-deliver")
	if _, found := nested(deliver, "spec")["replicas"]; found {
		t.Error("relay-deliver declares replicas; KEDA must be the only replica authority")
	}

	for _, deployment := range []*map[string]any{&ingest, &deliver} {
		container := firstContainer(t, *deployment)
		if got := container["image"]; got != "example.invalid/mlp-dev/relay@sha256:"+strings.Repeat("0", 64) {
			t.Errorf("%s fixture image = %v, want immutable relay digest", name(*deployment), got)
		}
		refs := list(container["envFrom"])
		if len(refs) != 2 || object(object(refs[0])["configMapRef"])["name"] != "relay-runtime" ||
			object(object(refs[1])["secretRef"])["name"] != "relay-secrets" {
			t.Errorf("%s envFrom = %v, want relay-runtime then relay-secrets", name(*deployment), refs)
		}
	}
	if len(kindsOf(relay, "ConfigMap")) != 0 || len(kindsOf(relay, "Secret")) != 0 {
		t.Error("AWS relay render owns runtime values; staging must own relay-runtime and relay-secrets")
	}

	sink := findNamed(t, awsDocs(t, "sink"), "Deployment", "sink")
	if got := firstContainer(t, sink)["image"]; got != "example.invalid/mlp-dev/sink@sha256:"+strings.Repeat("0", 64) {
		t.Errorf("sink fixture image = %v, want immutable sink digest", got)
	}
}

func TestAWSKEDAUsesOperatorPodIdentityAndRelayGroup(t *testing.T) {
	docs := awsDocs(t, "relay")
	auth := findNamed(t, docs, "TriggerAuthentication", "relay-msk")
	podIdentity := nested(auth, "spec", "podIdentity")
	if podIdentity["provider"] != "aws" || podIdentity["identityOwner"] != "keda" {
		t.Errorf("KEDA pod identity = %v, want provider aws owned by keda", podIdentity)
	}
	if len(podIdentity) != 2 {
		t.Errorf("KEDA pod identity contains static or role credentials: %v", podIdentity)
	}

	scaled := findNamed(t, docs, "ScaledObject", "relay-deliver")
	spec := nested(scaled, "spec")
	if spec["minReplicaCount"] != float64(1) || spec["maxReplicaCount"] != float64(12) {
		t.Errorf("AWS relay scale range = %v..%v, want 1..12", spec["minReplicaCount"], spec["maxReplicaCount"])
	}
	trigger := object(list(spec["triggers"])[0])
	meta := object(trigger["metadata"])
	want := map[string]any{
		"bootstrapServers": "__MSK_BOOTSTRAP_BROKERS__",
		"consumerGroup":    "relay-deliver",
		"topic":            "mlp.relay.deliveries",
		"awsRegion":        "us-east-1",
		"sasl":             "aws_msk_iam",
		"tls":              "enable",
	}
	for key, value := range want {
		if meta[key] != value {
			t.Errorf("KEDA %s = %v, want %v", key, meta[key], value)
		}
	}
	runtime := readYAML[map[string]any](t, "../aws/runtime-configmap.example.yaml")
	if meta["consumerGroup"] != nested(runtime, "data")["RELAY_CONSUMER_GROUP"] {
		t.Errorf("KEDA group %v differs from relay metrics group %v",
			meta["consumerGroup"], nested(runtime, "data")["RELAY_CONSUMER_GROUP"])
	}
	if object(trigger["authenticationRef"])["name"] != "relay-msk" {
		t.Errorf("KEDA authenticationRef = %v, want relay-msk", trigger["authenticationRef"])
	}
}

func runRenderer(t *testing.T, args ...string) map[string]any {
	t.Helper()
	cmd := exec.Command("python3", append([]string{"../../scripts/render-aws-k8s.py"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("render AWS Kubernetes input: %v\n%s", err, out)
	}
	var doc map[string]any
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("parse generated JSON: %v\n%s", err, out)
	}
	return doc
}

func TestGeneratedAWSRootPinsRevisionImagesAndMSK(t *testing.T) {
	doc := runRenderer(t,
		"application",
		"--commit", fixtureCommit,
		"--relay-image", fixtureRelay,
		"--sink-image", fixtureSink,
		"--msk-bootstrap", fixtureMSK,
	)
	if doc["kind"] != "Application" || nested(doc, "spec")["project"] != "mlp-root" {
		t.Fatalf("generated root header/project = %v/%v", doc["kind"], nested(doc, "spec")["project"])
	}
	source := nested(doc, "spec", "source")
	if source["targetRevision"] != fixtureCommit || source["path"] != "k8s/apps/aws" {
		t.Errorf("generated root source = %v", source)
	}
	annotations := nested(doc, "metadata", "annotations")
	if annotations["mlp.dev/runtime-config-map"] != "relay-runtime" ||
		annotations["mlp.dev/runtime-secret"] != "relay-secrets" {
		t.Errorf("generated runtime object contract = %v", annotations)
	}

	encoded, _ := json.Marshal(source["kustomize"])
	for _, value := range []string{fixtureCommit, fixtureRelay, fixtureSink, fixtureMSK} {
		if !bytes.Contains(encoded, []byte(value)) {
			t.Errorf("generated root Kustomize overrides omit %s", value)
		}
	}

	children := renderWithOverrides(t, "../apps/aws", object(source["kustomize"]))
	for _, child := range children {
		childSource := nested(child, "spec", "source")
		if childSource["targetRevision"] != fixtureCommit {
			t.Errorf("generated child %s revision = %v, want %s", name(child), childSource["targetRevision"], fixtureCommit)
		}
	}

	relayApp := findNamed(t, children, "Application", "relay")
	relaySource := nested(relayApp, "spec", "source", "kustomize")
	relayWorkloads := renderWithOverrides(t, "../aws/relay", relaySource)
	for _, deploymentName := range []string{"relay-ingest", "relay-deliver"} {
		deployment := findNamed(t, relayWorkloads, "Deployment", deploymentName)
		if got := firstContainer(t, deployment)["image"]; got != fixtureRelay {
			t.Errorf("final %s image = %v, want %s", deploymentName, got, fixtureRelay)
		}
	}
	scaled := findNamed(t, relayWorkloads, "ScaledObject", "relay-deliver")
	trigger := object(list(nested(scaled, "spec")["triggers"])[0])
	if got := object(trigger["metadata"])["bootstrapServers"]; got != fixtureMSK {
		t.Errorf("final KEDA bootstrapServers = %v, want %s", got, fixtureMSK)
	}

	sinkApp := findNamed(t, children, "Application", "sink")
	sinkSource := nested(sinkApp, "spec", "source", "kustomize")
	sinkWorkloads := renderWithOverrides(t, "../aws/sink", sinkSource)
	if got := firstContainer(t, findNamed(t, sinkWorkloads, "Deployment", "sink"))["image"]; got != fixtureSink {
		t.Errorf("final sink image = %v, want %s", got, fixtureSink)
	}
}

func renderWithOverrides(t *testing.T, resourcePath string, overrides map[string]any) []map[string]any {
	t.Helper()
	absResource, err := filepath.Abs(resourcePath)
	if err != nil {
		t.Fatal(err)
	}
	temp := t.TempDir()
	temp, err = filepath.EvalSymlinks(temp)
	if err != nil {
		t.Fatal(err)
	}
	relResource, err := filepath.Rel(temp, absResource)
	if err != nil {
		t.Fatal(err)
	}
	doc := map[string]any{
		"apiVersion": "kustomize.config.k8s.io/v1beta1",
		"kind":       "Kustomization",
		"resources":  []string{relResource},
	}
	for _, key := range []string{"images", "patches"} {
		if value, ok := overrides[key]; ok {
			if key == "images" {
				doc[key] = nativeKustomizeImages(t, list(value))
			} else {
				doc[key] = value
			}
		}
	}
	body, err := yaml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(temp, "kustomization.yaml"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("kubectl", "kustomize", temp)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("render generated Kustomize overrides: %v\n%s\n%s", err, body, out)
	}
	var docs []map[string]any
	for _, chunk := range strings.Split(string(out), "\n---") {
		if strings.TrimSpace(chunk) == "" {
			continue
		}
		var rendered map[string]any
		if err := yaml.Unmarshal([]byte(chunk), &rendered); err != nil {
			t.Fatalf("parse rendered generated override: %v", err)
		}
		docs = append(docs, rendered)
	}
	return docs
}

// ArgoCD's Application API represents image overrides as old=new@digest
// strings. Native kustomization.yaml represents the same input as objects.
func nativeKustomizeImages(t *testing.T, values []any) []map[string]any {
	t.Helper()
	out := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		value, ok := raw.(string)
		if !ok {
			t.Fatalf("Application image override is %T, want string", raw)
		}
		nameAndImage := strings.SplitN(value, "=", 2)
		if len(nameAndImage) != 2 {
			t.Fatalf("Application image override %q has no replacement", value)
		}
		imageAndDigest := strings.SplitN(nameAndImage[1], "@", 2)
		if len(imageAndDigest) != 2 {
			t.Fatalf("Application image override %q is not digest-selected", value)
		}
		out = append(out, map[string]any{
			"name":    nameAndImage[0],
			"newName": imageAndDigest[0],
			"digest":  imageAndDigest[1],
		})
	}
	return out
}

func TestGeneratedAWSRuntimeMatchesTheTrackedContract(t *testing.T) {
	doc := runRenderer(t, "runtime", "--msk-bootstrap", fixtureMSK)
	if doc["kind"] != "ConfigMap" || nested(doc, "metadata")["name"] != "relay-runtime" {
		t.Fatalf("generated runtime header = %v/%v", doc["kind"], nested(doc, "metadata"))
	}
	data := nested(doc, "data")
	tracked := readYAML[map[string]any](t, "../aws/runtime-configmap.example.yaml")
	want := nested(tracked, "data")
	want["KAFKA_BOOTSTRAP"] = fixtureMSK
	if !reflect.DeepEqual(data, want) {
		t.Errorf("generated runtime data = %v, want full tracked contract %v", data, want)
	}
}

func TestAWSChildApplicationsStayInsideWorkloadProject(t *testing.T) {
	docs := render(t, "../apps/aws")
	if len(docs) != 4 {
		t.Fatalf("AWS app-of-apps renders %d children, want 4", len(docs))
	}
	wantPaths := map[string]string{
		"relay":      "k8s/aws/relay",
		"sink":       "k8s/aws/sink",
		"monitoring": "k8s/manifests/monitoring",
		"telemetry":  "k8s/aws/telemetry",
	}
	for _, app := range docs {
		if app["kind"] != "Application" {
			t.Fatalf("AWS child kind = %v, want Application", app["kind"])
		}
		spec := nested(app, "spec")
		if spec["project"] != "mlp" {
			t.Errorf("AWS child %s project = %v, want mlp", name(app), spec["project"])
		}
		source := object(spec["source"])
		if source["targetRevision"] != "__GIT_COMMIT__" {
			t.Errorf("AWS child %s bypasses generated commit replacement: %v", name(app), source)
		}
		if source["path"] != wantPaths[name(app)] {
			t.Errorf("AWS child %s path = %v, want %s", name(app), source["path"], wantPaths[name(app)])
		}
		destination := object(spec["destination"])
		if destination["server"] != "https://kubernetes.default.svc" || destination["namespace"] != "mlp" {
			t.Errorf("AWS child %s destination = %v", name(app), destination)
		}
	}
}

func TestGeneratedAWSReplayUsesDeliverIdentityAndNoSecret(t *testing.T) {
	doc := runRenderer(t,
		"replay",
		"--commit", fixtureCommit,
		"--relay-image", fixtureRelay,
		"--since", "earliest",
	)
	if doc["kind"] != "Job" || nested(doc, "metadata")["namespace"] != "mlp" {
		t.Fatalf("generated replay header = %v/%v", doc["kind"], nested(doc, "metadata"))
	}
	pod := nested(doc, "spec", "template", "spec")
	if pod["serviceAccountName"] != "relay-deliver" {
		t.Errorf("replay serviceAccountName = %v, want relay-deliver", pod["serviceAccountName"])
	}
	container := firstContainer(t, doc)
	if container["image"] != fixtureRelay || strings.Join(stringsFrom(list(container["command"])), " ") != "/relay-replay" {
		t.Errorf("replay image/command = %v/%v", container["image"], container["command"])
	}
	encoded, _ := json.Marshal(container["envFrom"])
	if bytes.Contains(encoded, []byte("secretRef")) || !bytes.Contains(encoded, []byte("relay-runtime")) {
		t.Errorf("replay envFrom = %s; it needs runtime config and no application secret", encoded)
	}
}

func stringsFrom(values []any) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if text, ok := value.(string); ok {
			out = append(out, text)
		}
	}
	return out
}

func TestAWSTrackedFilesContainNoAccountSpecificEndpoint(t *testing.T) {
	account := regexp.MustCompile(`\b[0-9]{12}\b`)
	endpoint := regexp.MustCompile(`\.dkr\.ecr\.|\.kafka-serverless\.`)
	err := filepath.Walk("../aws", func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if account.Match(body) || endpoint.Match(body) {
			t.Errorf("%s contains an account-specific id or endpoint", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAWSRuntimeObjectsCarryTheRelayContract(t *testing.T) {
	runtime := readYAML[map[string]any](t, "../aws/runtime-configmap.example.yaml")
	data := nested(runtime, "data")
	want := map[string]any{
		"KAFKA_AUTH_MODE":             "aws_msk_iam",
		"AWS_REGION":                  "us-east-1",
		"DEPLOYMENT_ENVIRONMENT":      "aws",
		"RELAY_TOPIC":                 "mlp.relay.deliveries",
		"RELAY_DLQ_TOPIC":             "mlp.relay.deliveries.dlq",
		"RELAY_CONSUMER_GROUP":        "relay-deliver",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector:4317",
	}
	for key, value := range want {
		if data[key] != value {
			t.Errorf("AWS runtime %s = %v, want %v", key, data[key], value)
		}
	}
	secret := readYAML[map[string]any](t, "../aws/runtime-secret.example.yaml")
	secretData := nested(secret, "stringData")
	for _, key := range []string{"DATABASE_URL", "RELAY_SIGNING_SECRET"} {
		if _, ok := secretData[key]; !ok {
			t.Errorf("AWS secret contract omits %s", key)
		}
	}
}

func TestAWSPodIdentityNamesMatchTerraform(t *testing.T) {
	body, err := os.ReadFile("../../infra/terraform/envs/dev/identities.tf")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, block := range []string{
		"relay-ingest = {\n      namespace       = \"mlp\"\n      service_account = \"relay-ingest\"",
		"relay-deliver = {\n      namespace       = \"mlp\"\n      service_account = \"relay-deliver\"",
		"keda-operator = {\n      namespace       = \"keda\"\n      service_account = \"keda-operator\"",
	} {
		if !strings.Contains(text, block) {
			t.Errorf("Terraform Pod Identity map omits:\n%s", block)
		}
	}
	podIdentityMap := strings.SplitN(text, "  } : {}", 2)[0]
	if strings.Contains(podIdentityMap, "sink = {") {
		t.Error("Terraform gives the sink a Pod Identity role")
	}
}

func TestAWSTelemetryFeedsTempoAndGrafana(t *testing.T) {
	runtime := readYAML[map[string]any](t, "../aws/runtime-configmap.example.yaml")
	if nested(runtime, "data")["OTEL_EXPORTER_OTLP_ENDPOINT"] != "http://otel-collector:4317" {
		t.Error("relay runtime does not send traces to the in-cluster collector")
	}
	collector := findNamed(t, awsDocs(t, "telemetry"), "ConfigMap", "otel-collector")
	config, _ := nested(collector, "data")["config.yaml"].(string)
	if !strings.Contains(config, "endpoint: tempo:4317") {
		t.Error("collector does not export traces to Tempo")
	}
	values := readYAML[map[string]any](t, "../monitoring-values-aws.yaml")
	grafana := nested(values, "grafana")
	datasources := list(grafana["additionalDataSources"])
	if len(datasources) != 1 || object(datasources[0])["url"] != "http://tempo.mlp.svc.cluster.local:3200" {
		t.Errorf("Grafana Tempo datasource = %v", datasources)
	}
	grafanaINI := object(grafana["grafana.ini"])
	if _, exposed := grafanaINI["auth.anonymous"]; exposed {
		t.Error("AWS Grafana enables anonymous access")
	}
	for _, component := range []string{"grafana", "prometheus", "alertmanager"} {
		settings := nested(values, component)
		if nested(settings, "service")["type"] != "ClusterIP" || nested(settings, "ingress")["enabled"] != false {
			t.Errorf("AWS %s exposure = %v", component, settings)
		}
	}
}
