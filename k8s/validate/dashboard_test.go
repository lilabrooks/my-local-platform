package validate

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// The dashboard exists once, at this path, and the compose Grafana reads it as
// a file. The in-cluster Grafana cannot read files, so the same JSON is also
// carried in a ConfigMap its sidecar collects. Two copies of one artifact drift
// unless something fails when they do -- this is that something.
//
// The failure it prevents is quiet: the two Grafanas would show different
// panels, each correct on its own machine, and the difference would surface as
// "the demo looked different when I ran it".
const dashboardSource = "../../local/config/grafana/provisioning/dashboards/relay.json"

// TestDashboardConfigMapMatchesTheSourceFile fails when relay.json has been
// edited without re-running scripts/gen-dashboard-configmap.sh.
func TestDashboardConfigMapMatchesTheSourceFile(t *testing.T) {
	want, err := os.ReadFile(dashboardSource)
	if err != nil {
		t.Fatalf("reading %s: %v", dashboardSource, err)
	}

	cm := dashboardConfigMap(t)

	data := nested(cm, "data")
	if data == nil {
		t.Fatal("the dashboard ConfigMap has no data block")
	}
	got, ok := data["relay.json"].(string)
	if !ok {
		t.Fatalf("the dashboard ConfigMap has no relay.json key; found %v", keys(data))
	}

	if got != string(want) {
		t.Errorf("the ConfigMap and %s have diverged.\n"+
			"Run: make monitoring-dashboard\n"+
			"(source is %d bytes, ConfigMap payload is %d)",
			dashboardSource, len(want), len(got))
	}
}

// TestDashboardPayloadKeepsItsContractWithTheDemo checks the dashboard fields
// and queries that something outside it depends on.
//
// An earlier version of this comment claimed to guard the generator's block
// scalar indentation. That was wrong, and checking it rather than asserting it
// is what showed why: a uniform indent change is invisible, because YAML strips
// the common leading whitespace and the payload comes out identical -- there is
// no bug to catch. A NON-uniform one is caught already, by kustomize refusing to
// parse the file, which fails the comparison test above before this one runs.
//
// What is genuinely uncaught elsewhere is a dashboard that is internally fine
// and has quietly broken its contract with everything that links to it:
//
//   - the uid, which `make relay-demo` and the runbook reach as /d/relay-delivery
//   - having panels that query the broker assignment metrics
//
// Change either in relay.json, regenerate honestly, and every other check here
// passes: the ConfigMap matches its source, the YAML is well formed, the JSON
// parses. Only this test notices. Verified by doing exactly that.
func TestDashboardPayloadKeepsItsContractWithTheDemo(t *testing.T) {
	cm := dashboardConfigMap(t)
	payload, _ := nested(cm, "data")["relay.json"].(string)

	var dashboard map[string]any
	if err := json.Unmarshal([]byte(payload), &dashboard); err != nil {
		t.Fatalf("the ConfigMap payload is not valid JSON: %v", err)
	}
	if dashboard["uid"] != "relay-delivery" {
		t.Errorf("dashboard uid = %v, want relay-delivery -- the demo links to /d/relay-delivery",
			dashboard["uid"])
	}
	panels, ok := dashboard["panels"].([]any)
	if !ok || len(panels) == 0 {
		t.Error("the dashboard has no panels")
	}
	var expressions []string
	for _, rawPanel := range panels {
		panel, ok := rawPanel.(map[string]any)
		if !ok {
			continue
		}
		targets, _ := panel["targets"].([]any)
		for _, rawTarget := range targets {
			target, ok := rawTarget.(map[string]any)
			if !ok {
				continue
			}
			if expr, ok := target["expr"].(string); ok {
				expressions = append(expressions, expr)
			}
		}
	}

	for _, metric := range []string{
		"relay_group_members",
		"relay_group_unassigned_members",
		"relay_topic_partitions_unassigned",
	} {
		found := false
		for _, expr := range expressions {
			if strings.Contains(expr, metric) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("the dashboard does not query %s; group assignment would be invisible", metric)
		}
	}
}

func TestDashboardFreshnessPanelMakesAnAbsentIngestScrapeVisible(t *testing.T) {
	cm := dashboardConfigMap(t)
	payload, _ := nested(cm, "data")["relay.json"].(string)

	var dashboard map[string]any
	if err := json.Unmarshal([]byte(payload), &dashboard); err != nil {
		t.Fatalf("the ConfigMap payload is not valid JSON: %v", err)
	}
	panels, _ := dashboard["panels"].([]any)
	for _, rawPanel := range panels {
		panel, _ := rawPanel.(map[string]any)
		if panel["title"] != "Age of the broker measurement" {
			continue
		}

		targets, _ := panel["targets"].([]any)
		if len(targets) != 1 {
			t.Fatalf("freshness panel has %d targets, want 1", len(targets))
		}
		target, _ := targets[0].(map[string]any)
		expr, _ := target["expr"].(string)
		if !strings.Contains(expr, "or vector(999999999)") {
			t.Errorf("freshness query %q has no value for an absent ingest scrape", expr)
		}

		defaults := nested(panel, "fieldConfig", "defaults")
		mappings, _ := defaults["mappings"].([]any)
		for _, rawMapping := range mappings {
			mapping, _ := rawMapping.(map[string]any)
			if mapping["type"] != "value" {
				continue
			}
			options, _ := mapping["options"].(map[string]any)
			result, _ := options["999999999"].(map[string]any)
			if result["text"] == "NO INGEST SCRAPED" && result["color"] == "red" {
				return
			}
		}
		t.Fatal("freshness panel does not map its absent-ingest sentinel to a red message")
	}
	t.Fatal("dashboard has no Age of the broker measurement panel")
}

// TestDashboardConfigMapCarriesTheSidecarLabel asserts the one label that makes
// the ConfigMap visible at all.
//
// The chart's Grafana runs a sidecar watching for `grafana_dashboard: "1"`
// across every namespace. Without that label the ConfigMap is applied
// successfully, reports healthy in ArgoCD, and is collected by nothing.
func TestDashboardConfigMapCarriesTheSidecarLabel(t *testing.T) {
	cm := dashboardConfigMap(t)
	labels := nested(cm, "metadata", "labels")
	if labels == nil {
		t.Fatal("the dashboard ConfigMap has no labels")
	}
	if labels["grafana_dashboard"] != "1" {
		t.Errorf(`grafana_dashboard = %v, want "1" -- without it the sidecar `+
			`never collects this ConfigMap and the panels are simply absent`,
			labels["grafana_dashboard"])
	}
}

// dashboardConfigMap renders the monitoring directory and returns the dashboard
// ConfigMap. Rendered through kustomize rather than read as a file, so the tests
// see what actually reaches the cluster.
func dashboardConfigMap(t *testing.T) map[string]any {
	t.Helper()
	docs := render(t, manifestRoot+"/monitoring")
	for _, cm := range kindsOf(docs, "ConfigMap") {
		if name(cm) == "grafana-dashboard-relay" {
			return cm
		}
	}
	t.Fatal("no ConfigMap named grafana-dashboard-relay in k8s/manifests/monitoring")
	return nil
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
