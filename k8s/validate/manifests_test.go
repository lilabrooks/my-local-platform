// Package validate holds tests that assert properties of the Kubernetes
// manifests in this repository.
//
// These are invariants that a single `kubectl apply` would not catch, because
// they only bite on the second apply or in a specific failure mode. Running
// them as Go tests keeps the check in the same language as the rest of the
// repo and makes it runnable locally with `go test`, not only in CI.
//
// ALWAYS RUN WITH -count=1. These tests read YAML through `kubectl kustomize`,
// and Go's test cache keys on Go sources -- not on the manifests. Editing a
// manifest and re-running plain `go test` replays a cached PASS and tells you
// nothing. `make k8s-validate` and CI both pass -count=1.
package validate

import (
	"os/exec"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// render shells out to kustomize via kubectl. Reimplementing kustomize in
// order to test kustomize output would test the reimplementation instead.
func render(t *testing.T, dir string) []map[string]any {
	t.Helper()

	cmd := exec.Command("kubectl", "kustomize", dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl kustomize %s: %v\n%s", dir, err, out)
	}

	var docs []map[string]any
	for _, chunk := range strings.Split(string(out), "\n---") {
		if strings.TrimSpace(chunk) == "" {
			continue
		}
		var doc map[string]any
		if err := yaml.Unmarshal([]byte(chunk), &doc); err != nil {
			t.Fatalf("parsing rendered yaml: %v", err)
		}
		if len(doc) > 0 {
			docs = append(docs, doc)
		}
	}
	if len(docs) == 0 {
		t.Fatalf("%s rendered no documents", dir)
	}
	return docs
}

func kind(docs []map[string]any, want string) map[string]any {
	for _, d := range docs {
		if d["kind"] == want {
			return d
		}
	}
	return nil
}

// nested walks a path of map keys, returning nil if any step is missing.
func nested(doc map[string]any, path ...string) map[string]any {
	cur := doc
	for _, key := range path {
		next, ok := cur[key].(map[string]any)
		if !ok {
			return nil
		}
		cur = next
	}
	return cur
}

const managedBy = "app.kubernetes.io/managed-by"

// A Deployment's selector is immutable after creation. If a kustomize label
// leaks into it, the manifests apply once on a fresh cluster and then fail
// forever with "field is immutable" -- a bug that only appears on the second
// apply, which is exactly when nobody is looking.
func TestDeploymentSelectorExcludesManagedBy(t *testing.T) {
	docs := render(t, "../manifests/echo")

	deploy := kind(docs, "Deployment")
	if deploy == nil {
		t.Fatal("no Deployment in rendered output")
	}

	selector := nested(deploy, "spec", "selector", "matchLabels")
	if selector == nil {
		t.Fatal("Deployment has no spec.selector.matchLabels")
	}
	if _, found := selector[managedBy]; found {
		t.Errorf("%s leaked into the immutable Deployment selector: %v", managedBy, selector)
	}
	if selector["app.kubernetes.io/name"] != "echo" {
		t.Errorf("selector should match on the app name, got %v", selector)
	}
}

// The label still has to reach the pods, which is the point of setting it.
// includeSelectors:false alone would drop it from the pod template too.
func TestPodTemplateCarriesManagedBy(t *testing.T) {
	docs := render(t, "../manifests/echo")

	deploy := kind(docs, "Deployment")
	if deploy == nil {
		t.Fatal("no Deployment in rendered output")
	}

	labels := nested(deploy, "spec", "template", "metadata", "labels")
	if labels == nil {
		t.Fatal("Deployment has no pod template labels")
	}
	if labels[managedBy] != "argocd" {
		t.Errorf("pod template is missing %s=argocd, got %v", managedBy, labels)
	}
}

// The Service must select the same pods the Deployment creates. A mismatch
// yields a Service with no endpoints, which looks like an application failure
// rather than a manifest error.
func TestServiceSelectorMatchesPods(t *testing.T) {
	docs := render(t, "../manifests/echo")

	deploy, svc := kind(docs, "Deployment"), kind(docs, "Service")
	if deploy == nil || svc == nil {
		t.Fatal("expected both a Deployment and a Service")
	}

	podLabels := nested(deploy, "spec", "template", "metadata", "labels")
	svcSelector := nested(svc, "spec", "selector")
	if svcSelector == nil {
		t.Fatal("Service has no spec.selector")
	}

	for key, want := range svcSelector {
		if got, ok := podLabels[key]; !ok || got != want {
			t.Errorf("Service selects %s=%v but pods have %v -- Service would have no endpoints",
				key, want, podLabels[key])
		}
	}
}

// Probes are what make a rolling update safe. Without a readiness probe,
// Kubernetes routes traffic to a container the moment it starts.
func TestContainerHasProbes(t *testing.T) {
	docs := render(t, "../manifests/echo")

	deploy := kind(docs, "Deployment")
	spec := nested(deploy, "spec", "template", "spec")
	if spec == nil {
		t.Fatal("Deployment has no pod spec")
	}

	containers, ok := spec["containers"].([]any)
	if !ok || len(containers) == 0 {
		t.Fatal("pod spec has no containers")
	}

	first, ok := containers[0].(map[string]any)
	if !ok {
		t.Fatal("container is not an object")
	}
	for _, probe := range []string{"readinessProbe", "livenessProbe"} {
		if _, found := first[probe]; !found {
			t.Errorf("container has no %s", probe)
		}
	}
}
