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
//
// Directories are DISCOVERED rather than listed. An earlier version named
// ../manifests/echo in every test, so relay and sink would have been added
// with no coverage at all while the suite still reported success -- the same
// way scripts/lint.sh and the CI build matrix each silently skipped a new
// module until someone noticed.
package validate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

const manifestRoot = "../manifests"

// manifestDirs returns every directory holding a kustomization.yaml.
func manifestDirs(t *testing.T) []string {
	t.Helper()

	entries, err := os.ReadDir(manifestRoot)
	if err != nil {
		t.Fatalf("reading %s: %v", manifestRoot, err)
	}

	var dirs []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(manifestRoot, e.Name())
		if _, err := os.Stat(filepath.Join(dir, "kustomization.yaml")); err == nil {
			dirs = append(dirs, dir)
		}
	}
	if len(dirs) == 0 {
		t.Fatalf("no kustomize directories under %s -- discovery is broken, "+
			"and a suite that finds nothing passes vacuously", manifestRoot)
	}
	return dirs
}

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

// kindsOf returns every document of a kind, not just the first -- relay has two
// Deployments and two Services.
func kindsOf(docs []map[string]any, want string) []map[string]any {
	var out []map[string]any
	for _, d := range docs {
		if d["kind"] == want {
			out = append(out, d)
		}
	}
	return out
}

func name(doc map[string]any) string {
	meta := nested(doc, "metadata")
	if meta == nil {
		return "<unnamed>"
	}
	n, _ := meta["name"].(string)
	return n
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

// eachDeployment runs fn over every Deployment in every manifest directory,
// as a subtest named for the workload.
func eachDeployment(t *testing.T, fn func(t *testing.T, dir string, deploy map[string]any)) {
	t.Helper()
	for _, dir := range manifestDirs(t) {
		docs := render(t, dir)
		deploys := kindsOf(docs, "Deployment")
		if len(deploys) == 0 {
			t.Errorf("%s has a kustomization but no Deployment", dir)
			continue
		}
		for _, d := range deploys {
			t.Run(filepath.Base(dir)+"/"+name(d), func(t *testing.T) {
				fn(t, dir, d)
			})
		}
	}
}

// A Deployment's selector is immutable after creation. If a kustomize label
// leaks into it, the manifests apply once on a fresh cluster and then fail
// forever with "field is immutable" -- a bug that only appears on the second
// apply, which is exactly when nobody is looking.
func TestDeploymentSelectorExcludesManagedBy(t *testing.T) {
	eachDeployment(t, func(t *testing.T, _ string, deploy map[string]any) {
		selector := nested(deploy, "spec", "selector", "matchLabels")
		if selector == nil {
			t.Fatal("Deployment has no spec.selector.matchLabels")
		}
		if _, found := selector[managedBy]; found {
			t.Errorf("%s leaked into the immutable Deployment selector: %v", managedBy, selector)
		}
		if selector["app.kubernetes.io/name"] != name(deploy) {
			t.Errorf("selector should match on the app name %q, got %v", name(deploy), selector)
		}
	})
}

// The label still has to reach the pods, which is the point of setting it.
// includeSelectors:false alone would drop it from the pod template too.
func TestPodTemplateCarriesManagedBy(t *testing.T) {
	eachDeployment(t, func(t *testing.T, _ string, deploy map[string]any) {
		labels := nested(deploy, "spec", "template", "metadata", "labels")
		if labels == nil {
			t.Fatal("Deployment has no pod template labels")
		}
		if labels[managedBy] != "argocd" {
			t.Errorf("pod template is missing %s=argocd, got %v", managedBy, labels)
		}
	})
}

// Probes are what make a rolling update safe. Without a readiness probe,
// Kubernetes routes traffic to a container the moment it starts.
func TestContainerHasProbes(t *testing.T) {
	eachDeployment(t, func(t *testing.T, _ string, deploy map[string]any) {
		spec := nested(deploy, "spec", "template", "spec")
		if spec == nil {
			t.Fatal("Deployment has no pod spec")
		}
		containers, ok := spec["containers"].([]any)
		if !ok || len(containers) == 0 {
			t.Fatal("pod spec has no containers")
		}
		for i, c := range containers {
			container, ok := c.(map[string]any)
			if !ok {
				t.Fatalf("container %d is not an object", i)
			}
			for _, probe := range []string{"readinessProbe", "livenessProbe"} {
				if _, found := container[probe]; !found {
					t.Errorf("container %v has no %s", container["name"], probe)
				}
			}
		}
	})
}

// An image the cluster cannot pull leaves the pod in ImagePullBackOff, which
// reads as a broken application rather than a missing `make images`.
func TestLocalImagesAreNotPulledFromARegistry(t *testing.T) {
	eachDeployment(t, func(t *testing.T, _ string, deploy map[string]any) {
		spec := nested(deploy, "spec", "template", "spec")
		containers, _ := spec["containers"].([]any)
		for _, c := range containers {
			container, _ := c.(map[string]any)
			image, _ := container["image"].(string)
			// Locally built tags have no registry host and no digest.
			if !strings.HasSuffix(image, ":dev") {
				continue
			}
			if got := container["imagePullPolicy"]; got != "IfNotPresent" {
				t.Errorf("%s uses locally loaded image %q with imagePullPolicy %v; "+
					"only IfNotPresent works for a tag that exists in no registry",
					container["name"], image, got)
			}
		}
	})
}

// A Service must select pods that some Deployment in the same directory
// actually creates. A mismatch yields a Service with no endpoints, which looks
// like an application failure rather than a manifest error.
func TestServiceSelectorsMatchSomePod(t *testing.T) {
	for _, dir := range manifestDirs(t) {
		docs := render(t, dir)
		deploys := kindsOf(docs, "Deployment")

		for _, svc := range kindsOf(docs, "Service") {
			t.Run(filepath.Base(dir)+"/"+name(svc), func(t *testing.T) {
				selector := nested(svc, "spec", "selector")
				if selector == nil {
					t.Fatal("Service has no spec.selector")
				}

				for _, deploy := range deploys {
					podLabels := nested(deploy, "spec", "template", "metadata", "labels")
					matches := true
					for key, want := range selector {
						if got, ok := podLabels[key]; !ok || got != want {
							matches = false
							break
						}
					}
					if matches {
						return
					}
				}
				t.Errorf("Service selects %v but no Deployment in %s creates matching pods -- "+
					"the Service would have no endpoints", selector, dir)
			})
		}
	}
}
