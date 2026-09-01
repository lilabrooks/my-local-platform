package validate

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

const argocdManifestRoot = "../argocd"

type objectMetadata struct {
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
}

type projectDestination struct {
	Server    string `yaml:"server"`
	Namespace string `yaml:"namespace"`
}

type projectResourceRule struct {
	Group string `yaml:"group"`
	Kind  string `yaml:"kind"`
	Name  string `yaml:"name,omitempty"`
}

type appProjectManifest struct {
	APIVersion string         `yaml:"apiVersion"`
	Kind       string         `yaml:"kind"`
	Metadata   objectMetadata `yaml:"metadata"`
	Spec       struct {
		SourceRepos                []string              `yaml:"sourceRepos"`
		SourceNamespaces           []string              `yaml:"sourceNamespaces"`
		Destinations               []projectDestination  `yaml:"destinations"`
		ClusterResourceWhitelist   []projectResourceRule `yaml:"clusterResourceWhitelist"`
		NamespaceResourceWhitelist []projectResourceRule `yaml:"namespaceResourceWhitelist"`
		NamespaceResourceBlacklist []projectResourceRule `yaml:"namespaceResourceBlacklist"`
	} `yaml:"spec"`
}

type applicationManifest struct {
	APIVersion string         `yaml:"apiVersion"`
	Kind       string         `yaml:"kind"`
	Metadata   objectMetadata `yaml:"metadata"`
	Spec       struct {
		Project string `yaml:"project"`
		Source  struct {
			RepoURL string `yaml:"repoURL"`
			Path    string `yaml:"path"`
		} `yaml:"source"`
		Destination projectDestination `yaml:"destination"`
	} `yaml:"spec"`
}

func readYAML[T any](t *testing.T, path string) T {
	t.Helper()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var out T
	if err := yaml.Unmarshal(b, &out); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return out
}

func requireManifestHeader(t *testing.T, path, apiVersion, kind string, metadata objectMetadata) {
	t.Helper()

	if apiVersion != "argoproj.io/v1alpha1" || kind == "" {
		t.Fatalf("%s has apiVersion %q and kind %q", path, apiVersion, kind)
	}
	if metadata.Namespace != "argocd" {
		t.Fatalf("%s is in namespace %q, want argocd", path, metadata.Namespace)
	}
}

func TestDefaultProjectHasNoPermissions(t *testing.T) {
	path := filepath.Join(argocdManifestRoot, "default-project.yaml")
	project := readYAML[appProjectManifest](t, path)
	requireManifestHeader(t, path, project.APIVersion, project.Kind, project.Metadata)

	if project.Kind != "AppProject" || project.Metadata.Name != "default" {
		t.Fatalf("%s defines %s %q, want AppProject default", path, project.Kind, project.Metadata.Name)
	}
	if len(project.Spec.SourceRepos) != 0 || len(project.Spec.SourceNamespaces) != 0 ||
		len(project.Spec.Destinations) != 0 {
		t.Fatalf("default project grants a source or destination: %+v", project.Spec)
	}
	if project.Spec.ClusterResourceWhitelist == nil || project.Spec.NamespaceResourceWhitelist == nil {
		t.Fatal("default project must explicitly empty both resource whitelists")
	}
	if len(project.Spec.ClusterResourceWhitelist) != 0 || len(project.Spec.NamespaceResourceWhitelist) != 0 {
		t.Fatalf("default project has a resource whitelist: %+v", project.Spec)
	}
	wantBlacklist := []projectResourceRule{{Group: "*", Kind: "*"}}
	if !reflect.DeepEqual(project.Spec.NamespaceResourceBlacklist, wantBlacklist) {
		t.Fatalf("default namespace blacklist = %+v, want %+v",
			project.Spec.NamespaceResourceBlacklist, wantBlacklist)
	}
}

func TestRootProjectCanOnlyCreateApplicationsInArgoCD(t *testing.T) {
	path := filepath.Join(argocdManifestRoot, "root-project.yaml")
	project := readYAML[appProjectManifest](t, path)
	requireManifestHeader(t, path, project.APIVersion, project.Kind, project.Metadata)

	if project.Kind != "AppProject" || project.Metadata.Name != "mlp-root" {
		t.Fatalf("%s defines %s %q, want AppProject mlp-root", path, project.Kind, project.Metadata.Name)
	}
	wantSources := []string{"__REPO_URL__"}
	wantDestinations := []projectDestination{{
		Server: "https://kubernetes.default.svc", Namespace: "argocd",
	}}
	wantResources := []projectResourceRule{{Group: "argoproj.io", Kind: "Application"}}
	if !reflect.DeepEqual(project.Spec.SourceRepos, wantSources) {
		t.Fatalf("root sources = %v, want %v", project.Spec.SourceRepos, wantSources)
	}
	if !reflect.DeepEqual(project.Spec.Destinations, wantDestinations) {
		t.Fatalf("root destinations = %+v, want %+v", project.Spec.Destinations, wantDestinations)
	}
	if len(project.Spec.ClusterResourceWhitelist) != 0 {
		t.Fatalf("root project allows cluster resources: %+v", project.Spec.ClusterResourceWhitelist)
	}
	if !reflect.DeepEqual(project.Spec.NamespaceResourceWhitelist, wantResources) {
		t.Fatalf("root namespace resources = %+v, want %+v",
			project.Spec.NamespaceResourceWhitelist, wantResources)
	}
}

func TestWorkloadProjectStaysInsideMLPNamespace(t *testing.T) {
	path := filepath.Join(argocdManifestRoot, "project.yaml")
	project := readYAML[appProjectManifest](t, path)
	requireManifestHeader(t, path, project.APIVersion, project.Kind, project.Metadata)

	if project.Kind != "AppProject" || project.Metadata.Name != "mlp" {
		t.Fatalf("%s defines %s %q, want AppProject mlp", path, project.Kind, project.Metadata.Name)
	}
	wantSources := []string{"__REPO_URL__"}
	wantDestinations := []projectDestination{{
		Server: "https://kubernetes.default.svc", Namespace: "mlp",
	}}
	wantClusterResources := []projectResourceRule{{Group: "", Kind: "Namespace", Name: "mlp"}}
	wantNamespaceResources := []projectResourceRule{{Group: "*", Kind: "*"}}
	if !reflect.DeepEqual(project.Spec.SourceRepos, wantSources) {
		t.Fatalf("workload sources = %v, want %v", project.Spec.SourceRepos, wantSources)
	}
	if !reflect.DeepEqual(project.Spec.Destinations, wantDestinations) {
		t.Fatalf("workload destinations = %+v, want %+v", project.Spec.Destinations, wantDestinations)
	}
	if !reflect.DeepEqual(project.Spec.ClusterResourceWhitelist, wantClusterResources) {
		t.Fatalf("workload cluster resources = %+v, want %+v",
			project.Spec.ClusterResourceWhitelist, wantClusterResources)
	}
	if !reflect.DeepEqual(project.Spec.NamespaceResourceWhitelist, wantNamespaceResources) {
		t.Fatalf("workload namespace resources = %+v, want %+v",
			project.Spec.NamespaceResourceWhitelist, wantNamespaceResources)
	}
}

func TestRootApplicationUsesRootProject(t *testing.T) {
	path := filepath.Join(argocdManifestRoot, "root-app.yaml")
	app := readYAML[applicationManifest](t, path)
	requireManifestHeader(t, path, app.APIVersion, app.Kind, app.Metadata)

	if app.Kind != "Application" || app.Metadata.Name != "root" {
		t.Fatalf("%s defines %s %q, want Application root", path, app.Kind, app.Metadata.Name)
	}
	if app.Spec.Project != "mlp-root" {
		t.Fatalf("root Application uses project %q, want mlp-root", app.Spec.Project)
	}
	if app.Spec.Source.RepoURL != "__REPO_URL__" || app.Spec.Source.Path != "k8s/apps" {
		t.Fatalf("root source = %+v", app.Spec.Source)
	}
	wantDestination := projectDestination{
		Server: "https://kubernetes.default.svc", Namespace: "argocd",
	}
	if app.Spec.Destination != wantDestination {
		t.Fatalf("root destination = %+v, want %+v", app.Spec.Destination, wantDestination)
	}
}

func TestChildApplicationsUseWorkloadProject(t *testing.T) {
	paths, err := filepath.Glob("../apps/*.yaml")
	if err != nil {
		t.Fatalf("find child Applications: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no child Applications found")
	}

	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			app := readYAML[applicationManifest](t, path)
			requireManifestHeader(t, path, app.APIVersion, app.Kind, app.Metadata)
			if app.Kind != "Application" {
				t.Fatalf("%s defines %s, want Application", path, app.Kind)
			}
			if app.Spec.Project != "mlp" {
				t.Fatalf("%s uses project %q, want mlp", path, app.Spec.Project)
			}
			if app.Spec.Destination.Server != "https://kubernetes.default.svc" ||
				app.Spec.Destination.Namespace != "mlp" {
				t.Fatalf("%s deploys to %+v, want in-cluster namespace mlp",
					path, app.Spec.Destination)
			}
		})
	}
}

func TestArgoCDScriptsPreserveProjectMigrationOrder(t *testing.T) {
	wantOrder := []string{
		"$HERE/root-project.yaml",
		"$HERE/root-app.yaml",
		"$HERE/project.yaml",
		"$HERE/default-project.yaml",
	}

	for _, name := range []string{"install.sh", "repo-creds.sh"} {
		path := filepath.Join(argocdManifestRoot, name)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(b)
		previous := -1
		for _, token := range wantOrder {
			position := strings.Index(text, token)
			if position < 0 {
				t.Errorf("%s does not apply %s", path, token)
				continue
			}
			if position <= previous {
				t.Errorf("%s applies %s out of migration order", path, token)
			}
			previous = position
		}
	}
}
