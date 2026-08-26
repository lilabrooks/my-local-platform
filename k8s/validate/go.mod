module github.com/lilabrooks/my-local-platform/k8svalidate

go 1.27.0

require (
	github.com/lilabrooks/my-local-platform/relay v0.0.0-00010101000000-000000000000
	sigs.k8s.io/yaml v1.6.0
)

require go.yaml.in/yaml/v2 v2.4.2 // indirect

replace github.com/lilabrooks/my-local-platform/relay => ../../services/relay
