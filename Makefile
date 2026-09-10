# my-local-platform
#
# Local-first: everything runs in Docker for free. Real AWS is opt-in and
# ephemeral -- see the `aws-*` targets and docs/costs.md before applying.

SHELL := /bin/bash
ENV_FILE ?= $(if $(wildcard .env),.env,.env.example)
COMPOSE = docker compose --env-file "$(ENV_FILE)" -f local/docker-compose.yml
COMPOSE_ENV = MLP_ENV_FILE="$(ENV_FILE)" python3 scripts/with-compose-env.py

AWS_PROFILE_NAME ?= aws-public-change-feed
AWS_REAL_REGION ?= us-east-1
AWS_INIT_ARGS ?=
AWS_TF_ARGS ?=
AWS_PLAN_FILE ?= infra/terraform/envs/dev/.terraform/mlp-reviewed.tfplan
AWS_GUARDRAILS_PLAN_FILE ?= infra/terraform/guardrails/.terraform/mlp-guardrails.tfplan
AWS_DESTROY_ARGS ?=
AWS_PLAN_SUMMARY ?= .evidence/m4/$(AWS_RUN_ID)/03-plan-summary.json
AWS_ACCOUNT_EVIDENCE ?= .evidence/m4/$(AWS_RUN_ID)/01-identity.txt
AWS_INVENTORY_FILE ?= .evidence/m4/$(AWS_RUN_ID)/04-inventory-before.json
AWS_IMAGE_EVIDENCE ?= .evidence/m4/$(AWS_RUN_ID)/05-images.json
AWS_PRICE_INPUT ?= .evidence/m4/$(AWS_RUN_ID)/price-input.json
AWS_PRICE_EVIDENCE ?= .evidence/m4/$(AWS_RUN_ID)/02-prices.md
AWS_GO_NO_GO ?= .evidence/m4/$(AWS_RUN_ID)/06-go-no-go.json
AWS_REAL_ENV = env -i \
	HOME="$(HOME)" \
	PATH="$(PATH)" \
	TMPDIR="$(TMPDIR)" \
	AWS_PROFILE="$(AWS_PROFILE_NAME)" \
	AWS_REGION="$(AWS_REAL_REGION)" \
	AWS_DEFAULT_REGION="$(AWS_REAL_REGION)" \
	TF_VAR_region="$(AWS_REAL_REGION)"

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Local stack
# ---------------------------------------------------------------------------

.PHONY: up
up: ## Start everything (~1.6GB sustained; see docs/runbook-local.md)
	# --build because relay and the sink are built from source and `all`
	# includes them. Without it, compose starts the image it built last, so
	# editing relay and running the documented `make up && make smoke` reports
	# PASS for code that is not running. Docker's cache makes a no-change
	# rebuild a few seconds; a green check against a stale binary costs more.
	$(COMPOSE) --profile all up -d --build
	$(MAKE) seed

.PHONY: up-core
up-core: ## Start floci (AWS surface) + postgres only
	$(COMPOSE) --profile core up -d
	$(COMPOSE_ENV) AWS_ENDPOINT_URL AWS_DEFAULT_REGION \
		AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY -- ./local/bootstrap/seed.sh

.PHONY: up-core-containers
up-core-containers: ## Start core WITH the docker socket (needed for floci RDS/EKS/Lambda)
	@echo "Granting floci the docker socket: effective root on this host."
	@echo "Only needed for floci's container-backed services. See ADR 0002."
	$(COMPOSE) -f local/docker-compose.floci-containers.yml --profile core up -d
	$(COMPOSE_ENV) AWS_ENDPOINT_URL AWS_DEFAULT_REGION \
		AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY -- ./local/bootstrap/seed.sh

.PHONY: up-messaging
up-messaging: ## Start Kafka and RabbitMQ (~660MB)
	$(COMPOSE) --profile messaging up -d
	$(COMPOSE_ENV) BROKER_CONTAINER KAFKA_INTERNAL_BOOTSTRAP \
		-- ./local/bootstrap/kafka-topics.sh

.PHONY: up-tools
up-tools: ## Add Kafka UI (~285MB; needs the messaging profile)
	$(COMPOSE) --profile messaging --profile tools up -d

.PHONY: up-apps
up-apps: ## Start relay and sink (built from source; brings core and messaging too)
	$(COMPOSE) \
		--profile core --profile messaging --profile apps up -d --build --wait
	@echo "  relay ingest  http://localhost:8082  (POST /v1/events)"
	@echo "  relay deliver http://localhost:8083/readyz"
	@echo "  sink          http://localhost:8084/received"
	@echo "  run 'make seed' if you have not already -- relay needs its subscriptions"

.PHONY: mem
mem: ## Show what the stack is actually using right now
	@docker stats --no-stream --format '{{.MemUsage}}\t{{.Name}}' | sort -h -r

.PHONY: up-obs
up-obs: ## Start OTel collector, Prometheus, Tempo, Grafana
	$(COMPOSE) --profile obs up -d

.PHONY: seed
seed: ## Create local AWS resources and Kafka topics (idempotent)
	$(COMPOSE_ENV) AWS_ENDPOINT_URL AWS_DEFAULT_REGION \
		AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY -- ./local/bootstrap/seed.sh
	$(COMPOSE_ENV) BROKER_CONTAINER KAFKA_INTERNAL_BOOTSTRAP \
		-- ./local/bootstrap/kafka-topics.sh
	$(COMPOSE_ENV) PG_CONTAINER POSTGRES_USER POSTGRES_DB RELAY_SIGNING_SECRET \
		-- ./local/bootstrap/relay-db.sh

.PHONY: down
down: ## Stop the stack, keep volumes
	$(COMPOSE) --profile all down

.PHONY: clean
clean: ## Stop the stack and DELETE all local data volumes
	$(COMPOSE) --profile all down -v

.PHONY: ps
ps: ## Show running services
	$(COMPOSE) --profile all ps

.PHONY: logs
logs: ## Tail logs (make logs SVC=kafka)
	$(COMPOSE) logs -f $(SVC)

.PHONY: urls
urls: ## Print the local endpoints
	@echo "floci (AWS)     http://localhost:4566"
	@echo "Kafka           localhost:9092"
	@echo "Kafka UI        http://localhost:8080"
	@echo "RabbitMQ AMQP   localhost:5672"
	@echo "RabbitMQ UI     http://localhost:15672   (guest/guest)"
	@echo "Postgres        localhost:5432           (platform/platform)"
	@echo "OTLP gRPC       localhost:4317"
	@echo "Prometheus      http://localhost:9090"
	@echo "Grafana         http://localhost:3000    (anonymous viewer)"
	@echo "relay dashboard http://localhost:3000/d/relay-delivery"
	@echo "relay ingest    http://localhost:8082    (POST /v1/events, /metrics; apps profile)"
	@echo "relay deliver   http://localhost:8083    (/readyz, /metrics; apps profile)"
	@echo "sink            http://localhost:8084    (/received, /metrics; apps profile)"

# ---------------------------------------------------------------------------
# Smoke service
# ---------------------------------------------------------------------------

# Extra environment for the smoke run. Empty here; smoke-traces sets it.
SMOKE_ENV =

.PHONY: smoke
smoke: ## Run the end-to-end smoke check against the local stack
	$(SMOKE_ENV) $(COMPOSE_ENV) --chdir services/smoke \
		AWS_ENDPOINT_URL AWS_DEFAULT_REGION MLP_USE_REAL_AWS \
		MLP_BUCKET MLP_TOPIC MLP_QUEUE MLP_SES_SENDER \
		KAFKA_BOOTSTRAP MLP_KAFKA_TOPIC MLP_RELAY_TOPIC MLP_RELAY_DLQ_TOPIC \
		RELAY_INGEST_URL SINK_URL RABBITMQ_URL DATABASE_URL \
		OTEL_EXPORTER_OTLP_ENDPOINT OTEL_SERVICE_NAME TEMPO_URL \
		-- go run ./cmd/smoke

.PHONY: smoke-traces
smoke-traces: ## Run smoke AND require the relay trace in Tempo (needs the obs profile)
	# `make smoke` treats tracing as best effort, because relay lives in the
	# `apps` compose profile and Tempo in `obs` -- CI and the documented
	# `make up-apps` path both run without it. This target is the one that
	# asserts the trace, so a run that proves the end-to-end path names itself
	# rather than depending on which containers happened to be up.
	#
	# Passed as a plain environment assignment rather than through
	# with-compose-env.py's key list: that script pops every listed key from the
	# environment and refills it from the dotenv file, which is what makes the
	# dotenv authoritative for stack addresses -- and what would drop this.
	# Needs `make up` (or `make up-obs`).
	$(MAKE) smoke SMOKE_ENV='MLP_SMOKE_REQUIRE_TRACES=1'

.PHONY: relay-replay
relay-replay: ## Redeliver relay events from the last SINCE (default 1h; or SINCE=earliest)
	./scripts/relay-replay.sh

.PHONY: relay-demo
relay-demo: ## The M2 demo: six steps against the cluster, narrated (~4 min)
	./scripts/relay-demo.sh

.PHONY: relay-replay-verify
relay-replay-verify: ## Prove replay works: deliver, wipe, replay, assert the same ids return
	./scripts/verify-replay.sh

.PHONY: relay-verify-ordering
relay-verify-ordering: ## Assert one tenant's events are delivered in the order accepted
	./scripts/verify-ordering.sh

.PHONY: relay-verify-ordering-go
relay-verify-ordering-go: ## Run the Go pilot of the steady-state ordering assertion
	cd services/smoke && go run ./cmd/relay-verify ordering

.PHONY: relay-verify-duplicate-on-crash
relay-verify-duplicate-on-crash: ## Kill the consumer mid-record; assert the same webhook-id is redelivered
	./scripts/verify-duplicate-on-crash.sh

.PHONY: relay-verify-graceful-drain
relay-verify-graceful-drain: ## SIGTERM mid-record; assert relay drains, commits, and exits cleanly
	./scripts/verify-graceful-drain.sh

# Not in CI, for the same reason as the rebalance probe: it starts a second
# consumer and measures wall-clock delay, which is a demonstration rather than a
# gate. Issue #73 asked for it that way deliberately.
.PHONY: relay-verify-head-of-line
relay-verify-head-of-line: ## Show head-of-line blocking is member-scoped, not partition-scoped
	./scripts/verify-head-of-line.sh

# Not in CI, unlike the other three verify targets: it starts a second consumer,
# waits on a real group rebalance, and takes about a minute. It is a tool for
# investigating issue #69 rather than a gate.
.PHONY: relay-verify-ordering-rebalance
relay-verify-ordering-rebalance: ## Same assertion, with group membership changing mid-run
	./scripts/verify-ordering-rebalance.sh

# Every Go module, derived rather than restated.
#
# This list was hardcoded in four places and wrong in three of them: the CI
# build matrix (relay and sink "merged with a green tick that built none of
# their code" -- see ci.yml), AGENTS.md, .github/dependabot.yml, and these
# targets, where tidy/fmt/vet each covered services/smoke alone. The first
# three are fixed; a fifth hand-maintained copy is not the way to fix the
# fourth. scripts/lint.sh has always derived its own list the same way.
GO_MODULES := $(shell find . -name go.mod -not -path './*/.terraform/*' \
                -not -path './*/node_modules/*' -exec dirname {} \; \
                | sed 's|^\./||' | sort)

# Modules whose tests read files the Go test cache does not track, so a cached
# pass proves nothing. k8s/validate reads the YAML under k8s/manifests and the
# topic partition counts out of local/bootstrap/kafka-topics.sh and the shared
# services/relay/internal/bootstrap/topics.txt file.
UNCACHED_MODULES := k8s/validate

.PHONY: test
test: ## Run Go module and infrastructure guard tests
	@for m in $(GO_MODULES); do \
	  flags=""; \
	  for u in $(UNCACHED_MODULES); do \
	    if [ "$$m" = "$$u" ]; then flags="-count=1"; fi; \
	  done; \
	  echo "==> $$m: go test -race $$flags ./..."; \
	  ( cd "$$m" && go test -race $$flags ./... ) || exit 1; \
	done
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests

.PHONY: tidy
tidy: ## go mod tidy in every module
	@for m in $(GO_MODULES); do \
	  echo "==> $$m: go mod tidy"; \
	  ( cd "$$m" && go mod tidy ) || exit 1; \
	done

.PHONY: lint
lint: ## Run lint, documentation, infrastructure, security, and secret checks
	./scripts/lint.sh

.PHONY: fmt
fmt: ## Format Go code in every module
	@for m in $(GO_MODULES); do \
	  echo "==> $$m: go fmt"; \
	  ( cd "$$m" && go fmt ./... ) || exit 1; \
	done

.PHONY: vet
vet: ## go vet every module
	@for m in $(GO_MODULES); do \
	  echo "==> $$m: go vet"; \
	  ( cd "$$m" && go vet ./... ) || exit 1; \
	done

TERRAFORM_STACKS := infra/terraform/bootstrap infra/terraform/guardrails infra/terraform/envs/dev

.PHONY: terraform-check
terraform-check: ## Run the offline Terraform checks used by CI
	@for stack in $(TERRAFORM_STACKS); do \
	  echo "==> $$stack: terraform fmt, init, and validate"; \
	  terraform -chdir="$$stack" fmt -check -recursive || exit 1; \
	  terraform -chdir="$$stack" init -backend=false -input=false || exit 1; \
	  terraform -chdir="$$stack" validate || exit 1; \
	  if [ "$$stack" != "infra/terraform/bootstrap" ]; then \
	    echo "==> $$stack: terraform test"; \
	    terraform -chdir="$$stack" test || exit 1; \
	  fi; \
	done

# ---------------------------------------------------------------------------
# Kubernetes + GitOps (local, free)
# ---------------------------------------------------------------------------

MINIKUBE_PROFILE ?= mlp

# Raised from 3g after measuring. At 3g the supporting cast alone -- ArgoCD,
# KEDA, kube-system and kube-prometheus-stack -- held the node container at
# 88-92% of its cap with ZERO relay-deliver replicas, and the control plane
# thrashed: 22 restarts in kube-system, etcd and the apiserver among them.
#
# The node cannot warn about this. minikube's kubelet reports the Docker VM's
# memory as node allocatable (7.75Gi), not the container's cgroup limit, so
# MemoryPressure stays False and the kernel kills processes inside the
# container instead. They surface as `Error exit=1`, not OOMKilled, which reads
# as unrelated application crashes.
#
# Memory cannot be changed on an existing cluster with the docker driver:
# `make k8s-delete` first, then `make k8s-up`.
MINIKUBE_MEMORY  ?= 6g
REPO_URL         ?= https://github.com/lilabrooks/my-local-platform.git

.PHONY: k8s-up
k8s-up: ## Start the local Kubernetes cluster (minikube profile 'mlp')
	minikube start -p $(MINIKUBE_PROFILE) --driver=docker --nodes=1 \
	  --cpus=4 --memory=$(MINIKUBE_MEMORY) --kubernetes-version=v1.35.1
	kubectl config use-context $(MINIKUBE_PROFILE)

.PHONY: k8s-down
k8s-down: ## Stop the cluster, keep its state
	minikube stop -p $(MINIKUBE_PROFILE)

.PHONY: k8s-delete
k8s-delete: ## Delete the cluster entirely
	minikube delete -p $(MINIKUBE_PROFILE)

.PHONY: echo-image
echo-image: ## Build the echo image and load it into the cluster
	cd services/echo && docker build --build-arg VERSION=$$(git rev-parse --short HEAD) -t echo:dev .
	minikube image load echo:dev -p $(MINIKUBE_PROFILE)

.PHONY: relay-image
relay-image: ## Build the relay image and load it into the cluster
	cd services/relay && docker build --build-arg VERSION=$$(git rev-parse --short HEAD) -t relay:dev .
	minikube image load relay:dev -p $(MINIKUBE_PROFILE)

.PHONY: sink-image
sink-image: ## Build the sink image and load it into the cluster
	cd services/sink && docker build --build-arg VERSION=$$(git rev-parse --short HEAD) -t sink:dev .
	minikube image load sink:dev -p $(MINIKUBE_PROFILE)

# `minikube image load` is what lets the manifests use imagePullPolicy:
# IfNotPresent against a tag that exists in no registry. Without it a pod sits
# in ImagePullBackOff trying to reach Docker Hub for `relay:dev`.
#
# It is also slow -- minutes per image, most of it spent transferring the layers
# into the cluster rather than building. That is expected, not a hang.
.PHONY: images
images: echo-image relay-image sink-image ## Build and load every workload image (slow: minutes per image)

# Pinned like every other component here. --server-side because KEDA's CRDs
# exceed the annotation size limit a client-side apply has to work within, the
# same way the ArgoCD manifests do.
KEDA_VERSION ?= 2.20.2

.PHONY: keda-install
keda-install: ## Install KEDA into the cluster (pinned; needed for lag autoscaling)
	kubectl apply --server-side -f \
	  https://github.com/kedacore/keda/releases/download/v$(KEDA_VERSION)/keda-$(KEDA_VERSION).yaml
	kubectl -n keda rollout status deploy/keda-operator --timeout=180s

# kube-prometheus-stack, pinned like every other cluster component. Installed
# by this target rather than synced by ArgoCD: routing the chart through ArgoCD
# would need k8s/argocd/project.yaml widened -- a second sourceRepos entry, a
# monitoring destination, and clusterResourceWhitelist opened to CRDs and
# ClusterRoles -- which loosens a boundary that file exists to enforce. KEDA
# sets the precedent. See docs/adr/0008-in-cluster-observability-for-the-demo.md.
#
# MONITORING_RELEASE is not cosmetic. The chart renders its Prometheus with
# serviceMonitorSelector matchLabels release=<release>, and a ServiceMonitor
# without a matching label is selected by nothing and reports no error. The
# value here must equal the label in k8s/manifests/monitoring/servicemonitor.yaml.
KPS_VERSION        ?= 88.5.4
MONITORING_RELEASE ?= monitoring
MONITORING_NS      ?= monitoring

.PHONY: monitoring-install
monitoring-install: ## Install kube-prometheus-stack into the cluster (pinned; the demo's panel)
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update prometheus-community
	helm upgrade --install $(MONITORING_RELEASE) prometheus-community/kube-prometheus-stack \
	  --version $(KPS_VERSION) \
	  --namespace $(MONITORING_NS) --create-namespace \
	  --values k8s/monitoring-values.yaml \
	  --values k8s/monitoring-values-local.yaml \
	  --wait --timeout 10m
	@echo
	@echo "  next:  make monitoring-ready   (asserts the demo's panel will have data)"
	@echo "         make monitoring-ui      (Grafana on :3001, not 3000)"

.PHONY: monitoring-install-aws
monitoring-install-aws: ## Install the pinned private evidence stack on the current EKS cluster
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update prometheus-community
	helm upgrade --install $(MONITORING_RELEASE) prometheus-community/kube-prometheus-stack \
	  --version $(KPS_VERSION) \
	  --namespace $(MONITORING_NS) --create-namespace \
	  --values k8s/monitoring-values.yaml \
	  --values k8s/monitoring-values-aws.yaml \
	  --wait --timeout 10m

.PHONY: monitoring-ready
monitoring-ready: ## Assert Prometheus is actually scraping relay-deliver
	MONITORING_RELEASE=$(MONITORING_RELEASE) MONITORING_NAMESPACE=$(MONITORING_NS) 	  ./scripts/monitoring-ready.sh

.PHONY: monitoring-dashboard
monitoring-dashboard: ## Regenerate the in-cluster dashboard ConfigMap from relay.json
	./scripts/gen-dashboard-configmap.sh

# 3001, because the compose Grafana holds 3000. Running both and guessing which
# one you are looking at is how a demo shows the wrong stack -- the sink already
# sits on 8084 for the same reason.
.PHONY: monitoring-password
monitoring-password: ## Print the in-cluster Grafana admin password
	@kubectl -n $(MONITORING_NS) get secret $(MONITORING_RELEASE)-grafana \
	  -o jsonpath='{.data.admin-password}' | base64 -d; echo

.PHONY: monitoring-ui
monitoring-ui: ## Port-forward the in-cluster Grafana to http://localhost:3001
	@echo "http://localhost:3001/d/relay-delivery   (no login -- anonymous admin)"
	@echo
	@echo "  Anonymous access comes from k8s/monitoring-values-local.yaml, matching"
	@echo "  compose Grafana. The login form still works for admin-only pages:"
	@echo "  user admin, password from: make monitoring-password"
	@echo "  (NOT prom-operator -- this chart version generates a random one.)"
	@echo
	kubectl -n $(MONITORING_NS) port-forward svc/$(MONITORING_RELEASE)-grafana 3001:80

.PHONY: argocd-install
argocd-install: ## Install ArgoCD and register the app-of-apps
	REPO_URL=$(REPO_URL) ./k8s/argocd/install.sh

AWS_ROOT_APPLICATION ?=

.PHONY: argocd-install-aws
argocd-install-aws: ## Install ArgoCD using the generated AWS root Application
	@test -n "$(AWS_ROOT_APPLICATION)" || { echo "AWS_ROOT_APPLICATION is required" >&2; exit 2; }
	@test -f "$(AWS_ROOT_APPLICATION)" || { echo "not found: $(AWS_ROOT_APPLICATION)" >&2; exit 2; }
	REPO_URL=$(REPO_URL) ROOT_APPLICATION_FILE="$(AWS_ROOT_APPLICATION)" ./k8s/argocd/install.sh

.PHONY: argocd-repo-creds
argocd-repo-creds: ## Give ArgoCD read access to a private fork (deploy key)
	ROOT_APPLICATION_FILE="$(AWS_ROOT_APPLICATION)" ./k8s/argocd/repo-creds.sh

.PHONY: argocd-password
argocd-password: ## Print the initial ArgoCD admin password
	@kubectl -n argocd get secret argocd-initial-admin-secret \
	  -o jsonpath='{.data.password}' | base64 -d; echo

.PHONY: argocd-ui
argocd-ui: ## Port-forward the ArgoCD UI to https://localhost:8081
	@echo "https://localhost:8081  (admin / \`make argocd-password\`)"
	kubectl port-forward -n argocd svc/argocd-server 8081:443

.PHONY: k8s-apply-local
k8s-apply-local: ## Apply manifests directly, bypassing git and ArgoCD
	kubectl apply -f k8s/manifests/namespace.yaml
	kubectl apply -k k8s/manifests/echo
	kubectl apply -k k8s/manifests/relay
	kubectl apply -k k8s/manifests/sink
	@# Skipped rather than failed when the operator CRDs are absent: this
	@# directory holds a ServiceMonitor, and `kubectl apply` on a cluster
	@# without kube-prometheus-stack fails with "no matches for kind
	@# ServiceMonitor" -- which would break applying relay for anyone who
	@# has not run `make monitoring-install`.
	@if kubectl get crd servicemonitors.monitoring.coreos.com >/dev/null 2>&1; then \
	  kubectl apply -k k8s/manifests/monitoring; \
	else \
	  echo "  skipping k8s/manifests/monitoring -- no ServiceMonitor CRD (run 'make monitoring-install')"; \
	fi
	@echo
	@echo "  relay and the sink read the compose Kafka and Postgres over"
	@echo "  host.minikube.internal, so 'make up' and 'make seed' first."
	@echo
	@echo "  BUT NOT 'make up-apps'. The compose and cluster delivery consumers"
	@echo "  join the SAME Kafka group and split the partitions between them, so"
	@echo "  half the events get delivered to whichever sink you are not looking"
	@echo "  at. Run one or the other:  docker compose --env-file $(ENV_FILE) \\"
	@echo "                               -f local/docker-compose.yml \\"
	@echo "                               stop relay-ingest relay-deliver sink" 

.PHONY: k8s-validate
k8s-validate: ## Assert manifest invariants (selector immutability, probes, endpoints)
	cd k8s/validate && go test -count=1 ./...
	./scripts/validate-k8s-schema.sh

AWS_RUN_ID          ?=
AWS_APPROVED_COMMIT ?=
AWS_RELAY_IMAGE     ?=
AWS_SINK_IMAGE      ?=
AWS_MSK_BOOTSTRAP   ?=
AWS_K8S_RENDER_DIR  ?= .evidence/m4/$(AWS_RUN_ID)/rendered
AWS_EVIDENCE_PHASE  ?= provisional
AWS_REDACTIONS_FILE ?=
MLP_AWS_LIVE_CONTROLLER_PID ?=
M4_OPERATOR         ?=
M4_LOCAL_RUN_ID     ?=
M4_SIGTERM_EVIDENCE ?= .evidence/m4-local/$(M4_LOCAL_RUN_ID)/k8s-sigterm.json
M4_ABORT_EVIDENCE   ?= .evidence/m4-local/$(M4_LOCAL_RUN_ID)/abort-rehearsal.json
M4_DEMO_EVIDENCE    ?= .evidence/m4-local/$(M4_LOCAL_RUN_ID)/demo-rehearsal.json
M4_SOURCE_COMMIT    ?= $(shell git rev-parse HEAD)

.PHONY: m4-images
m4-images: ## Build the relay and sink images from the exact M4 source commit
	docker build --build-arg "VERSION=$(M4_SOURCE_COMMIT)" -t relay:dev services/relay
	docker build --build-arg "VERSION=$(M4_SOURCE_COMMIT)" -t sink:dev services/sink

.PHONY: m4-aws-images
m4-aws-images: ## Build linux/amd64 M4 images for the t3.medium node group
	@test -n "$(AWS_APPROVED_COMMIT)" || { echo "AWS_APPROVED_COMMIT is required" >&2; exit 2; }
	docker build --platform linux/amd64 \
	  --build-arg "VERSION=$(AWS_APPROVED_COMMIT)" \
	  -t relay:m4-aws services/relay
	docker build --platform linux/amd64 \
	  --build-arg "VERSION=$(AWS_APPROVED_COMMIT)" \
	  -t sink:m4-aws services/sink

.PHONY: aws-preflight-check
aws-preflight-check: ## Check the account-independent M4 repository contract
	python3 scripts/m4-preflight.py check-repository
	python3 scripts/m4-evidence.py check-protocol

.PHONY: aws-preflight
aws-preflight: ## Run the complete local M4 preflight and write its receipt
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required (UTC YYYYMMDDTHHMMSSZ)" >&2; exit 2; }
	AWS_RUN_ID="$(AWS_RUN_ID)" python3 scripts/m4-preflight.py run
	AWS_RUN_ID="$(AWS_RUN_ID)" python3 scripts/m4-evidence.py init

.PHONY: aws-inventory-empty
aws-inventory-empty: ## Capture service-native AWS inventory and require no M4 runtime
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_APPROVED_COMMIT)" || { echo "AWS_APPROVED_COMMIT is required" >&2; exit 2; }
	$(AWS_REAL_ENV) python3 scripts/m4-aws-inventory.py \
	  --run-id "$(AWS_RUN_ID)" \
	  --commit "$(AWS_APPROVED_COMMIT)" \
	  --region "$(AWS_REAL_REGION)" \
	  --output "$(AWS_INVENTORY_FILE)" \
	  --require-no-runtime

.PHONY: aws-account-check
aws-account-check: ## Capture identity, backend, budget, quota, and availability gates
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_APPROVED_COMMIT)" || { echo "AWS_APPROVED_COMMIT is required" >&2; exit 2; }
	$(AWS_REAL_ENV) python3 scripts/m4-aws-account.py \
	  --run-id "$(AWS_RUN_ID)" \
	  --commit "$(AWS_APPROVED_COMMIT)" \
	  --profile "$(AWS_PROFILE_NAME)" \
	  --region "$(AWS_REAL_REGION)" \
	  --output "$(AWS_ACCOUNT_EVIDENCE)"

.PHONY: aws-price-template
aws-price-template: ## Write the private input for a current AWS price review
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_APPROVED_COMMIT)" || { echo "AWS_APPROVED_COMMIT is required" >&2; exit 2; }
	python3 scripts/m4-stage.py price-template \
	  --run-id "$(AWS_RUN_ID)" \
	  --commit "$(AWS_APPROVED_COMMIT)" \
	  --output "$(AWS_PRICE_INPUT)"

.PHONY: aws-prices
aws-prices: ## Validate reviewed AWS rates and write fixed-topology cost evidence
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_APPROVED_COMMIT)" || { echo "AWS_APPROVED_COMMIT is required" >&2; exit 2; }
	python3 scripts/m4-stage.py prices \
	  --run-id "$(AWS_RUN_ID)" \
	  --commit "$(AWS_APPROVED_COMMIT)" \
	  --region "$(AWS_REAL_REGION)" \
	  --input "$(AWS_PRICE_INPUT)" \
	  --output "$(AWS_PRICE_EVIDENCE)"

.PHONY: aws-stage-images
aws-stage-images: m4-aws-images ## Push missing commit images to ECR and record digests
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	$(AWS_REAL_ENV) python3 scripts/m4-stage.py stage-images \
	  --run-id "$(AWS_RUN_ID)" \
	  --commit "$(AWS_APPROVED_COMMIT)" \
	  --region "$(AWS_REAL_REGION)" \
	  --output "$(AWS_IMAGE_EVIDENCE)"

.PHONY: aws-inspect-images
aws-inspect-images: ## Recheck staged ECR tags, digests, provenance, and platform
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_APPROVED_COMMIT)" || { echo "AWS_APPROVED_COMMIT is required" >&2; exit 2; }
	$(AWS_REAL_ENV) python3 scripts/m4-stage.py images \
	  --run-id "$(AWS_RUN_ID)" \
	  --commit "$(AWS_APPROVED_COMMIT)" \
	  --region "$(AWS_REAL_REGION)" \
	  --output "$(AWS_IMAGE_EVIDENCE)"

.PHONY: aws-go-no-go
aws-go-no-go: ## Require one consistent staged packet before the paid window
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_APPROVED_COMMIT)" || { echo "AWS_APPROVED_COMMIT is required" >&2; exit 2; }
	@test -n "$(M4_OPERATOR)" || { echo "M4_OPERATOR is required" >&2; exit 2; }
	python3 scripts/m4-stage.py go-no-go \
	  --run-id "$(AWS_RUN_ID)" \
	  --commit "$(AWS_APPROVED_COMMIT)" \
	  --region "$(AWS_REAL_REGION)" \
	  --cleanup-owner "$(M4_OPERATOR)" \
	  --output "$(AWS_GO_NO_GO)"

.PHONY: aws-evidence-publish
aws-evidence-publish: ## Sanitize and verify the provisional or final M4 packet
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_REDACTIONS_FILE)" || { echo "AWS_REDACTIONS_FILE is required" >&2; exit 2; }
	python3 scripts/m4-evidence.py publish \
	  --run-id "$(AWS_RUN_ID)" \
	  --phase "$(AWS_EVIDENCE_PHASE)" \
	  --redactions-file "$(AWS_REDACTIONS_FILE)"

.PHONY: aws-evidence-verify
aws-evidence-verify: ## Recheck a published M4 evidence packet
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_REDACTIONS_FILE)" || { echo "AWS_REDACTIONS_FILE is required" >&2; exit 2; }
	python3 scripts/m4-evidence.py verify \
	  --run-id "$(AWS_RUN_ID)" \
	  --phase "$(AWS_EVIDENCE_PHASE)" \
	  --redactions-file "$(AWS_REDACTIONS_FILE)"

.PHONY: m4-k8s-sigterm
m4-k8s-sigterm: ## Terminate live minikube relay pods and record drain evidence
	@test -n "$(M4_LOCAL_RUN_ID)" || { echo "M4_LOCAL_RUN_ID is required" >&2; exit 2; }
	python3 scripts/verify-k8s-sigterm.py --output "$(M4_SIGTERM_EVIDENCE)"

.PHONY: m4-local-abort
m4-local-abort: ## Rehearse SIGTERM, destroy-first ordering, audit, and temp cleanup
	@test -n "$(M4_LOCAL_RUN_ID)" || { echo "M4_LOCAL_RUN_ID is required" >&2; exit 2; }
	python3 scripts/m4-local-abort.py rehearse --output "$(M4_ABORT_EVIDENCE)"

.PHONY: m4-local-demo
m4-local-demo: ## Run the machine-checked minikube demo and record local evidence
	@test -n "$(M4_LOCAL_RUN_ID)" || { echo "M4_LOCAL_RUN_ID is required" >&2; exit 2; }
	python3 scripts/m4-local-demo.py --output "$(M4_DEMO_EVIDENCE)"

.PHONY: aws-k8s-render
aws-k8s-render: ## Render the untracked AWS deployment bundle (no cluster mutation)
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_APPROVED_COMMIT)" || { echo "AWS_APPROVED_COMMIT is required" >&2; exit 2; }
	@test -n "$(AWS_RELAY_IMAGE)" || { echo "AWS_RELAY_IMAGE digest is required" >&2; exit 2; }
	@test -n "$(AWS_SINK_IMAGE)" || { echo "AWS_SINK_IMAGE digest is required" >&2; exit 2; }
	@test -n "$(AWS_MSK_BOOTSTRAP)" || { echo "AWS_MSK_BOOTSTRAP is required" >&2; exit 2; }
	mkdir -p "$(AWS_K8S_RENDER_DIR)"
	python3 scripts/render-aws-k8s.py application \
	  --commit "$(AWS_APPROVED_COMMIT)" \
	  --relay-image "$(AWS_RELAY_IMAGE)" \
	  --sink-image "$(AWS_SINK_IMAGE)" \
	  --run-id "$(AWS_RUN_ID)" \
	  --go-no-go "$(AWS_GO_NO_GO)" \
	  --msk-bootstrap "$(AWS_MSK_BOOTSTRAP)" \
	  --repo-url "$(REPO_URL)" >"$(AWS_K8S_RENDER_DIR)/root-app.json"
	python3 scripts/render-aws-k8s.py runtime \
	  --msk-bootstrap "$(AWS_MSK_BOOTSTRAP)" \
	  >"$(AWS_K8S_RENDER_DIR)/relay-runtime.json"
	python3 scripts/render-aws-k8s.py replay \
	  --commit "$(AWS_APPROVED_COMMIT)" \
	  --relay-image "$(AWS_RELAY_IMAGE)" \
	  --run-id "$(AWS_RUN_ID)" \
	  --go-no-go "$(AWS_GO_NO_GO)" \
	  >"$(AWS_K8S_RENDER_DIR)/relay-replay.json"
	helm template $(MONITORING_RELEASE) kube-prometheus-stack \
	  --repo https://prometheus-community.github.io/helm-charts \
	  --version $(KPS_VERSION) \
	  --namespace $(MONITORING_NS) \
	  --values k8s/monitoring-values.yaml \
	  --values k8s/monitoring-values-aws.yaml \
	  >"$(AWS_K8S_RENDER_DIR)/monitoring.yaml"
	@echo "rendered $(AWS_K8S_RENDER_DIR) (no cluster mutation)"

.PHONY: aws-runtime-bootstrap
aws-runtime-bootstrap: ## Initialize live MSK, RDS, and relay secrets inside the paid controller window
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_APPROVED_COMMIT)" || { echo "AWS_APPROVED_COMMIT is required" >&2; exit 2; }
	@echo "This mutates the active paid EKS, MSK, RDS, and Secrets Manager runtime."
	$(AWS_REAL_ENV) AWS_PAGER= MLP_USE_REAL_AWS=1 go -C tools/m4-bootstrap run . \
	  --root ../.. \
	  --run-id "$(AWS_RUN_ID)" \
	  --commit "$(AWS_APPROVED_COMMIT)" \
	  --region "$(AWS_REAL_REGION)"

.PHONY: aws-kubeconfig
aws-kubeconfig: ## Point kubectl at the live EKS cluster recorded in dev state
	@$(AWS_REAL_ENV) bash -c 'set -euo pipefail; \
	  cluster="$$(terraform -chdir=infra/terraform/envs/dev output -raw eks_cluster_name)"; \
	  aws eks update-kubeconfig --name "$$cluster" --region "$(AWS_REAL_REGION)"'

.PHONY: k8s-status
k8s-status: ## Show ArgoCD applications and the mlp namespace
	@kubectl get applications -n argocd 2>/dev/null || echo "ArgoCD not installed"
	@echo
	@kubectl get pods,svc -n mlp 2>/dev/null || echo "namespace mlp not present"

# ---------------------------------------------------------------------------
# Real AWS -- costs money. Read docs/costs.md first.
# ---------------------------------------------------------------------------

.PHONY: aws-login
aws-login: ## Refresh the AWS SSO session
	$(AWS_REAL_ENV) aws sso login

.PHONY: aws-whoami
aws-whoami: ## Show the active AWS identity
	$(AWS_REAL_ENV) aws sts get-caller-identity

.PHONY: aws-bootstrap
aws-bootstrap: aws-whoami ## Create the one-time S3 state backend
	@echo "This creates persistent remote-state resources in the selected AWS account."
	@read -p "Type 'yes' to continue: " ok && [ "$$ok" = "yes" ]
	cd infra/terraform/bootstrap && $(AWS_REAL_ENV) terraform init -input=false && \
	  $(AWS_REAL_ENV) terraform apply

.PHONY: aws-guardrails-init
aws-guardrails-init: aws-whoami ## Initialize the persistent AWS cost-guardrail stack
	@mlp_account_id="$$($(AWS_REAL_ENV) aws sts get-caller-identity \
	  --query Account --output text)"; \
	  cd infra/terraform/guardrails && \
	  $(AWS_REAL_ENV) terraform init -input=false $(AWS_INIT_ARGS) \
	    -backend-config="bucket=mlp-tfstate-$$mlp_account_id" \
	    -backend-config="key=guardrails/terraform.tfstate" \
	    -backend-config="region=$(AWS_REAL_REGION)" \
	    -backend-config="use_lockfile=true" \
	    -backend-config="encrypt=true"

.PHONY: aws-guardrails-plan
aws-guardrails-plan: ## Save an exact plan for the persistent $5 AWS budget
	@rm -f -- "$(AWS_GUARDRAILS_PLAN_FILE)"
	$(MAKE) aws-guardrails-init
	$(AWS_REAL_ENV) terraform -chdir=infra/terraform/guardrails plan \
	  -out="$(abspath $(AWS_GUARDRAILS_PLAN_FILE))"

.PHONY: aws-guardrails-up
aws-guardrails-up: aws-whoami ## Apply the reviewed persistent AWS budget plan
	@test -f "$(AWS_GUARDRAILS_PLAN_FILE)" || { \
	  echo "guardrail plan is missing; run 'make aws-guardrails-plan' first" >&2; exit 1; \
	}
	@echo "This creates or updates the persistent account-wide AWS cost alert."
	@read -p "Type 'yes' to continue: " ok && [ "$$ok" = "yes" ]
	@set -e; \
	  trap 'rm -f -- "$(AWS_GUARDRAILS_PLAN_FILE)"' EXIT; \
	  $(AWS_REAL_ENV) terraform -chdir=infra/terraform/guardrails apply \
	    "$(abspath $(AWS_GUARDRAILS_PLAN_FILE))"

.PHONY: aws-init
aws-init: aws-whoami ## Initialize the remote state backend for the dev environment
	@mlp_account_id="$$($(AWS_REAL_ENV) aws sts get-caller-identity \
	  --query Account --output text)"; \
	  cd infra/terraform/envs/dev && \
	  $(AWS_REAL_ENV) terraform init -input=false $(AWS_INIT_ARGS) \
	    -backend-config="bucket=mlp-tfstate-$$mlp_account_id" \
	    -backend-config="key=envs/dev/terraform.tfstate" \
	    -backend-config="region=$(AWS_REAL_REGION)" \
	    -backend-config="use_lockfile=true" \
	    -backend-config="encrypt=true"

.PHONY: aws-plan
aws-plan: ## Guard and save a reviewable Terraform plan for dev
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_APPROVED_COMMIT)" || { echo "AWS_APPROVED_COMMIT is required" >&2; exit 2; }
	$(MAKE) aws-init
	$(AWS_REAL_ENV) \
	  AWS_RUN_ID="$(AWS_RUN_ID)" \
	  MLP_AWS_APPROVED_COMMIT="$(AWS_APPROVED_COMMIT)" \
	  MLP_AWS_PLAN_FILE="$(AWS_PLAN_FILE)" \
	  MLP_AWS_PLAN_SUMMARY="$(AWS_PLAN_SUMMARY)" \
	  MLP_AWS_GO_NO_GO="$(AWS_GO_NO_GO)" \
	  ./scripts/aws-terraform-guard.sh plan $(AWS_TF_ARGS)

AWS_STATE_BACKUP ?= .terraform/mlp-last-known.tfstate

.PHONY: aws-state-backup
aws-state-backup: aws-whoami ## Save a private recovery copy of the current remote state
	@cd infra/terraform/envs/dev; \
	  set -e; \
	  umask 077; \
	  tmp="$(AWS_STATE_BACKUP).tmp"; \
	  $(AWS_REAL_ENV) terraform state pull > "$$tmp"; \
	  mv "$$tmp" "$(AWS_STATE_BACKUP)"; \
	  echo "saved infra/terraform/envs/dev/$(AWS_STATE_BACKUP)"

.PHONY: aws-up
aws-up: ## Apply the dev environment to real AWS (INCURS COST)
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_APPROVED_COMMIT)" || { echo "AWS_APPROVED_COMMIT is required" >&2; exit 2; }
	@echo "This applies the exact reviewed plan and may create billable AWS resources."
	@if [ "$(origin MLP_AWS_LIVE_CONTROLLER_PID)" = "command line" ] && \
	  python3 scripts/m4-evidence.py check-controller \
	    --run-id "$(AWS_RUN_ID)" \
	    --commit "$(AWS_APPROVED_COMMIT)" \
	    --controller-pid "$(MLP_AWS_LIVE_CONTROLLER_PID)" >/dev/null 2>&1; then \
	  :; \
	else \
	  read -p "Type 'yes' to continue: " ok && [ "$$ok" = "yes" ]; \
	fi
	$(MAKE) aws-init
	$(AWS_REAL_ENV) \
	  AWS_RUN_ID="$(AWS_RUN_ID)" \
	  MLP_AWS_APPROVED_COMMIT="$(AWS_APPROVED_COMMIT)" \
	  MLP_AWS_PLAN_FILE="$(AWS_PLAN_FILE)" \
	  MLP_AWS_PLAN_SUMMARY="$(AWS_PLAN_SUMMARY)" \
	  MLP_AWS_GO_NO_GO="$(AWS_GO_NO_GO)" \
	  MLP_AWS_LIVE_CONTROLLER_PID="$(MLP_AWS_LIVE_CONTROLLER_PID)" \
	  ./scripts/aws-terraform-guard.sh apply
	$(MAKE) aws-state-backup

.PHONY: aws-live-run
aws-live-run: ## Run one guarded live AWS session with timed cleanup
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	@test -n "$(AWS_APPROVED_COMMIT)" || { echo "AWS_APPROVED_COMMIT is required" >&2; exit 2; }
	@test -n "$(M4_OPERATOR)" || { echo "M4_OPERATOR is required" >&2; exit 2; }
	@echo "This authorizes the controller to apply the reviewed plan and run automatic cleanup."
	@echo "Keep this Mac powered, awake, open, and online until cleanup passes."
	@read -p "Type 'yes' to continue: " ok && [ "$$ok" = "yes" ]
	@mlp_live_command=(go -C tools/m4-live-run run . run \
	  --run-id "$(AWS_RUN_ID)" \
	  --commit "$(AWS_APPROVED_COMMIT)" \
	  --operator "$(M4_OPERATOR)" \
	  --profile "$(AWS_PROFILE_NAME)" \
	  --region "$(AWS_REAL_REGION)"); \
	  if command -v caffeinate >/dev/null 2>&1; then \
	    caffeinate -i "$${mlp_live_command[@]}"; \
	  else \
	    "$${mlp_live_command[@]}"; \
	  fi

.PHONY: aws-live-status
aws-live-status: ## Show the active live AWS deadline and cleanup state
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	go -C tools/m4-live-run run . status --run-id "$(AWS_RUN_ID)"

.PHONY: aws-live-stop
aws-live-stop: ## Stop evidence capture and ask the controller to clean up now
	@test -n "$(AWS_RUN_ID)" || { echo "AWS_RUN_ID is required" >&2; exit 2; }
	go -C tools/m4-live-run run . stop --run-id "$(AWS_RUN_ID)"

.PHONY: aws-live-rehearse
aws-live-rehearse: ## Run the local controller and abort-path tests without AWS
	cd tools/m4-live-run && go test -race ./...
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest \
	  scripts.tests.test_m4_evidence \
	  scripts.tests.test_m4_local_abort

.PHONY: aws-down
aws-down: aws-whoami ## Destroy dev using its already initialized backend
	@test -f infra/terraform/envs/dev/.terraform/terraform.tfstate || { \
	  echo "backend is not initialized; run 'make aws-init' first" >&2; exit 1; \
	}
	cd infra/terraform/envs/dev && $(AWS_REAL_ENV) terraform destroy $(AWS_DESTROY_ARGS)
	$(MAKE) aws-state-backup

.PHONY: aws-state-empty
aws-state-empty: ## Require the initialized dev Terraform state to contain no resources
	@test -f infra/terraform/envs/dev/.terraform/terraform.tfstate || { \
	  echo "backend is not initialized; run 'make aws-init' first" >&2; exit 1; \
	}
	@set -e; \
	  resources="$$( $(AWS_REAL_ENV) terraform -chdir=infra/terraform/envs/dev state list )"; \
	  if [ -n "$$resources" ]; then \
	    echo "dev Terraform state still contains resources:" >&2; \
	    printf '%s\n' "$$resources" >&2; \
	    exit 1; \
	  fi; \
	  echo "dev Terraform state is empty"

.PHONY: aws-cost
aws-cost: ## Month-to-date spend on the account
	@$(AWS_REAL_ENV) aws ce get-cost-and-usage \
	  --time-period Start=$$(date -u +%Y-%m-01),End=$$(date -u -v+1d +%Y-%m-%d) \
	  --granularity MONTHLY --metrics UnblendedCost \
	  --query 'ResultsByTime[0].Total.UnblendedCost' --output table

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
