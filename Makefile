# my-local-platform
#
# Local-first: everything runs in Docker for free. Real AWS is opt-in and
# ephemeral -- see the `aws-*` targets and docs/costs.md before applying.

SHELL := /bin/bash
COMPOSE := docker compose -f local/docker-compose.yml

# Docker Compose reads .env itself. Include the same file as Make variables so
# settings used by recipes, such as AWS_PROFILE_NAME, do not silently diverge.
# Recipes do not inherit these values unless they explicitly source or export
# them; that keeps local emulator credentials away from real-AWS commands.
ifneq (,$(wildcard .env))
include .env
endif

AWS_PROFILE_NAME ?= aws-public-change-feed
AWS_DEFAULT_REGION ?= us-east-1
AWS_INIT_ARGS ?= -input=false
AWS_REAL_ENV := env \
	-u AWS_ENDPOINT_URL \
	-u AWS_ENDPOINT_URL_DYNAMODB \
	-u AWS_ENDPOINT_URL_S3 \
	-u AWS_ENDPOINT_URL_STS \
	-u AWS_ACCESS_KEY_ID \
	-u AWS_SECRET_ACCESS_KEY \
	-u AWS_SESSION_TOKEN \
	-u AWS_DEFAULT_PROFILE \
	-u AWS_ROLE_ARN \
	-u AWS_WEB_IDENTITY_TOKEN_FILE \
	AWS_PROFILE="$(AWS_PROFILE_NAME)"

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
	./local/bootstrap/seed.sh

.PHONY: up-core-containers
up-core-containers: ## Start core WITH the docker socket (needed for floci RDS/EKS/Lambda)
	@echo "Granting floci the docker socket: effective root on this host."
	@echo "Only needed for floci's container-backed services. See ADR 0002."
	$(COMPOSE) -f local/docker-compose.floci-containers.yml --profile core up -d
	./local/bootstrap/seed.sh

.PHONY: up-messaging
up-messaging: ## Start Kafka and RabbitMQ (~660MB)
	$(COMPOSE) --profile messaging up -d
	./local/bootstrap/kafka-topics.sh

.PHONY: up-tools
up-tools: ## Add Kafka UI (~285MB; needs the messaging profile)
	$(COMPOSE) --profile messaging --profile tools up -d

.PHONY: up-apps
up-apps: ## Start relay and sink (built from source; brings core and messaging too)
	docker compose -f local/docker-compose.yml \
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
seed: export RELAY_SIGNING_SECRET := $(RELAY_SIGNING_SECRET)
seed: ## Create local AWS resources and Kafka topics (idempotent)
	./local/bootstrap/seed.sh
	./local/bootstrap/kafka-topics.sh
	./local/bootstrap/relay-db.sh

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

.PHONY: smoke
smoke: ## Run the end-to-end smoke check against the local stack
	@if [ -f .env ]; then set -a; source ./.env; set +a; fi; \
	  cd services/smoke && go run ./cmd/smoke

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
# topic partition counts out of local/bootstrap/kafka-topics.sh.
UNCACHED_MODULES := k8s/validate

.PHONY: test
test: ## Run Go tests across every module
	@for m in $(GO_MODULES); do \
	  flags=""; \
	  for u in $(UNCACHED_MODULES); do \
	    if [ "$$m" = "$$u" ]; then flags="-count=1"; fi; \
	  done; \
	  echo "==> $$m: go test $$flags ./..."; \
	  ( cd "$$m" && go test $$flags ./... ) || exit 1; \
	done

.PHONY: tidy
tidy: ## go mod tidy in every module
	@for m in $(GO_MODULES); do \
	  echo "==> $$m: go mod tidy"; \
	  ( cd "$$m" && go mod tidy ) || exit 1; \
	done

.PHONY: lint
lint: ## Run every linter (yaml, shell, markdown, actions, docker, terraform, secrets)
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
REPO_URL         ?= https://github.com/lilabrooks/my-local-platform

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
	  --wait --timeout 10m
	@echo
	@echo "  next:  make monitoring-ready   (asserts the demo's panel will have data)"
	@echo "         make monitoring-ui      (Grafana on :3001, not 3000)"

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
	@echo "  Anonymous access comes from k8s/monitoring-values.yaml, matching the"
	@echo "  compose Grafana. The login form still works for admin-only pages:"
	@echo "  user admin, password from: make monitoring-password"
	@echo "  (NOT prom-operator -- this chart version generates a random one.)"
	@echo
	kubectl -n $(MONITORING_NS) port-forward svc/$(MONITORING_RELEASE)-grafana 3001:80

.PHONY: argocd-install
argocd-install: ## Install ArgoCD and register the app-of-apps
	REPO_URL=$(REPO_URL) ./k8s/argocd/install.sh

.PHONY: argocd-repo-creds
argocd-repo-creds: ## Give ArgoCD read access to this private repo (deploy key)
	./k8s/argocd/repo-creds.sh

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
	@echo "  at. Run one or the other:  docker compose -f local/docker-compose.yml \\"
	@echo "                               stop relay-ingest relay-deliver sink" 

.PHONY: k8s-validate
k8s-validate: ## Assert manifest invariants (selector immutability, probes, endpoints)
	cd k8s/validate && go test -count=1 ./...

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
aws-bootstrap: aws-whoami ## Create the one-time S3 state backend and lock table
	@echo "This creates persistent remote-state resources in the selected AWS account."
	@read -p "Type 'yes' to continue: " ok && [ "$$ok" = "yes" ]
	cd infra/terraform/bootstrap && $(AWS_REAL_ENV) terraform init -input=false && \
	  $(AWS_REAL_ENV) terraform apply

.PHONY: aws-init
aws-init: aws-whoami ## Initialize the remote state backend for the dev environment
	@mlp_account_id="$$($(AWS_REAL_ENV) aws sts get-caller-identity \
	  --query Account --output text)"; \
	  cd infra/terraform/envs/dev && \
	  $(AWS_REAL_ENV) terraform init $(AWS_INIT_ARGS) \
	    -backend-config="bucket=mlp-tfstate-$$mlp_account_id" \
	    -backend-config="key=envs/dev/terraform.tfstate" \
	    -backend-config="region=$(AWS_DEFAULT_REGION)" \
	    -backend-config="dynamodb_table=mlp-tfstate-lock" \
	    -backend-config="encrypt=true"

.PHONY: aws-plan
aws-plan: aws-init ## Terraform plan for the dev environment (free, read-only)
	cd infra/terraform/envs/dev && $(AWS_REAL_ENV) terraform plan

.PHONY: aws-up
aws-up: aws-init ## Apply the dev environment to real AWS (INCURS COST)
	@echo "This creates real, billable AWS resources. See docs/costs.md."
	@read -p "Type 'yes' to continue: " ok && [ "$$ok" = "yes" ]
	cd infra/terraform/envs/dev && $(AWS_REAL_ENV) terraform apply

.PHONY: aws-down
aws-down: aws-init ## Destroy the dev environment (do this when you stop working)
	cd infra/terraform/envs/dev && $(AWS_REAL_ENV) terraform destroy

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
