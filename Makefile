# my-local-platform
#
# Local-first: everything runs in Docker for free. Real AWS is opt-in and
# ephemeral -- see the `aws-*` targets and docs/costs.md before applying.

SHELL := /bin/bash
COMPOSE := docker compose -f local/docker-compose.yml
AWS_PROFILE_NAME ?= aws-public-change-feed

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Local stack
# ---------------------------------------------------------------------------

.PHONY: up
up: ## Start the whole local stack (~1GB idle across 9 containers)
	$(COMPOSE) --profile all up -d
	$(MAKE) seed

.PHONY: up-core
up-core: ## Start floci (AWS surface) + postgres only
	$(COMPOSE) --profile core up -d
	./local/bootstrap/seed.sh

.PHONY: up-messaging
up-messaging: ## Start Kafka, Kafka UI and RabbitMQ
	$(COMPOSE) --profile messaging up -d
	./local/bootstrap/kafka-topics.sh

.PHONY: up-obs
up-obs: ## Start OTel collector, Prometheus, Tempo, Grafana
	$(COMPOSE) --profile obs up -d

.PHONY: seed
seed: ## Create local AWS resources and Kafka topics (idempotent)
	./local/bootstrap/seed.sh
	./local/bootstrap/kafka-topics.sh

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
	@echo "Grafana         http://localhost:3000    (anonymous admin)"

# ---------------------------------------------------------------------------
# Smoke service
# ---------------------------------------------------------------------------

.PHONY: smoke
smoke: ## Run the end-to-end smoke check against the local stack
	cd services/smoke && go run ./cmd/smoke

.PHONY: test
test: ## Run Go tests
	cd services/smoke && go test ./...

.PHONY: tidy
tidy: ## go mod tidy
	cd services/smoke && go mod tidy

.PHONY: fmt
fmt: ## Format Go code
	cd services/smoke && go fmt ./...

.PHONY: vet
vet: ## go vet
	cd services/smoke && go vet ./...

# ---------------------------------------------------------------------------
# Real AWS -- costs money. Read docs/costs.md first.
# ---------------------------------------------------------------------------

.PHONY: aws-login
aws-login: ## Refresh the AWS SSO session
	aws sso login --profile $(AWS_PROFILE_NAME)

.PHONY: aws-whoami
aws-whoami: ## Show the active AWS identity
	aws sts get-caller-identity --profile $(AWS_PROFILE_NAME)

.PHONY: aws-plan
aws-plan: ## Terraform plan for the dev environment (free, read-only)
	cd infra/terraform/envs/dev && terraform init -input=false && \
	  AWS_PROFILE=$(AWS_PROFILE_NAME) terraform plan

.PHONY: aws-up
aws-up: ## Apply the dev environment to real AWS (INCURS COST)
	@echo "This creates real, billable AWS resources. See docs/costs.md."
	@read -p "Type 'yes' to continue: " ok && [ "$$ok" = "yes" ]
	cd infra/terraform/envs/dev && terraform init -input=false && \
	  AWS_PROFILE=$(AWS_PROFILE_NAME) terraform apply

.PHONY: aws-down
aws-down: ## Destroy the dev environment (do this when you stop working)
	cd infra/terraform/envs/dev && \
	  AWS_PROFILE=$(AWS_PROFILE_NAME) terraform destroy

.PHONY: aws-cost
aws-cost: ## Month-to-date spend on the account
	@AWS_PROFILE=$(AWS_PROFILE_NAME) aws ce get-cost-and-usage \
	  --time-period Start=$$(date -u +%Y-%m-01),End=$$(date -u -v+1d +%Y-%m-%d) \
	  --granularity MONTHLY --metrics UnblendedCost \
	  --query 'ResultsByTime[0].Total.UnblendedCost' --output table

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
