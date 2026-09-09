#!/usr/bin/env bash
# Guard and execute the reviewed Terraform plan for the M4 AWS runtime.
set -euo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
TF_DIR="$ROOT_DIR/infra/terraform/envs/dev"
PLAN_INPUT=${MLP_AWS_PLAN_FILE:-infra/terraform/envs/dev/.terraform/mlp-reviewed.tfplan}
SUMMARY_INPUT=${MLP_AWS_PLAN_SUMMARY:-.evidence/m4/${AWS_RUN_ID:-missing}/03-plan-summary.json}
GO_NO_GO_INPUT=${MLP_AWS_GO_NO_GO:-.evidence/m4/${AWS_RUN_ID:-missing}/06-go-no-go.json}

case "$PLAN_INPUT" in
	/*) PLAN_FILE=$PLAN_INPUT ;;
	*) PLAN_FILE="$ROOT_DIR/$PLAN_INPUT" ;;
esac
case "$SUMMARY_INPUT" in
	/*) SUMMARY_FILE=$SUMMARY_INPUT ;;
	*) SUMMARY_FILE="$ROOT_DIR/$SUMMARY_INPUT" ;;
esac
case "$GO_NO_GO_INPUT" in
	/*) GO_NO_GO_FILE=$GO_NO_GO_INPUT ;;
	*) GO_NO_GO_FILE="$ROOT_DIR/$GO_NO_GO_INPUT" ;;
esac

usage() {
	echo "usage: $0 plan [terraform plan arguments] | apply" >&2
	exit 2
}

require_command() {
	command -v "$1" >/dev/null 2>&1 || {
		echo "$1 is required" >&2
		exit 1
	}
}

check_budget() {
	local account_id budget_name=$1 limit notifications_json notification_count
	local notification subscriber_count
	account_id=$(aws sts get-caller-identity --query Account --output text)
	if ! limit=$(aws budgets describe-budget \
		--account-id "$account_id" \
		--budget-name "$budget_name" \
		--query 'Budget.BudgetLimit.Amount' \
		--output text 2>/dev/null); then
		echo "hourly resources require the pre-existing $budget_name budget" >&2
		echo "apply the persistent guardrail stack, then plan again" >&2
		exit 1
	fi
	if ! awk -v value="$limit" 'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]+)?$/) }'; then
		echo "$budget_name returned an invalid budget limit" >&2
		exit 1
	fi
	awk -v limit="$limit" 'BEGIN { exit !(limit == 5) }' || {
		echo "$budget_name must have the approved \$5 limit" >&2
		exit 1
	}
	notifications_json=$(aws budgets describe-notifications-for-budget \
		--account-id "$account_id" \
		--budget-name "$budget_name" \
		--query 'Notifications' \
		--output json)
	if ! notification_count=$(jq -er \
		'if type == "array" then length else error("notifications must be an array") end' \
		<<<"$notifications_json"); then
		echo "$budget_name returned an invalid notification list" >&2
		exit 1
	fi
	if [ "$notification_count" -lt 1 ]; then
		echo "$budget_name has no notification; hourly resources remain blocked" >&2
		exit 1
	fi
	if ! jq -e '
		(map([.NotificationType, .ComparisonOperator, .Threshold, .ThresholdType]) | sort)
		==
		([ ["ACTUAL", "GREATER_THAN", 80, "PERCENTAGE"],
		   ["ACTUAL", "GREATER_THAN", 100, "PERCENTAGE"],
		   ["FORECASTED", "GREATER_THAN", 100, "PERCENTAGE"] ] | sort)
	' <<<"$notifications_json" >/dev/null; then
		echo "$budget_name notification settings do not match the guardrail stack" >&2
		exit 1
	fi
	if ! jq -e 'all(.[]; .NotificationState == "OK")' \
		<<<"$notifications_json" >/dev/null; then
		echo "$budget_name notifications are not all OK; hourly resources remain blocked" >&2
		exit 1
	fi

	while IFS= read -r notification; do
		subscriber_count=$(aws budgets describe-subscribers-for-notification \
			--account-id "$account_id" \
			--budget-name "$budget_name" \
			--notification "$notification" \
			--query 'length(Subscribers)' \
			--output text)
		case "$subscriber_count" in
			'' | *[!0-9]*)
				echo "$budget_name returned an invalid subscriber count" >&2
				exit 1
				;;
		esac
		if [ "$subscriber_count" -lt 1 ]; then
			echo "$budget_name has a notification without a subscriber; hourly resources remain blocked" >&2
			exit 1
		fi
	done < <(jq -c \
		'.[] | {NotificationType, ComparisonOperator, Threshold, ThresholdType}' \
		<<<"$notifications_json")
}

check_eks_support() {
	local region=$1 version=$2 count
	count=$(aws eks describe-cluster-versions \
		--region "$region" \
		--cluster-versions "$version" \
		--version-status STANDARD_SUPPORT \
		--query 'length(clusterVersions)' \
		--output text)
	if [ "$count" != "1" ]; then
		echo "EKS Kubernetes $version is not in STANDARD_SUPPORT in $region" >&2
		exit 1
	fi
}

guard_shape() {
	local runtime_json=$1 hourly_enabled enable_eks budget_name region version
	hourly_enabled=$(jq -r '.hourly_enabled' <<<"$runtime_json")
	enable_eks=$(jq -r '.enable_eks' <<<"$runtime_json")
	budget_name=$(jq -r '.budget_name' <<<"$runtime_json")
	region=$(jq -r '.region' <<<"$runtime_json")
	version=$(jq -r '.eks_version' <<<"$runtime_json")
	case "$hourly_enabled" in
		true | false) ;;
		*)
			echo "runtime shape has an invalid hourly_enabled value" >&2
			exit 1
			;;
	esac
	case "$enable_eks" in
		true | false) ;;
		*)
			echo "runtime shape has an invalid enable_eks value" >&2
			exit 1
			;;
	esac

	if [ "$hourly_enabled" = "true" ]; then
		check_budget "$budget_name"
	fi
	if [ "$enable_eks" = "true" ]; then
		# Keep this last: the Terraform command follows immediately.
		check_eks_support "$region" "$version"
	fi
}

check_source() {
	local head dirty
	SOURCE_COMMIT=${MLP_AWS_APPROVED_COMMIT:-}
	RUN_ID=${AWS_RUN_ID:-}
	if [[ ! $SOURCE_COMMIT =~ ^[0-9a-f]{40}$ ]]; then
		echo "MLP_AWS_APPROVED_COMMIT must be a full lowercase git SHA" >&2
		exit 1
	fi
	if [[ ! $RUN_ID =~ ^[0-9]{8}T[0-9]{6}Z$ ]]; then
		echo "AWS_RUN_ID must use UTC YYYYMMDDTHHMMSSZ" >&2
		exit 1
	fi
	head=$(git -C "$ROOT_DIR" rev-parse HEAD)
	if [ "$head" != "$SOURCE_COMMIT" ]; then
		echo "HEAD $head does not match approved commit $SOURCE_COMMIT" >&2
		exit 1
	fi
	dirty=$(git -C "$ROOT_DIR" status --porcelain --untracked-files=all)
	if [ -n "$dirty" ]; then
		echo "worktree must be clean before creating or applying a reviewed plan" >&2
		exit 1
	fi
}

reject_terraform_cli_environment() {
	local name
	while IFS='=' read -r name _; do
		case "$name" in
			TF_CLI_ARGS | TF_CLI_ARGS_*)
				echo "$name is not accepted by the guarded Terraform workflow" >&2
				exit 1
				;;
		esac
	done < <(env)
}

validate_artifact_paths() {
	local expected_plan expected_summary expected_go_no_go path current
	expected_plan="$TF_DIR/.terraform/mlp-reviewed.tfplan"
	expected_summary="$ROOT_DIR/.evidence/m4/$RUN_ID/03-plan-summary.json"
	expected_go_no_go="$ROOT_DIR/.evidence/m4/$RUN_ID/06-go-no-go.json"

	[ "$PLAN_FILE" = "$expected_plan" ] || {
		echo "reviewed plan must be $expected_plan" >&2
		exit 2
	}
	[ "$SUMMARY_FILE" = "$expected_summary" ] || {
		echo "plan summary must be $expected_summary" >&2
		exit 2
	}
	[ "$GO_NO_GO_FILE" = "$expected_go_no_go" ] || {
		echo "GO packet must be $expected_go_no_go" >&2
		exit 2
	}

	for path in "$PLAN_FILE" "$SUMMARY_FILE" "$GO_NO_GO_FILE"; do
		current=$path
		while [ "$current" != "$ROOT_DIR" ]; do
			[ ! -L "$current" ] || {
				echo "artifact path contains a symlink: $current" >&2
				exit 2
			}
			current=$(dirname -- "$current")
		done
	done
}

require_command aws
require_command git
require_command jq
require_command python3
require_command terraform
export AWS_PAGER=""
umask 077

action=${1:-}
[ -n "$action" ] || usage
shift
reject_terraform_cli_environment

case "$action" in
	plan)
		check_source
		validate_artifact_paths
		# A failed re-plan must not leave an older reviewed pair available to apply.
		rm -f -- "$PLAN_FILE" "$SUMMARY_FILE" "$GO_NO_GO_FILE"
		console_args=()
		input_files=()
		expect_value=
		for argument in "$@"; do
			if [ -n "$expect_value" ]; then
				console_args+=("$expect_value" "$argument")
				case "$expect_value" in
					-var-file | --var-file) input_files+=("$argument") ;;
				esac
				expect_value=
				continue
			fi
			case "$argument" in
				-var | --var | -var-file | --var-file) expect_value=$argument ;;
				-var=* | --var=* | -var-file=* | --var-file=*)
					console_args+=("$argument")
					case "$argument" in
						-var-file=* | --var-file=*) input_files+=("${argument#*=}") ;;
					esac
					;;
				-out | --out | -out=* | --out=*)
					echo "the guarded plan owns -out; set MLP_AWS_PLAN_FILE instead" >&2
					exit 2
					;;
			esac
		done
		[ -z "$expect_value" ] || usage
		for candidate in \
			"$TF_DIR/terraform.tfvars" \
			"$TF_DIR/terraform.tfvars.json" \
			"$TF_DIR"/*.auto.tfvars \
			"$TF_DIR"/*.auto.tfvars.json; do
			[ -f "$candidate" ] && input_files+=("$candidate")
		done
		terraform_input_sha=$(
			{
				printf 'arguments\0'
				printf '%s\0' "$@"
				for input_file in ${input_files[@]+"${input_files[@]}"}; do
					if [[ $input_file = /* ]]; then
						input_path=$input_file
					else
						input_path="$TF_DIR/$input_file"
					fi
					if [ ! -f "$input_path" ]; then
						echo "Terraform variable file not found: $input_file" >&2
						exit 1
					fi
					printf 'file\0%s\0' "$input_file"
					shasum -a 256 "$input_path" | awk '{print $1}'
				done
			} | shasum -a 256 | awk '{print $1}'
		)

		encoded_runtime=$(printf '%s\n' \
			'jsonencode({ hourly_enabled = local.hourly_enabled, enable_eks = var.enable_eks, budget_name = local.runtime_budget_name, region = var.region, eks_version = var.eks_kubernetes_version })' |
			terraform -chdir="$TF_DIR" console ${console_args[@]+"${console_args[@]}"})
		runtime_json=$(jq -r . <<<"$encoded_runtime")
		guard_shape "$runtime_json"

		mkdir -p "$(dirname -- "$PLAN_FILE")" "$(dirname -- "$SUMMARY_FILE")"
		terraform -chdir="$TF_DIR" plan -out="$PLAN_FILE" "$@"
		python3 "$ROOT_DIR/scripts/check-aws-plan.py" \
			--terraform-directory "$TF_DIR" \
			--run-id "$RUN_ID" \
			--source-commit "$SOURCE_COMMIT" \
			--terraform-input-sha256 "$terraform_input_sha" \
			"$PLAN_FILE" "$SUMMARY_FILE"
		;;
	apply)
		check_source
		validate_artifact_paths
		[ "$#" -eq 0 ] || usage
		[ -f "$PLAN_FILE" ] && [ ! -L "$PLAN_FILE" ] || {
			echo "reviewed plan not found: $PLAN_FILE; run make aws-plan first" >&2
			exit 1
		}
		[ -f "$SUMMARY_FILE" ] && [ ! -L "$SUMMARY_FILE" ] || {
			echo "reviewed summary not found: $SUMMARY_FILE; run make aws-plan first" >&2
			exit 1
		}
		verify_summary=$(mktemp "${SUMMARY_FILE}.verify.XXXXXX")
		trap 'rm -f "$verify_summary"' EXIT
		terraform_input_sha=$(jq -er '.terraform_input_sha256 | select(test("^[0-9a-f]{64}$"))' "$SUMMARY_FILE")
		python3 "$ROOT_DIR/scripts/check-aws-plan.py" \
			--terraform-directory "$TF_DIR" \
			--run-id "$RUN_ID" \
			--source-commit "$SOURCE_COMMIT" \
			--terraform-input-sha256 "$terraform_input_sha" \
			--verification-output \
			"$PLAN_FILE" "$verify_summary"
		if ! cmp -s "$SUMMARY_FILE" "$verify_summary"; then
			echo "the plan or summary changed after review; refusing apply" >&2
			exit 1
		fi
		runtime_json=$(jq -c '{hourly_enabled: .shape.hourly_enabled, enable_eks: .shape.enable_eks, budget_name: .budget_name, region: .shape.region, eks_version: .shape.eks.kubernetes_version}' "$SUMMARY_FILE")
		plan_sha=$(jq -r '.plan_sha256' "$SUMMARY_FILE")
		current_sha=$(shasum -a 256 "$PLAN_FILE" | awk '{print $1}')
		if [ "$current_sha" != "$plan_sha" ]; then
			echo "reviewed plan changed during preflight; refusing apply" >&2
			exit 1
		fi
		if [ "$(jq -r '.shape.hourly_enabled' "$SUMMARY_FILE")" = "true" ]; then
			[ -f "$GO_NO_GO_FILE" ] && [ ! -L "$GO_NO_GO_FILE" ] || {
				echo "GO packet not found: $GO_NO_GO_FILE; run make aws-go-no-go first" >&2
				exit 1
			}
			python3 "$ROOT_DIR/scripts/m4-stage.py" verify-go-no-go \
				--run-id "$RUN_ID" \
				--commit "$SOURCE_COMMIT" \
				--region "$(jq -r '.shape.region' "$SUMMARY_FILE")" \
				--plan "$PLAN_FILE" \
				--summary "$SUMMARY_FILE" \
				--output "$GO_NO_GO_FILE"
			controller_pid=${MLP_AWS_LIVE_CONTROLLER_PID:-}
			case "$controller_pid" in
				'' | *[!0-9]*)
					echo "hourly apply must run under make aws-live-run" >&2
					exit 1
					;;
			esac
			python3 "$ROOT_DIR/scripts/m4-evidence.py" check-controller \
				--run-id "$RUN_ID" \
				--commit "$SOURCE_COMMIT" \
				--controller-pid "$controller_pid"
		fi
		# Keep this last: its EKS support query must immediately precede apply.
		guard_shape "$runtime_json"
		terraform -chdir="$TF_DIR" apply "$PLAN_FILE"
		;;
	*) usage ;;
esac
