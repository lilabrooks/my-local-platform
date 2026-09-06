#!/usr/bin/env bash
# Guard and execute the reviewed Terraform plan for the M4 AWS runtime.
set -euo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
TF_DIR="$ROOT_DIR/infra/terraform/envs/dev"
PLAN_INPUT=${MLP_AWS_PLAN_FILE:-infra/terraform/envs/dev/.terraform/mlp-reviewed.tfplan}
SUMMARY_INPUT=${MLP_AWS_PLAN_SUMMARY:-infra/terraform/envs/dev/.terraform/mlp-plan-summary.json}

case "$PLAN_INPUT" in
	/*) PLAN_FILE=$PLAN_INPUT ;;
	*) PLAN_FILE="$ROOT_DIR/$PLAN_INPUT" ;;
esac
case "$SUMMARY_INPUT" in
	/*) SUMMARY_FILE=$SUMMARY_INPUT ;;
	*) SUMMARY_FILE="$ROOT_DIR/$SUMMARY_INPUT" ;;
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
	local notification subscriber_count has_subscriber=false
	account_id=$(aws sts get-caller-identity --query Account --output text)
	if ! limit=$(aws budgets describe-budget \
		--account-id "$account_id" \
		--budget-name "$budget_name" \
		--query 'Budget.BudgetLimit.Amount' \
		--output text 2>/dev/null); then
		echo "hourly resources require the pre-existing $budget_name budget" >&2
		echo "apply the cheap tier with budget_alert_email set, then plan again" >&2
		exit 1
	fi
	if ! awk -v value="$limit" 'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]+)?$/) }'; then
		echo "$budget_name returned an invalid budget limit" >&2
		exit 1
	fi
	awk -v limit="$limit" 'BEGIN { exit !(limit <= 5) }' || {
		echo "$budget_name has a limit above the approved \$5 maximum" >&2
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
		if [ "$subscriber_count" -ge 1 ]; then
			has_subscriber=true
			break
		fi
	done < <(jq -c \
		'.[] | {NotificationType, ComparisonOperator, Threshold, ThresholdType}' \
		<<<"$notifications_json")
	if [ "$has_subscriber" != "true" ]; then
		echo "$budget_name has no notification subscriber; hourly resources remain blocked" >&2
		exit 1
	fi
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

require_command aws
require_command jq
require_command python3
require_command terraform
export AWS_PAGER=""
umask 077

action=${1:-}
[ -n "$action" ] || usage
shift

case "$action" in
	plan)
		# A failed re-plan must not leave an older reviewed pair available to apply.
		rm -f -- "$PLAN_FILE" "$SUMMARY_FILE"
		console_args=()
		expect_value=
		for argument in "$@"; do
			if [ -n "$expect_value" ]; then
				console_args+=("$expect_value" "$argument")
				expect_value=
				continue
			fi
			case "$argument" in
				-var | -var-file) expect_value=$argument ;;
				-var=* | -var-file=*) console_args+=("$argument") ;;
				-out | -out=*)
					echo "the guarded plan owns -out; set MLP_AWS_PLAN_FILE instead" >&2
					exit 2
					;;
			esac
		done
		[ -z "$expect_value" ] || usage

		encoded_runtime=$(printf '%s\n' \
			'jsonencode({ hourly_enabled = local.hourly_enabled, enable_eks = var.enable_eks, budget_name = local.runtime_budget_name, region = var.region, eks_version = var.eks_kubernetes_version })' |
			terraform -chdir="$TF_DIR" console ${console_args[@]+"${console_args[@]}"})
		runtime_json=$(jq -r . <<<"$encoded_runtime")
		guard_shape "$runtime_json"

		mkdir -p "$(dirname -- "$PLAN_FILE")" "$(dirname -- "$SUMMARY_FILE")"
		terraform -chdir="$TF_DIR" plan -out="$PLAN_FILE" "$@"
		python3 "$ROOT_DIR/scripts/check-aws-plan.py" \
			--terraform-directory "$TF_DIR" "$PLAN_FILE" "$SUMMARY_FILE"
		;;
	apply)
		[ "$#" -eq 0 ] || usage
		[ -f "$PLAN_FILE" ] || {
			echo "reviewed plan not found: $PLAN_FILE; run make aws-plan first" >&2
			exit 1
		}
		[ -f "$SUMMARY_FILE" ] || {
			echo "reviewed summary not found: $SUMMARY_FILE; run make aws-plan first" >&2
			exit 1
		}
		verify_summary=$(mktemp "${SUMMARY_FILE}.verify.XXXXXX")
		trap 'rm -f "$verify_summary"' EXIT
		python3 "$ROOT_DIR/scripts/check-aws-plan.py" \
			--terraform-directory "$TF_DIR" "$PLAN_FILE" "$verify_summary"
		if ! cmp -s "$SUMMARY_FILE" "$verify_summary"; then
			echo "the plan or summary changed after review; refusing apply" >&2
			exit 1
		fi
		runtime_json=$(jq -c '{hourly_enabled: .shape.hourly_enabled, enable_eks: .shape.enable_eks, budget_name: .budget_name, region: .shape.region, eks_version: .shape.eks.kubernetes_version}' "$SUMMARY_FILE")
		plan_sha=$(jq -r '.plan_sha256' "$SUMMARY_FILE")
		guard_shape "$runtime_json"
		current_sha=$(shasum -a 256 "$PLAN_FILE" | awk '{print $1}')
		if [ "$current_sha" != "$plan_sha" ]; then
			echo "reviewed plan changed during preflight; refusing apply" >&2
			exit 1
		fi
		terraform -chdir="$TF_DIR" apply "$PLAN_FILE"
		;;
	*) usage ;;
esac
