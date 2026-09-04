package validate

import (
	"fmt"
	"testing"
	"time"

	"github.com/lilabrooks/my-local-platform/relay/config"
)

// On SIGTERM a delivery consumer stops fetching, fails readiness, and drains the
// record already in hand. Consumer.Run caps that complete work at
// DefaultStallBudget. If Kubernetes SIGKILLs the pod before the cap expires, the
// offset is uncommitted and the record is redelivered.
//
// Redelivery is correct -- at-least-once is the stated contract -- but it means
// every scale-down manufactures duplicate deliveries, and scale-down is half of
// what M2 demonstrates. KEDA removing a pod mid-delivery should be a drain, not
// a dropped record.
//
// The settings that have to agree live in different files and look unrelated.
// RELAY_RETRY_DELAYS and RELAY_DELIVERY_TIMEOUT are in a ConfigMap; the record
// deadline is in relay's config package; terminationGracePeriodSeconds is in a
// Deployment. These assertions keep the chain in step.
//
// The worst case is computed with relay's own config package rather than a copy
// of the preset table, so changing what "demo" means cannot leave this
// assertion passing against numbers nothing uses.
//
// The schedule assertion prevents a manifest from deploying straight to a
// crashloop. The grace assertion catches a shorter grace period. The second test
// below catches a record deadline raised past that grace period. Together they
// prove schedule worst case < record deadline < termination grace period.
func TestDeliveryScheduleFitsInsideRecordDeadlineAndGracePeriod(t *testing.T) {
	checked := 0

	for _, dir := range manifestDirs(t) {
		docs := render(t, dir)

		// Rendered ConfigMaps by name, so a Deployment's envFrom can be resolved.
		configMaps := map[string]map[string]string{}
		for _, cm := range kindsOf(docs, "ConfigMap") {
			data, _ := cm["data"].(map[string]any)
			flat := map[string]string{}
			for k, v := range data {
				if s, ok := v.(string); ok {
					flat[k] = s
				}
			}
			configMaps[name(cm)] = flat
		}

		for _, deploy := range kindsOf(docs, "Deployment") {
			spec := nested(deploy, "spec", "template", "spec")
			if spec == nil {
				continue
			}
			containers, _ := spec["containers"].([]any)
			for _, c := range containers {
				container, _ := c.(map[string]any)
				if !isDeliveryConsumer(container) {
					continue
				}

				env := resolveEnv(container, configMaps)
				delays, ok := env["RELAY_RETRY_DELAYS"]
				if !ok {
					t.Errorf("%s/%s runs RELAY_MODE=deliver but no RELAY_RETRY_DELAYS reaches it",
						name(deploy), container["name"])
					continue
				}
				timeoutSpec := env["RELAY_DELIVERY_TIMEOUT"]
				if timeoutSpec == "" {
					timeoutSpec = "2s" // relay's own default
				}

				schedule, err := config.ParseRetrySchedule(delays, false)
				if err != nil {
					t.Errorf("%s: RELAY_RETRY_DELAYS=%q is not a schedule relay would accept: %v",
						name(deploy), delays, err)
					continue
				}
				attemptTimeout, err := time.ParseDuration(timeoutSpec)
				if err != nil {
					t.Errorf("%s: RELAY_DELIVERY_TIMEOUT=%q: %v", name(deploy), timeoutSpec, err)
					continue
				}

				// The check that catches a manifest deploying straight to a
				// crashloop: relay refuses to start on a schedule it cannot
				// outlive, and nothing else here would notice until the pod
				// was already restarting.
				if err := schedule.ValidateStallBudget(config.DefaultStallBudget, attemptTimeout); err != nil {
					t.Errorf("%s would not start: %v", name(deploy), err)
					continue
				}

				worst := schedule.WorstCase(attemptTimeout)
				grace, err := gracePeriod(spec)
				if err != nil {
					t.Errorf("%s: %v", name(deploy), err)
					continue
				}

				checked++
				if grace <= worst {
					t.Errorf("%s has terminationGracePeriodSeconds=%v but one record can take %v "+
						"(%q is %v of delays plus %d attempts at %v).\n"+
						"A scale-down would SIGKILL mid-delivery, leaving the offset uncommitted and "+
						"the record redelivered. Raise the grace period above %v, or shorten the schedule.",
						name(deploy), grace, worst,
						schedule.Name, schedule.Total(), schedule.MaxAttempts(), attemptTimeout,
						worst)
				}
			}
		}
	}

	// A discovery bug that finds no consumers would otherwise pass silently,
	// which is the failure this whole file exists to prevent elsewhere.
	if checked == 0 {
		t.Fatal("no RELAY_MODE=deliver container found in any manifest directory; " +
			"this invariant is asserting nothing")
	}
}

// isDeliveryConsumer reports whether a container runs relay in deliver mode.
func isDeliveryConsumer(container map[string]any) bool {
	return relayMode(container) == "deliver"
}

// relayMode reports a container's RELAY_MODE, or "" when it is not relay.
func relayMode(container map[string]any) string {
	env, _ := container["env"].([]any)
	for _, e := range env {
		entry, _ := e.(map[string]any)
		if entry["name"] != "RELAY_MODE" {
			continue
		}
		mode, _ := entry["value"].(string)
		return mode
	}
	return ""
}

// resolveEnv flattens a container's inline env and its envFrom configMapRefs
// into the values the process would actually see. Inline env wins, matching
// Kubernetes.
func resolveEnv(container map[string]any, configMaps map[string]map[string]string) map[string]string {
	out := map[string]string{}

	envFrom, _ := container["envFrom"].([]any)
	for _, e := range envFrom {
		entry, _ := e.(map[string]any)
		ref, _ := entry["configMapRef"].(map[string]any)
		if ref == nil {
			continue // Secrets are not readable from the rendered manifests, and hold no schedule
		}
		refName, _ := ref["name"].(string)
		for k, v := range configMaps[refName] {
			out[k] = v
		}
	}

	env, _ := container["env"].([]any)
	for _, e := range env {
		entry, _ := e.(map[string]any)
		k, _ := entry["name"].(string)
		if v, ok := entry["value"].(string); ok {
			out[k] = v
		}
	}
	return out
}

// gracePeriod reads terminationGracePeriodSeconds, defaulting the way
// Kubernetes does when it is absent.
func gracePeriod(podSpec map[string]any) (time.Duration, error) {
	raw, found := podSpec["terminationGracePeriodSeconds"]
	if !found {
		// Kubernetes' own default. Stated rather than assumed, because a
		// delivery consumer relying on it is relying on 30s.
		return 30 * time.Second, nil
	}
	switch v := raw.(type) {
	case float64:
		return time.Duration(v) * time.Second, nil
	case int:
		return time.Duration(v) * time.Second, nil
	case int64:
		return time.Duration(v) * time.Second, nil
	default:
		return 0, fmt.Errorf("terminationGracePeriodSeconds is %T (%v), want a number", raw, raw)
	}
}

// TestStallBudgetFitsInsideTheGracePeriod asserts that the bounded drain relay
// performs after SIGTERM ends before Kubernetes may send SIGKILL.
//
// DefaultStallBudget is service policy. Consumer.Run enforces it as the complete
// record deadline during normal work and shutdown drain. An interrupted HTTP
// attempt then gets one bounded history write on a fresh context. Raising their
// combined budget past the grace period would leave relay waiting when
// Kubernetes sends SIGKILL, while the schedule-only assertion above could still
// pass.
//
// Every ordered cleanup step counts, not just the record work, and both relay
// modes have one. config.DeliverDrainBudget and config.IngestDrainBudget are
// the sums; this test's job is to hold each manifest above the sum for the mode
// it runs.
//
// Two review rounds on 2026-09-03 each found a term missing from those sums --
// first the OTLP flush, then the Kafka and pool closes, which had no deadline
// at all. Both were invisible here while the number this test compared against
// read as exact. That is the failure mode: a term the validator cannot see is a
// term that can grow past the grace period without failing anything.
//
// What it does NOT prove: that relay exits inside its budget. It proves the
// deadlines relay CONFIGURES sum to less than the grace period. Work without a
// deadline cannot be bounded by arithmetic -- closeBounded abandons it instead,
// which is why each manifest also carries slack above its budget.
func TestStallBudgetFitsInsideTheGracePeriod(t *testing.T) {
	modes := map[string]struct {
		budget time.Duration
		terms  string
	}{
		"deliver": {config.DeliverDrainBudget(),
			"record work, interrupted-attempt history write, health-server shutdown, " +
				"broker and pool closes, trace flush"},
		"ingest": {config.IngestDrainBudget(),
			"readiness grace, HTTP server shutdown, broker and pool closes, trace flush"},
	}
	checked := map[string]int{}

	for _, dir := range manifestDirs(t) {
		for _, deploy := range kindsOf(render(t, dir), "Deployment") {
			spec := nested(deploy, "spec", "template", "spec")
			if spec == nil {
				continue
			}
			mode := ""
			containers, _ := spec["containers"].([]any)
			for _, c := range containers {
				container, _ := c.(map[string]any)
				if found := relayMode(container); found != "" {
					mode = found
					break
				}
			}
			want, known := modes[mode]
			if !known {
				continue
			}

			grace, err := gracePeriod(spec)
			if err != nil {
				t.Errorf("%s: %v", name(deploy), err)
				continue
			}
			checked[mode]++
			if want.budget >= grace {
				t.Errorf("%s (RELAY_MODE=%s) has terminationGracePeriodSeconds=%v but its drain "+
					"budget is %v -- the sum of %s.\n"+
					"Work that uses all of it must finish before SIGKILL. Raise the grace period "+
					"above %v, or lower one of the budgets in relay's config package.\n"+
					"See docs/adr/0006-kafka-over-sqs-for-delivery.md.",
					name(deploy), mode, grace, want.budget, want.terms, want.budget)
			}
		}
	}

	for mode := range modes {
		if checked[mode] == 0 {
			t.Errorf("no RELAY_MODE=%s container found in any manifest directory; "+
				"the discovery this test shares with the budget assertion has broken", mode)
		}
	}
}
