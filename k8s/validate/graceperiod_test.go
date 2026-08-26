package validate

import (
	"fmt"
	"testing"
	"time"

	"github.com/lilabrooks/my-local-platform/relay/config"
)

// A delivery consumer holds one record for as long as its retry schedule says
// it might: every delay waited in full, and every attempt running to its
// timeout. If Kubernetes SIGKILLs the pod before that, the offset is
// uncommitted and the record is redelivered.
//
// Redelivery is correct -- at-least-once is the stated contract -- but it means
// every scale-down manufactures duplicate deliveries, and scale-down is half of
// what M2 demonstrates. KEDA removing a pod mid-delivery should be a drain, not
// a dropped record.
//
// The two settings that have to agree live in different files and look
// unrelated. RELAY_RETRY_DELAYS and RELAY_DELIVERY_TIMEOUT are in a ConfigMap;
// terminationGracePeriodSeconds is in a Deployment. Nothing about either
// mentions the other, so this is the only thing keeping them in step.
//
// The worst case is computed with relay's own config package rather than a copy
// of the preset table, so changing what "demo" means cannot leave this
// assertion passing against numbers nothing uses.
//
// Worth being precise about what each half catches, because they are not
// symmetric. relay's own startup check already caps the worst case below
// DefaultRebalanceTimeout (30s), so with a grace period at or above that the
// budget can never outgrow it -- probed directly: "demo" is 25s and starts,
// "10s,20s" is 36s and does not. The grace assertion therefore guards one
// direction only, someone lowering the grace period, and the schedule
// assertion guards the other, a manifest that deploys straight to a crashloop.
func TestDeliveryConsumerOutlivesItsRetryBudget(t *testing.T) {
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
				if err := schedule.ValidateLiveness(config.DefaultRebalanceTimeout, attemptTimeout); err != nil {
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
	env, _ := container["env"].([]any)
	for _, e := range env {
		entry, _ := e.(map[string]any)
		if entry["name"] == "RELAY_MODE" && entry["value"] == "deliver" {
			return true
		}
	}
	return false
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
