package validate

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"testing"
)

// The bootstrap script is the only place topic partition counts are declared.
// Reading it here rather than restating the number is the point: an invariant
// that carries its own copy of the value it guards can agree with itself while
// both copies are wrong.
const topicScript = "../../local/bootstrap/kafka-topics.sh"

// `topic <name> <partitions>` -- the helper every topic in the script is
// created through. Anchored to the line start so the helper's own definition
// and the commentary around it do not match.
var topicLine = regexp.MustCompile(`(?m)^topic\s+(\S+)\s+(\d+)\s*$`)

// topicPartitions parses the bootstrap script into topic name -> partition
// count.
func topicPartitions(t *testing.T) map[string]int {
	t.Helper()

	body, err := os.ReadFile(topicScript)
	if err != nil {
		t.Fatalf("reading %s: %v", topicScript, err)
	}

	out := map[string]int{}
	for _, m := range topicLine.FindAllStringSubmatch(string(body), -1) {
		n, err := strconv.Atoi(m[2])
		if err != nil {
			t.Fatalf("topic %q in %s has a non-numeric partition count %q", m[1], topicScript, m[2])
		}
		out[m[1]] = n
	}

	// A parser that silently matches nothing would make every assertion below
	// vacuous, and it would look exactly like a passing test.
	if len(out) == 0 {
		t.Fatalf("parsed no topics out of %s; the `topic <name> <partitions>` "+
			"convention this reads has probably changed", topicScript)
	}
	return out
}

// scaledObjects returns every ScaledObject across the rendered manifest dirs,
// keyed by name.
func scaledObjects(t *testing.T) map[string]map[string]any {
	t.Helper()

	out := map[string]map[string]any{}
	for _, dir := range manifestDirs(t) {
		for _, so := range kindsOf(render(t, dir), "ScaledObject") {
			out[name(so)] = so
		}
	}
	return out
}

// sortedKeys is the map[string]int counterpart of keys() in dashboard_test.go,
// sorted so a failure message reads the same way twice.
func sortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// asInt copes with YAML numbers arriving as int, int64 or float64 depending on
// the decoder's mood, rather than asserting one and failing confusingly.
func asInt(v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		return int(n), nil
	case string:
		return strconv.Atoi(n)
	default:
		return 0, fmt.Errorf("%v is %T, not a number", v, v)
	}
}

// TestScaledObjectMaxReplicasMatchesPartitionCount is the invariant ADR 0007
// asked for in its own "Revisit when" section, which noted the two numbers were
// "set equal on purpose and nothing enforces it".
//
// Partition count is the hard ceiling on consumer-group parallelism. Above it,
// extra pods join the group, are assigned nothing, and sit idle -- so a
// maxReplicaCount larger than the partition count buys nothing and makes the
// demo show KEDA scaling while nothing drains faster.
//
// Below it is worse in a quieter way: the group cannot use partitions it has
// members for, and the ceiling that limits throughput is invisible in the
// manifest.
//
// The drift is silent either way, which is why a sentence in an ADR was not
// enough. Both numbers are read from the files that declare them, and the topic
// is taken from the ScaledObject's own trigger rather than hardcoded here -- so
// repointing the trigger at a different topic follows the trigger.
func TestScaledObjectMaxReplicasMatchesPartitionCount(t *testing.T) {
	partitions := topicPartitions(t)

	objs := scaledObjects(t)
	if len(objs) == 0 {
		t.Fatal("no ScaledObject found in any manifest directory")
	}

	for soName, so := range objs {
		t.Run(soName, func(t *testing.T) {
			spec := nested(so, "spec")
			if spec == nil {
				t.Fatal("ScaledObject has no spec")
			}

			rawMax, found := spec["maxReplicaCount"]
			if !found {
				t.Fatal("ScaledObject has no maxReplicaCount, so nothing caps the consumer group")
			}
			maxReplicas, err := asInt(rawMax)
			if err != nil {
				t.Fatalf("maxReplicaCount: %v", err)
			}

			triggers, ok := spec["triggers"].([]any)
			if !ok || len(triggers) == 0 {
				t.Fatal("ScaledObject has no triggers")
			}

			checked := 0
			for i, raw := range triggers {
				trigger, ok := raw.(map[string]any)
				if !ok {
					t.Fatalf("trigger %d is not an object", i)
				}
				// Only Kafka triggers carry a partition ceiling. A CPU or cron
				// trigger on the same object is not this invariant's business.
				if trigger["type"] != "apache-kafka" {
					continue
				}
				meta := nested(trigger, "metadata")
				if meta == nil {
					t.Fatalf("trigger %d has no metadata", i)
				}
				topic, ok := meta["topic"].(string)
				if !ok || topic == "" {
					t.Fatalf("trigger %d names no topic", i)
				}

				want, known := partitions[topic]
				if !known {
					t.Fatalf("the trigger scales on topic %q, which %s never creates.\n"+
						"Either the topic is missing from the bootstrap script or the "+
						"trigger points at one that does not exist.\nTopics it does "+
						"create: %v", topic, topicScript, sortedKeys(partitions))
				}

				checked++
				if maxReplicas != want {
					t.Errorf("maxReplicaCount is %d but %s has %d partitions (%s).\n"+
						"A consumer group cannot usefully exceed its partition count, and "+
						"below it the ceiling is invisible in the manifest.\n"+
						"These are set equal on purpose -- see docs/adr/0007-keda-lag-autoscaling.md.",
						maxReplicas, topic, want, topicScript)
				}
			}

			if checked == 0 {
				t.Fatal("no apache-kafka trigger found, so the partition ceiling was never checked")
			}
		})
	}
}
