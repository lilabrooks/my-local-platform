package dbmcp

import (
	"context"
	"math"
	"net"
	"regexp"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// The application's audit owns stderr. Driver debug logs may contain queries.
type quietMongoLog struct{}

func (quietMongoLog) Info(int, string, ...any)    {}
func (quietMongoLog) Error(error, string, ...any) {}

func mongoOptions(t *Target) (*options.ClientOptions, error) {
	pass, err := t.password()
	if err != nil {
		return nil, err
	}
	tlsConfig, err := t.tlsConfig()
	if err != nil {
		return nil, err
	}
	pref, err := readpref.ModeFromString(t.ReadPreference)
	if err != nil {
		return nil, err
	}
	rp, err := readpref.New(pref)
	if err != nil {
		return nil, err
	}
	opts := options.Client().SetHosts([]string{net.JoinHostPort(t.Host, strconv.Itoa(t.Port))}).
		SetAuth(options.Credential{AuthSource: t.AuthDatabase, Username: t.Username, Password: pass, PasswordSet: true}).
		SetReadPreference(rp).SetRetryWrites(false).SetServerSelectionTimeout(5 * time.Second).SetTimeout(queryTimeout)
	opts.SetLoggerOptions(options.Logger().SetSink(quietMongoLog{}))
	if tlsConfig != nil {
		opts.SetTLSConfig(tlsConfig)
	} else {
		// A loopback seed could advertise non-loopback replica members.
		opts.SetDirect(true)
	}
	if t.ReplicaSet != "" {
		opts.SetReplicaSet(t.ReplicaSet)
	}
	return opts, nil
}

func readMongo(ctx context.Context, t *Target, rid, collection string, p mongo.Pipeline, cap int, explain bool) (document, error) {
	opts, err := mongoOptions(t)
	if err != nil {
		return nil, err
	}
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = client.Disconnect(cleanup)
	}()
	return mongoCommand(ctx, client.Database(t.Database), opts.ReadPreference, rid, collection, p, cap, explain)
}

func mongoCommand(ctx context.Context, db *mongo.Database, rp *readpref.ReadPref, rid, collection string, p mongo.Pipeline, cap int, explain bool) (document, error) {
	pipeline := append(p, stage("$limit", cap+1))
	command := bson.D{{Key: "aggregate", Value: collection}, {Key: "pipeline", Value: pipeline},
		{Key: "cursor", Value: stage("batchSize", cap+1)}, {Key: "allowDiskUse", Value: false}}
	// Raw commands preserve the server-side maxTimeMS derived by the driver
	// from the deadline. Collection.Aggregate omits it for user-managed cursors.
	placement := options.RunCmd().SetReadPreference(rp)
	if explain {
		cmd := bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "queryPlanner"}, {Key: "comment", Value: rid}}
		var tree bson.D
		if err := db.RunCommand(ctx, cmd, placement).Decode(&tree); err != nil {
			return nil, err
		}
		return document{"operators": planNodes(tree, "mongodb")}, nil
	}
	command = append(command, bson.E{Key: "comment", Value: rid})
	cursor, err := db.RunCommandCursor(ctx, command, placement)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()
	rows := make([]any, 0)
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		row := document{}
		for _, el := range doc {
			row[el.Key] = mongoScalar(el.Value)
		}
		rows = append(rows, row)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	return document{"rows": rows}, nil
}

func mongoScalar(v any) any {
	switch n := v.(type) {
	case nil, bool, string, int32, int64:
		return n
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return nil
		}
		return n
	case bson.ObjectID:
		return n.Hex()
	case bson.DateTime:
		return isoTimestamp(n.Time().UTC(), false)
	default:
		return nonScalar
	}
}

var operatorName = regexp.MustCompile(`^[A-Za-z_ ]{1,48}$`)

func planNodes(tree any, engine string) []string {
	nameKey := "stage"
	edges := []string{"queryPlanner", "winningPlan", "queryPlan", "inputStage", "inputStages", "outerStage", "innerStage", "stages", "$cursor", "shards"}
	switch engine {
	case "postgres":
		nameKey = "Node Type"
		edges = []string{"Plan", "Plans"}
	case "mysql":
		nameKey = "access_type"
		edges = []string{"query_block", "table", "nested_loop", "ordering_operation", "grouping_operation", "duplicates_removal", "materialized_from_subquery"}
	}
	names := []string{}
	seen := map[string]bool{}
	var visit func(any)
	visit = func(node any) {
		var fields map[string]any
		switch n := node.(type) {
		case []any:
			for _, e := range n {
				visit(e)
			}
			return
		case bson.A:
			for _, e := range n {
				visit(e)
			}
			return
		case map[string]any:
			fields = n
		case bson.M:
			fields = n
		case bson.D:
			fields = map[string]any{}
			for _, el := range n {
				fields[el.Key] = el.Value
			}
		default:
			return
		}
		name, _ := fields[nameKey].(string)
		if operatorName.MatchString(name) && !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
		// Only follow plan-tree edges. A user predicate can also contain a "stage" field.
		for _, edge := range edges {
			if child, ok := fields[edge]; ok {
				visit(child)
			}
		}
	}
	visit(tree)
	return names
}
