package dbmcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// BSON documents are ordered. Decoding JSON sort specifications into a map
// would silently change the priority of the caller's sort keys.
func ordered(raw json.RawMessage) (bson.D, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return bson.D{}, nil
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	value, err := readMongoValue(d, 0)
	if safe, ok := err.(refusal); ok {
		return nil, safe
	}
	doc, ok := value.(bson.D)
	if err != nil || !ok {
		return nil, refusal("Expected an object with scalar values or operators.")
	}
	return doc, nil
}
func readMongoValue(d *json.Decoder, depth int) (any, error) {
	if depth > 16 {
		return nil, refusal("MongoDB argument is too deeply nested.")
	}
	tok, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch v := tok.(type) {
	case json.Delim:
		if v != '{' {
			return nil, refusal("Expected a document.")
		}
		doc := bson.D{}
		seen := map[string]bool{}
		for d.More() {
			name, err := d.Token()
			if err != nil {
				return nil, err
			}
			key, ok := name.(string)
			if !ok || seen[key] {
				return nil, refusal("Invalid or duplicate document key.")
			}
			seen[key] = true
			val, err := readMongoValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			doc = append(doc, bson.E{Key: key, Value: val})
		}
		_, err = d.Token()
		return doc, err
	case json.Number:
		if strings.ContainsAny(v.String(), ".eE") {
			return mongoDouble(v.String())
		}
		return v.Int64()
	default:
		return v, nil
	}
}

// Compare decimal spellings after the normal BSON double conversion. Ordinary
// values such as 0.1 keep double equality; a rounded threshold is refused.
func mongoDouble(number string) (float64, error) {
	const rounded = refusal("The number would change at BSON double precision.")
	value, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return 0, rounded
	}
	digits, scale, err := decimalMagnitude(number)
	shortDigits, shortScale, shortErr := decimalMagnitude(strconv.FormatFloat(value, 'g', -1, 64))
	if err != nil || shortErr != nil || digits != shortDigits || scale != shortScale {
		return 0, rounded
	}
	return value, nil
}

func stage(name string, value any) bson.D { return bson.D{{Key: name, Value: value}} }
func mongoField(fields []string, name string) error {
	if !slices.Contains(fields, name) {
		return refusal("Field is outside the catalog.")
	}
	return nil
}
func match(fields []string, filter bson.D) (bson.D, error) {
	out := bson.D{}
	for _, el := range filter {
		if err := mongoField(fields, el.Key); err != nil {
			return nil, err
		}
		ops, ok := el.Value.(bson.D)
		if !ok {
			ops = stage("$eq", el.Value)
		}
		if len(ops) == 0 {
			return nil, refusal("Unsupported filter operator.")
		}
		for _, op := range ops {
			if !slices.Contains([]string{"$eq", "$ne", "$lt", "$lte", "$gt", "$gte"}, op.Key) {
				return nil, refusal("Unsupported filter operator.")
			}
			switch op.Value.(type) {
			case nil, string, int64, float64, bool:
			default:
				return nil, refusal("A scalar filter value is required.")
			}
		}
		out = append(out, bson.E{Key: el.Key, Value: ops})
	}
	return out, nil
}
func sortSpec(fields []string, doc bson.D) (bson.D, error) {
	for _, item := range doc {
		if err := mongoField(fields, item.Key); err != nil {
			return nil, err
		}
		n, ok := item.Value.(int64)
		if !ok || n != 1 && n != -1 {
			return nil, refusal("Sort direction must be 1 or -1.")
		}
	}
	return doc, nil
}
func project(fields []string, selected bson.D) (bson.D, error) {
	keys := fields
	if selected != nil {
		if len(selected) == 0 {
			return nil, refusal("Specify an inclusion projection.")
		}
		keys = make([]string, 0, len(selected))
		for _, item := range selected {
			if err := mongoField(fields, item.Key); err != nil {
				return nil, err
			}
			if item.Value != int64(1) {
				return nil, refusal("Specify an inclusion projection.")
			}
			keys = append(keys, item.Key)
		}
	}
	out := bson.D{}
	if !slices.Contains(keys, "_id") {
		out = append(out, bson.E{Key: "_id", Value: 0})
	}
	for _, key := range keys {
		out = append(out, bson.E{Key: key, Value: 1})
	}
	return out, nil
}
func groupSpec(fields []string, doc bson.D) ([]string, error) {
	var next []string
	for _, el := range doc {
		if el.Key == "_id" {
			if el.Value != nil {
				v, ok := el.Value.(string)
				if !ok || !strings.HasPrefix(v, "$") || !slices.Contains(fields, v[1:]) {
					return nil, refusal("Group by an approved field or null.")
				}
			}
		} else {
			accum, ok := el.Value.(bson.D)
			if !identifier.MatchString(el.Key) || !ok || len(accum) != 1 || accum[0].Key != "$sum" || accum[0].Value != int64(1) {
				return nil, refusal("Only count accumulators are supported.")
			}
		}
		next = append(next, el.Key)
	}
	if !slices.Contains(next, "_id") {
		return nil, refusal("A group needs an _id.")
	}
	return next, nil
}
func aggregate(fields []string, stages []json.RawMessage) (mongo.Pipeline, error) {
	if len(stages) > 12 {
		return nil, refusal("Use at most 12 aggregation stages.")
	}
	initial, err := project(fields, nil)
	if err != nil {
		return nil, err
	}
	result := mongo.Pipeline{stage("$project", initial)}
	for _, raw := range stages {
		doc, err := ordered(raw)
		if err != nil {
			return nil, err
		}
		if len(doc) != 1 {
			return nil, refusal("Each aggregation stage needs one operator.")
		}
		op := doc[0]
		cfg, isDoc := op.Value.(bson.D)
		var compiled any = cfg
		switch {
		case op.Key == "$match" && isDoc:
			compiled, err = match(fields, cfg)
		case op.Key == "$sort" && isDoc && len(cfg) > 0:
			compiled, err = sortSpec(fields, cfg)
		case op.Key == "$group" && isDoc:
			fields, err = groupSpec(fields, cfg)
		case op.Key == "$limit":
			n, ok := op.Value.(int64)
			if !ok || n < 1 || n > 1000000 {
				return nil, refusal("Invalid aggregation limit.")
			}
			compiled = n
		default:
			return nil, refusal("Unsupported aggregation stage.")
		}
		if err != nil {
			return nil, err
		}
		result = append(result, stage(op.Key, compiled))
	}
	return result, nil
}

func mongoPipeline(tool string, fields []string, a arguments) (mongo.Pipeline, error) {
	if tool == "mongo_aggregate" || tool == "explain_query" {
		return aggregate(fields, a.Pipeline)
	}
	raw, err := ordered(a.Filter)
	if err != nil {
		return nil, err
	}
	filters, err := match(fields, raw)
	if err != nil {
		return nil, err
	}
	p := mongo.Pipeline{stage("$match", filters)}
	switch tool {
	case "mongo_find":
		sort, err := ordered(a.Sort)
		if err != nil {
			return nil, err
		}
		if len(sort) > 0 {
			order, err := sortSpec(fields, sort)
			if err != nil {
				return nil, err
			}
			p = append(p, stage("$sort", order))
		}
		var selection bson.D
		if len(a.Projection) > 0 && string(a.Projection) != "null" {
			selection, err = ordered(a.Projection)
			if err != nil {
				return nil, err
			}
		}
		projection, err := project(fields, selection)
		if err != nil {
			return nil, err
		}
		return append(p, stage("$project", projection)), nil
	case "mongo_count":
		return append(p, stage("$count", "total")), nil
	case "mongo_distinct":
		if err := mongoField(fields, a.Field); err != nil {
			return nil, err
		}
		return append(p, stage("$group", stage("_id", "$"+a.Field)), stage("$sort", stage("_id", 1)),
			stage("$project", bson.D{{Key: "_id", Value: 0}, {Key: "value", Value: "$_id"}})), nil
	default:
		return nil, fmt.Errorf("unknown Mongo tool %s", tool)
	}
}
