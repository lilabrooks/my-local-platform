"""Build MongoDB reads from catalog fields and a small aggregation vocabulary."""

from .catalog import IDENTIFIER, Refused


def field(name, fields):
    if name not in fields:
        raise Refused("Field is outside the catalog.")
    return name


def scalar(v):
    if v is not None and type(v) not in (int, float, str, bool):
        raise Refused("A scalar filter value is required.")
    return v


def match(filters, fields):
    out = {}
    for name, value in (filters or {}).items():
        field(name, fields)
        if isinstance(value, dict):
            if not value or not value.keys() <= {"$eq", "$ne", "$lt", "$lte", "$gt", "$gte"}:
                raise Refused("Unsupported filter operator.")
            out[name] = {op: scalar(v) for op, v in value.items()}
        else:
            out[name] = {"$eq": scalar(value)}
    return out


def sort_keys(order, fields):
    for name, direction in order.items():
        field(name, fields)
        if type(direction) is not int or direction not in (-1, 1):
            raise Refused("Sort direction must be 1 or -1.")
    return dict(order)


def projection(fields, requested=None):
    if requested is None:
        selected = fields
    else:
        if not requested or any(type(v) is not int or v != 1 for v in requested.values()):
            raise Refused("Specify an inclusion projection.")
        selected = [field(name, fields) for name in requested]
    return {"_id": 0, **dict.fromkeys(selected, 1)}


def aggregation(stages, fields):
    if len(stages) > 12:
        raise Refused("Use at most 12 aggregation stages.")
    result = [{"$project": projection(fields)}]
    for stage in stages:
        if not isinstance(stage, dict) or len(stage) != 1:
            raise Refused("Each aggregation stage needs one operator.")
        op, config = next(iter(stage.items()))
        if op == "$limit" and type(config) is int and 0 < config <= 1000000:
            result.append({op: config})
            continue
        if not isinstance(config, dict):
            raise Refused("Invalid aggregation stage.")
        if op == "$match":
            result.append({op: match(config, fields)})
        elif op == "$sort" and config:
            result.append({op: sort_keys(config, fields)})
        elif op == "$group" and "_id" in config:
            group = {}
            for key, expr in config.items():
                if key == "_id":
                    if expr is not None and not (isinstance(expr, str) and expr.startswith("$") and expr[1:] in fields):
                        raise Refused("Group by an approved field or null.")
                    group[key] = expr
                elif IDENTIFIER.fullmatch(key) and isinstance(expr, dict) and expr.keys() == {"$sum"} and type(expr["$sum"]) is int and expr["$sum"] == 1:
                    group[key] = {"$sum": 1}
                else:
                    raise Refused("Only count accumulators are supported.")
            fields = tuple(group)
            result.append({op: group})
        else:
            raise Refused("Unsupported aggregation stage.")
    return result
