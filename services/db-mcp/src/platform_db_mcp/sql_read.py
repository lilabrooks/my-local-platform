"""Compile the supported SELECT grammar into a new parameterized statement."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from .catalog import IDENTIFIER, Refused, Target


@dataclass(frozen=True)
class Statement:
    text: str
    values: list[Any]


def reject():
    raise Refused("Use a single catalog view, approved columns, comparisons, COUNT(*), grouping, ordering or LIMIT.")


def only(node, *keys):
    if any(value not in (None, False, [], "") and key not in keys for key, value in node.args.items()):
        reject()


def compile_select(target: Target, source: str, params: list | None, cap: int) -> Statement:
    if not source or len(source) > 8000 or len(params or []) > 100:
        reject()
    try:
        trees = sqlglot.parse(source, read="postgres" if target.engine == "postgres" else "mysql")
    except (SqlglotError, RecursionError):
        reject()
    if len(trees) != 1 or not isinstance(trees[0], exp.Select):
        reject()
    query = trees[0]
    only(query, "expressions", "from_", "where", "group", "order", "limit")
    origin = query.args.get("from_")
    if not origin or not isinstance(origin.this, exp.Table) or not isinstance(origin.this.this, exp.Identifier):
        reject()
    table = origin.this
    only(table, "this", "db")
    if table.db and table.db != target.schema:
        reject()
    fields = target.fields(table.name)
    quote = '"' if target.engine == "postgres" else "`"

    def ident(name):
        if not IDENTIFIER.fullmatch(name):
            reject()
        return quote + name + quote

    def column(node, allowed):
        if not isinstance(node, exp.Column) or not isinstance(node.this, exp.Identifier):
            reject()
        only(node, "this")
        if node.name not in allowed:
            reject()
        return ident(node.name)

    aliases = set()
    select = []
    for item in query.expressions:
        alias = None
        if isinstance(item, exp.Alias):
            if not isinstance(item.args.get("alias"), exp.Identifier):
                reject()
            alias = item.alias
            item = item.this
        if isinstance(item, exp.Star):
            only(item)
            if alias:
                reject()
            select.extend(ident(f) for f in fields)
            continue
        if isinstance(item, exp.Count) and isinstance(item.this, exp.Star):
            only(item, "this", "big_int")
            only(item.this)
            expression = "COUNT(*)"
        else:
            expression = column(item, fields)
        if alias:
            expression += " AS " + ident(alias)
            aliases.add(alias)
        select.append(expression)
    if not select:
        reject()
    supplied = list(params or [])
    values = []

    def value(node):
        if isinstance(node, exp.Placeholder):
            if not supplied:
                reject()
            v = supplied.pop(0)
            if v is not None and type(v) not in (str, int, float, bool):
                reject()
            return v
        if isinstance(node, exp.Literal):
            return node.this if node.is_string else Decimal(node.this)
        if isinstance(node, exp.Boolean):
            return node.this
        reject()

    operators = {exp.EQ: "=", exp.NEQ: "<>", exp.GT: ">", exp.GTE: ">=", exp.LT: "<", exp.LTE: "<="}

    def predicate(node, depth=0):
        if depth > 20:
            reject()
        if isinstance(node, exp.Paren):
            return predicate(node.this, depth + 1)
        if isinstance(node, (exp.And, exp.Or)):
            op = " AND " if isinstance(node, exp.And) else " OR "
            return "(" + predicate(node.left, depth + 1) + op + predicate(node.right, depth + 1) + ")"
        if type(node) not in operators:
            reject()
        left = column(node.left, fields)
        values.append(value(node.right))
        return left + " " + operators[type(node)] + " %s"

    text = "SELECT " + ", ".join(select) + " FROM " + ident(target.schema) + "." + ident(table.name)
    if where := query.args.get("where"):
        text += " WHERE " + predicate(where.this)
    if supplied:
        reject()
    if group := query.args.get("group"):
        only(group, "expressions")
        text += " GROUP BY " + ", ".join(column(c, fields) for c in group.expressions)
    if order := query.args.get("order"):
        keys = []
        for entry in order.expressions:
            if not isinstance(entry, exp.Ordered):
                reject()
            only(entry, "this", "desc", "nulls_first")
            direction = " DESC" if entry.args.get("desc") else " ASC"
            key = column(entry.this, set(fields) | aliases) + direction
            # Preserve the parsed dialect's NULL ordering where the backend supports it.
            if target.engine == "postgres":
                key += " NULLS FIRST" if entry.args.get("nulls_first") else " NULLS LAST"
            keys.append(key)
        text += " ORDER BY " + ", ".join(keys)
    limit = cap + 1
    if clause := query.args.get("limit"):
        only(clause, "expression")
        n = clause.expression
        if not isinstance(n, exp.Literal) or n.is_string or not n.this.isascii() or not n.this.isdigit():
            reject()
        limit = min(limit, int(n.this))
    return Statement(text + " LIMIT " + str(limit), values)
