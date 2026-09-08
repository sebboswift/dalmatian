from __future__ import annotations

import re

from dalmatian.errors import QueryRejected

_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|create|drop|alter|merge|truncate|cache|uncache|refresh|set|use|"
    r"describe|show|grant|revoke|call|copy)\b",
    re.IGNORECASE,
)
_TABLE = re.compile(r"\b(?:from|join)\s+([`\w.]+)", re.IGNORECASE)
_CTE = re.compile(r"(?:\bwith\b|,)\s*([A-Za-z_]\w*)\s+as\s*\(", re.IGNORECASE)


def validate_and_normalize(sql: str, aliases: set[str]) -> str:
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return _fallback(sql, aliases)

    try:
        statements = sqlglot.parse(sql, read="spark")
    except Exception as exc:
        raise QueryRejected(f"invalid SQL: {exc}") from exc
    if len(statements) != 1 or statements[0] is None:
        raise QueryRejected("exactly one SQL statement is required")
    expression = statements[0]
    if not isinstance(expression, exp.Query):
        raise QueryRejected("only read-only SELECT queries are allowed")

    ctes = {cte.alias_or_name for cte in expression.find_all(exp.CTE)}
    allowed = aliases | ctes
    for table in expression.find_all(exp.Table):
        if table.catalog or table.db:
            raise QueryRejected("catalog and database-qualified tables are not allowed")
        if table.name not in allowed:
            raise QueryRejected(f"query references undeclared source: {table.name}")

    return expression.sql(dialect="spark", pretty=False)


def _fallback(sql: str, aliases: set[str]) -> str:
    value = sql.strip()
    if not value:
        raise QueryRejected("SQL cannot be empty")
    stripped = value.rstrip().rstrip(";").strip()
    if ";" in stripped:
        raise QueryRejected("exactly one SQL statement is required")
    if not re.match(r"^(select|with)\b", stripped, re.IGNORECASE):
        raise QueryRejected("only read-only SELECT queries are allowed")
    if _FORBIDDEN.search(stripped):
        raise QueryRejected("only read-only SELECT queries are allowed")
    ctes = set(_CTE.findall(stripped))
    for raw in _TABLE.findall(stripped):
        name = raw.strip("`").split(".")[-1]
        if "." in raw or name not in aliases | ctes:
            raise QueryRejected(f"query references undeclared source: {name}")
    return re.sub(r"\s+", " ", stripped)
