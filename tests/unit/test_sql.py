import pytest

from dalmatian.sql import validate_and_normalize


def test_select_may_only_reference_declared_sources() -> None:
    with pytest.raises(ValueError, match="undeclared source"):
        validate_and_normalize("select * from secret", {"orders"})


def test_cte_is_allowed_when_base_table_is_declared() -> None:
    sql = "with recent as (select * from orders) select * from recent"
    assert "recent" in validate_and_normalize(sql, {"orders"}).lower()


def test_write_statement_is_rejected() -> None:
    with pytest.raises(ValueError, match="read-only"):
        validate_and_normalize("delete from orders", {"orders"})


@pytest.mark.parametrize(
    "sql",
    [
        "insert into orders select * from orders",
        "update orders set total = 0",
        "create table copied as select * from orders",
        "drop table orders",
        (
            "merge into orders using orders as updates on orders.id = updates.id "
            "when matched then delete"
        ),
        "select * from default.orders",
        "select * from spark_catalog.default.orders",
        "select * from orders; select * from orders",
    ],
)
def test_mutation_qualification_and_multiple_statements_are_rejected(sql: str) -> None:
    with pytest.raises(ValueError):
        validate_and_normalize(sql, {"orders"})


def test_aliases_ctes_and_normalization_preserve_read_only_query_semantics() -> None:
    sql = """
        WITH totals AS (
          SELECT o.country, SUM(o.total) AS amount
          FROM orders AS o
          GROUP BY o.country
        )
        SELECT country, amount FROM totals WHERE amount > 10 ORDER BY country
    """
    normalized = validate_and_normalize(sql, {"orders"})
    assert "orders AS o" in normalized
    assert "FROM totals" in normalized
    assert "ORDER BY country" in normalized
