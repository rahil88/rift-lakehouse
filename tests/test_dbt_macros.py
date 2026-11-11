"""Evaluate the real dbt Wilson macro: render the Jinja, run the SQL in SQLite, compare to
reference values. This tests the macro the mart uses, not a Python re-implementation."""

import sqlite3
from pathlib import Path

import jinja2
import pytest

MACRO = Path("dbt/macros/wilson.sql").read_text()


def wilson_sql(successes, trials, side):
    env = jinja2.Environment()
    module = env.from_string(MACRO, globals={"var": lambda name: 1.96}).module
    return str(module.wilson_bound(str(successes), str(trials), side))


def evaluate(successes, trials, side):
    # SQLite integer division would truncate; Spark's "/" returns a double, so mirror that
    sql = wilson_sql(f"{successes}.0", f"{trials}.0", side)
    return sqlite3.connect(":memory:").execute(f"select {sql}").fetchone()[0]


@pytest.mark.parametrize(
    ("wins", "games", "lower", "upper"),
    [
        (8, 10, 0.4902, 0.9433),  # textbook example
        (3, 3, 0.4385, 1.0),  # perfect record, tiny sample: wide interval
        (0, 5, 0.0, 0.4345),
        (500, 1000, 0.4691, 0.5309),
    ],
)
def test_wilson_matches_reference_values(wins, games, lower, upper):
    assert evaluate(wins, games, "lower") == pytest.approx(lower, abs=1e-4)
    assert evaluate(wins, games, "upper") == pytest.approx(upper, abs=1e-4)


def test_wilson_zero_games_is_null():
    assert evaluate(0, 0, "lower") is None
