"""Schema for the `entry_outcome_dependencies` table."""

import dataframely as dy
import polars as pl


class EntryOutcomeDependenciesSchema(dy.Schema):
    """Schema for the `entry_outcome_dependencies` table."""

    dependency_uuid = dy.String(primary_key=True, min_length=1, max_length=255)
    outcome_id_a = dy.String(min_length=1, max_length=255)
    outcome_id_b = dy.String(min_length=1, max_length=255)
    relationship_type = dy.Enum(
        [
            "complement",
            "equivalent",
            "implies",
            "mutex",
            "composite",
            "disjunction",
            "upper_bound",
            "lower_bound",
            "conditional",
        ]
    )
    comments = dy.String(nullable=True, max_length=500)

    @dy.rule(group_by=["outcome_id_a", "outcome_id_b"])
    def _unique_outcome_pair(cls) -> pl.Expr:
        """Ensure unique outcome pairs."""
        return pl.len() == 1

    @dy.rule()
    def _outcome_a_and_b_are_distinct(cls) -> pl.Expr:
        """Ensure outcome A and B are distinct/different from each other."""
        return pl.col("outcome_id_a") != pl.col("outcome_id_b")


# | Type               | Price Condition            | Action                         |
# | ------------------ | -------------------------- | ------------------------------ |
# | complement         | Yes + No < 1 - fees        | Buy both                       |
# | complement         | Yes + No > 1 - fees        | Sell both                      |
# | equivalent         | P(A) > P(B) + fees         | Sell A, Buy B                  |
# | implies / temporal | P(A) > P(B)                | Buy B, optionally Sell A       |
# | mutex              | P(A) + P(B) > 1            | Sell both                      |
# | composite          | P(C) > min(P(A), P(B))     | Sell C, Buy cheaper leg        |
# | composite          | P(C) >> P(A) x P(B)        | Sell C, Buy A and B            |
# | composite          | P(C) << P(A) x P(B)        | Buy C, Sell A and B            |
# | disjunction        | P(C) < max(P(A), P(B))     | Buy C, Sell larger leg         |
# | disjunction        | P(C) >> P(A)+P(B)-P(A)P(B) | Sell C, Buy A and B            |
# | upper/lower bound  | Strict > Loose in price    | Buy Loose, Sell Strict         |
# | conditional        | P(B) moves, P(A) lags      | Trade A in direction B implies |
