"""create analytics schema

Revision ID: b812d4794cb3
Revises: 
Create Date: 2026-03-02 18:09:22.886580

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b812d4794cb3'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create schema
    op.execute("CREATE SCHEMA IF NOT EXISTS analytics")

    # 2. dim_fund
    op.create_table(
        "dim_fund",
        sa.Column("fund_key", sa.Text(), primary_key=True),
        sa.Column("registrant_cik", sa.Text(), nullable=True),
        sa.Column("series_id", sa.Text(), nullable=True),
        sa.Column("class_id", sa.Text(), nullable=True),
        sa.Column("fund_name", sa.Text(), nullable=True),
        sa.Column("fund_type", sa.Text(), nullable=True),
        sa.Column("domicile", sa.Text(), nullable=True),
        sa.Column("advisor_name", sa.Text(), nullable=True),
        schema="analytics",
    )

    # 3. dim_security
    op.create_table(
        "dim_security",
        sa.Column("security_key", sa.Text(), primary_key=True),
        sa.Column("cusip", sa.Text(), nullable=True),
        sa.Column("isin", sa.Text(), nullable=True),
        sa.Column("ticker", sa.Text(), nullable=True),
        sa.Column("security_name", sa.Text(), nullable=True),
        sa.Column("asset_category", sa.Text(), nullable=True),
        sa.Column("currency", sa.Text(), nullable=True),
        sa.Column("issuer_name", sa.Text(), nullable=True),
        schema="analytics",
    )

    # 4. fact_holding
    op.create_table(
        "fact_holding",
        sa.Column("fund_key", sa.Text(), nullable=False),
        sa.Column("report_date", sa.Date(), nullable=False),
        sa.Column("security_key", sa.Text(), nullable=False),
        sa.Column("weight_pct", sa.Float(), nullable=True),
        sa.Column("market_value", sa.Float(), nullable=True),
        sa.Column("shares", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint(
            "fund_key", "report_date", "security_key"
        ),
        schema="analytics",
    )

    op.create_index(
        "ix_fact_holding_report_date",
        "fact_holding",
        ["report_date"],
        schema="analytics",
    )

    op.create_index(
        "ix_fact_holding_security",
        "fact_holding",
        ["security_key"],
        schema="analytics",
    )

    # 5. fact_fund_return
    op.create_table(
        "fact_fund_return",
        sa.Column("fund_key", sa.Text(), nullable=False),
        sa.Column("month_end", sa.Date(), nullable=False),
        sa.Column("total_return", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("fund_key", "month_end"),
        schema="analytics",
    )

def downgrade() -> None:
    op.drop_table("fact_fund_return", schema="analytics")
    op.drop_index("ix_fact_holding_security", table_name="fact_holding", schema="analytics")
    op.drop_index("ix_fact_holding_report_date", table_name="fact_holding", schema="analytics")
    op.drop_table("fact_holding", schema="analytics")
    op.drop_table("dim_security", schema="analytics")
    op.drop_table("dim_fund", schema="analytics")
    op.execute("DROP SCHEMA IF EXISTS analytics CASCADE")