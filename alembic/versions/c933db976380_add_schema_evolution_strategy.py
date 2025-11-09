"""add_schema_evolution_strategy

Revision ID: c933db976380
Revises: f789e2f419ac
Create Date: 2025-11-09 13:26:53.282427

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c933db976380'
down_revision: Union[str, None] = 'f789e2f419ac'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add on_schema_change column to ingestions table
    op.add_column('ingestions',
        sa.Column('on_schema_change', sa.String(50), nullable=False, server_default='ignore')
    )

    # Add schema evolution tracking columns to schema_versions table
    op.add_column('schema_versions',
        sa.Column('changes_json', sa.JSON(), nullable=True)
    )
    op.add_column('schema_versions',
        sa.Column('strategy_applied', sa.String(50), nullable=True)
    )


def downgrade() -> None:
    # Remove columns from schema_versions
    op.drop_column('schema_versions', 'strategy_applied')
    op.drop_column('schema_versions', 'changes_json')

    # Remove column from ingestions
    op.drop_column('ingestions', 'on_schema_change')
