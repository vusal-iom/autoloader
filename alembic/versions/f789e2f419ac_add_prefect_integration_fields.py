"""add_prefect_integration_fields

Revision ID: f789e2f419ac
Revises: 67371aeb5ae0
Create Date: 2025-11-07 08:37:15.416350

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f789e2f419ac'
down_revision: Union[str, None] = '67371aeb5ae0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add Prefect deployment tracking fields
    op.add_column('ingestions',
        sa.Column('prefect_deployment_id', sa.String(255), nullable=True)
    )
    op.add_column('ingestions',
        sa.Column('prefect_flow_id', sa.String(255), nullable=True)
    )

    # Add index for faster lookups
    op.create_index(
        'ix_ingestions_prefect_deployment_id',
        'ingestions',
        ['prefect_deployment_id']
    )


def downgrade() -> None:
    # Drop index and columns
    op.drop_index('ix_ingestions_prefect_deployment_id', 'ingestions')
    op.drop_column('ingestions', 'prefect_flow_id')
    op.drop_column('ingestions', 'prefect_deployment_id')
