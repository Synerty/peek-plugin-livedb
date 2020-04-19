"""added queue table

Peek Plugin Database Migration Script

Revision ID: d4ffb9c5cb27
Revises: 19b2a8d326ad
Create Date: 2020-04-18 16:15:31.166637

"""

# revision identifiers, used by Alembic.
revision = 'd4ffb9c5cb27'
down_revision = '19b2a8d326ad'
branch_labels = None
depends_on = None

import sqlalchemy as sa
from alembic import op


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('LiveDbRawValueQueue',
                    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
                    sa.Column('modelSetId', sa.Integer(), nullable=False),
                    sa.Column('key', sa.String(), nullable=False),
                    sa.Column('rawValue', sa.String(), nullable=False),
                    sa.PrimaryKeyConstraint('id', 'modelSetId', 'key', 'rawValue'),
                    schema='pl_livedb'
                    )

    op.add_column('LiveDbModelSet', sa.Column('key', sa.String(), nullable=True),
                  schema='pl_livedb')

    op.execute('''UPDATE "pl_livedb"."LiveDbModelSet"
                  SET "key" = "name"''')

    op.alter_column('LiveDbModelSet', 'key', type_=sa.String(), nullable=False,
                    schema='pl_livedb')
    op.create_unique_constraint('idx_liveDbModelSet_key', 'LiveDbModelSet', ['key'],
                                schema='pl_livedb')

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('idx_liveDbModelSet_key', 'LiveDbModelSet', schema='pl_livedb',
                       type_='unique')
    op.drop_column('LiveDbModelSet', 'key', schema='pl_livedb')
    op.drop_table('LiveDbRawValueQueue', schema='pl_livedb')
    # ### end Alembic commands ###
