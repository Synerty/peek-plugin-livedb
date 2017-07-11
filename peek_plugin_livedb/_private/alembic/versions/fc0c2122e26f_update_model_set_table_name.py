"""Update model set table name

Peek Plugin Database Migration Script

Revision ID: fc0c2122e26f
Revises: 51086341adcd
Create Date: 2017-07-11 19:55:31.202057

"""

# revision identifiers, used by Alembic.
revision = 'fc0c2122e26f'
down_revision = '51086341adcd'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
import geoalchemy2


def upgrade():
    op.drop_constraint('LiveDbItem_modelSetId_fkey', 'LiveDbItem', schema='pl_livedb', type_='foreignkey')
    op.create_table('LiveDbModelSet',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('comment', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='pl_livedb'
    )
    op.create_foreign_key(None, 'LiveDbItem', 'LiveDbModelSet', ['modelSetId'], ['id'], source_schema='pl_livedb', referent_schema='pl_livedb', ondelete='CASCADE')
    op.drop_table('ModelSet', schema='pl_livedb')


def downgrade():
    op.drop_constraint(None, 'LiveDbItem', schema='pl_livedb', type_='foreignkey')
    op.create_foreign_key('LiveDbItem_modelSetId_fkey', 'LiveDbItem', 'ModelSet', ['modelSetId'], ['id'], source_schema='pl_livedb', referent_schema='pl_livedb', ondelete='CASCADE')
    op.create_table('ModelSet',
    sa.Column('id', sa.INTEGER(), server_default=sa.text('nextval(\'pl_livedb."ModelSet_id_seq"\'::regclass)'), nullable=False),
    sa.Column('name', sa.VARCHAR(), autoincrement=False, nullable=False),
    sa.Column('comment', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('id', name='ModelSet_pkey'),
    schema='pl_livedb'
    )
    op.drop_table('LiveDbModelSet', schema='pl_livedb')