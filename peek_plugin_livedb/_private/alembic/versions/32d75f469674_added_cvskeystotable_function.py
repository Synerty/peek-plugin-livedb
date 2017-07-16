"""Added cvsKeysToTable function

Peek Plugin Database Migration Script

Revision ID: 32d75f469674
Revises: ab9da4532175
Create Date: 2017-07-16 21:52:46.619578

"""

# revision identifiers, used by Alembic.
revision = '32d75f469674'
down_revision = 'ab9da4532175'
branch_labels = None
depends_on = None

from alembic import op

from sqlalchemy.dialects.mssql.base import MSDialect
from sqlalchemy.dialects.postgresql.base import PGDialect


def isMssqlDialect():
    return isinstance(op.get_bind().engine.dialect, MSDialect)


def isPostGreSQLDialect():
    return isinstance(op.get_bind().engine.dialect, PGDialect)


def upgrade():
    msSql = '''CREATE FUNCTION [pl_livedb].[csvKeysToTable](@input AS Varchar(max) )
RETURNS
      @Result TABLE(Value varchar(100))
AS
BEGIN
      DECLARE @str VARCHAR(20)
      DECLARE @ind Int
      IF(@input is not null)
      BEGIN
            SET @ind = CharIndex(',',@input)
            WHILE @ind > 0
            BEGIN
                  SET @str = SUBSTRING(@input,1,@ind-1)
                  SET @input = SUBSTRING(@input,@ind+1,LEN(@input)-@ind)
                  INSERT INTO @Result values (@str)
                  SET @ind = CharIndex(',',@input)
            END
            SET @str = @input
            INSERT INTO @Result values (@str)
      END
      RETURN
END
'''

    if isMssqlDialect():
        op.execute(msSql)


def downgrade():
    raise NotImplementedError()
