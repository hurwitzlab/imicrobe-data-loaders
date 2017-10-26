import sqlalchemy as sa
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects import mysql

from imicrobe_model import models


class Uproc(models.Model):
    __tablename__ = 'uproc'
    uproc_id = sa.Column(
        'uproc_id',
        mysql.INTEGER(unsigned=True),
        primary_key=True)

    accession = sa.Column('accession', sa.VARCHAR(16), unique=True)
    identifier = sa.Column('identifier', sa.VARCHAR(16)) # these are not unique? cf 'Peptidase_S26'
    name = sa.Column('name', sa.VARCHAR(80))
    description = sa.Column('description', sa.TEXT)

    sample_list = relationship('Sample', secondary='sample_to_uproc')

    mysql_engine = 'InnoDB'
    mysql_charset = 'utf-8'


class SampleToUproc(models.Model):
    __tablename__ = 'sample_to_uproc'
    sample_to_uproc_id = sa.Column(
        'sample_to_uproc_id',
        mysql.INTEGER(unsigned=True),
        primary_key=True)

    sample_id = sa.Column(
        'sample_id',
        mysql.INTEGER(unsigned=True),
        sa.ForeignKey('sample.sample_id', ondelete='CASCADE'),
        nullable=False)
    uproc_id = sa.Column(
        'uproc_id',
        mysql.INTEGER(unsigned=True),
        sa.ForeignKey('uproc.uproc_id', ondelete='CASCADE'),
        nullable=False)

    __table_args__ = (
        sa.UniqueConstraint('sample_id', 'uproc_id'), )

    read_count = sa.Column('read_count', sa.Integer)

    mysql_engine = 'InnoDB'
    mysql_charset = 'utf-8'
