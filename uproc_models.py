import sqlalchemy as sa
from sqlalchemy.orm import relationship
from sqlalchemy.dialects import mysql

from imicrobe_model import models


class Uproc(models.Model):
    __tablename__ = 'uproc'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'}

    uproc_id = sa.Column(
        'uproc_id',
        mysql.INTEGER(unsigned=True),
        primary_key=True)

    accession = sa.Column('accession', sa.VARCHAR(16), unique=True)
    identifier = sa.Column('identifier', sa.VARCHAR(16)) # these are not unique? cf 'Peptidase_S26'
    name = sa.Column('name', sa.VARCHAR(80))
    description = sa.Column('description', sa.TEXT)

    sample_list = relationship('Sample', secondary='sample_to_uproc')



class SampleToUproc(models.Model):
    __tablename__ = 'sample_to_uproc'
    __table_args__ = (
        sa.UniqueConstraint('sample_id', 'uproc_id'), {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'})

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

    read_count = sa.Column('read_count', sa.Integer)
