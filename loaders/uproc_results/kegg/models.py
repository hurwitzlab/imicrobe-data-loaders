import sqlalchemy as sa
from sqlalchemy.orm import relationship
from sqlalchemy.dialects import mysql

from imicrobe_model import models


class Uproc_kegg_result(models.Model):
    __tablename__ = 'uproc_kegg_result'
    __table_args__ = (
        sa.UniqueConstraint('kegg_annotation_id', 'sample_id'), {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'})

    uproc_kegg_id = sa.Column(
        'uproc_kegg_result_id',
        mysql.INTEGER(unsigned=True),
        autoincrement=True,
        primary_key=True)

    sample_id = sa.Column(
        'sample_id',
        mysql.INTEGER(unsigned=True),
        sa.ForeignKey('sample.sample_id', ondelete='CASCADE'),
        nullable=False)

    kegg_annotation_id = sa.Column(
        'kegg_annotation_id',
        sa.String(16),
        sa.ForeignKey('kegg_annotation.kegg_annotation_id', ondelete='CASCADE'),
        nullable=False)

    read_count = sa.Column('read_count', sa.Integer)


class Kegg_annotation(models.Model):
    __tablename__ = 'kegg_annotation'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'}

    # K01467
    kegg_annotation_id = sa.Column(
        'kegg_annotation_id',
        sa.String(16),
        primary_key=True)

    # ampC
    name = sa.Column('name', sa.VARCHAR(80))

    # beta-lactamase class C [EC:3.5.2.6]
    definition = sa.Column('definition', sa.VARCHAR(200))

    # ko01501  beta-Lactam resistance
    # ko02020  Two-component system
    pathway = sa.Column('pathway', sa.TEXT())

    # M00628  beta-Lactam resistance, AmpC system
    module = sa.Column('module', sa.TEXT())
