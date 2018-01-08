import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.dialects import mysql

from loader.imicrobe import models


"""
-- KEGG, PFAM
create table protein_type (
    protein_type_id int unsigned not null auto_increment primary key,
    type varchar(100) not null,
    unique (type)
) ENGINE=InnoDB DEFAULT CHARSET='utf8';
"""
class Protein_type(models.Model):
    __tablename__ = 'protein_type'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'}

    protein_type_id = sa.Column(
        'protein_type_id',
        mysql.INTEGER(unsigned=True),
        nullable=False,
        primary_key=True)

    type_ = sa.Column('type', sa.VARCHAR(100), unique=True)


"""
-- UProC, Interpro
create table protein_evidence_type (
    protein_evidence_type_id int unsigned not null auto_increment primary key,
    type varchar(100) not null,
    unique (type)
) ENGINE=InnoDB DEFAULT CHARSET='utf8';
"""
class Protein_evidence_type(models.Model):
    __tablename__ = 'protein_evidence_type'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'}

    protein_evidence_type_id = sa.Column(
        'protein_evidence_type_id',
        mysql.INTEGER(unsigned=True),
        nullable=False,
        primary_key=True)

    type_ = sa.Column('type', sa.VARCHAR(100), nullable=False, unique=True)


"""
-- A KEGG or PFAM annotation
create table protein (
    protein_id int unsigned not null auto_increment primary key,
    protein_type_id int unsigned not null,
    accession varchar(100) not null,
    description text,
    unique (accession),
    foreign key (protein_type_id) references protein_type (protein_type_id)
) ENGINE=InnoDB DEFAULT CHARSET='utf8';
"""
class Protein(models.Model):
    __tablename__ = 'protein'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'}

    protein_id = sa.Column(
        'protein_id',
        mysql.INTEGER(unsigned=True),
        nullable=False,
        primary_key=True)

    protein_type_id = sa.Column(
        'protein_type_id',
        mysql.INTEGER(unsigned=True),
        sa.ForeignKey('protein_type.protein_type_id', name='fk_pfkpt', ondelete='CASCADE'),
        nullable=False)

    accession = sa.Column('accession', sa.VARCHAR(100), nullable=False, unique=True)
    description = sa.Column('description', sa.TEXT())

    protein_type = orm.relationship('Protein_type')


"""
-- the relationship
create table sample_to_protein (
    sample_to_protein_id int unsigned not null auto_increment primary key,
    sample_id int unsigned not null,
    protein_id int unsigned not null,
    unique (sample_id, protein_id),
    foreign key (sample_id) references sample (sample_id),
    foreign key (protein_id) references protein (protein_id)
) ENGINE=InnoDB DEFAULT CHARSET='utf8';
"""
class Sample_to_protein(models.Model):
    __tablename__ = 'sample_to_protein'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'}

    sample_to_protein_id = sa.Column(
        'sample_to_protein_id',
        mysql.INTEGER(unsigned=True),
        nullable=False,
        primary_key=True)

    sample_id = sa.Column(
        'sample_id',
        mysql.INTEGER(unsigned=True),
        sa.ForeignKey('sample.sample_id', name='fk_stpfks', ondelete='CASCADE'),
        nullable=False)

    protein_id = sa.Column(
        'protein_id',
        mysql.INTEGER(unsigned=True),
        sa.ForeignKey('protein.protein_id', name='fk_stpfkp', ondelete='CASCADE'),
        nullable=False)

    # seems like protein evidence type should be here
    protein_evidence_type_id = sa.Column(
        'protein_evidence_type_id',
        mysql.INTEGER(unsigned=True),
        sa.ForeignKey('protein_evidence_type.protein_evidence_type_id', name='fk_stpfkpet', ondelete='CASCADE'),
        nullable=False)

    read_count = sa.Column('read_count', sa.Integer(), nullable=False)

    sa.UniqueConstraint('sample_id', 'protein_id')


"""
-- the tool supporting the annotation to the sample
create table protein_evidence (
    protein_evidence_id int unsigned not null auto_increment primary key,
    sample_to_protein_id int unsigned not null,
    protein_evidence_type_id int unsigned not null,
    unique (sample_to_protein_id, protein_evidence_type_id),
    foreign key (sample_to_protein_id) references sample_to_protein (sample_to_protein_id),
    foreign key (protein_evidence_type_id) references protein_evidence_type (protein_evidence_type_id)
) ENGINE=InnoDB DEFAULT CHARSET='utf8';
"""
class Protein_evidence(models.Model):
    __tablename__ = 'protein_evidence'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'}

    protein_evidence_id = sa.Column(
        'protein_evidence_id',
        mysql.INTEGER(unsigned=True),
        nullable=False,
        primary_key=True)

    sample_to_protein_id = sa.Column(
        'sample_to_protein_id',
        mysql.INTEGER(unsigned=True),
        sa.ForeignKey('sample_to_protein.sample_to_protein_id', name='fk_pefkstp', ondelete='CASCADE'),
        nullable=False)

    protein_evidence_type_id = sa.Column(
        'protein_evidence_type_id',
        mysql.INTEGER(unsigned=True),
        sa.ForeignKey('protein_evidence_type.protein_evidence_type_id', name='fk_pefkpet', ondelete='CASCADE'),
        nullable=False)

    sa.UniqueConstraint('sample_to_protein_id', 'protein_evidence_type_id')
