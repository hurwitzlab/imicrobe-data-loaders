import gzip
import itertools
import os
import time

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from loaders.uproc_results.uproc_models import SampleToUproc, Uproc


def main():
    # connect to database on server
    # e.g. mysql+pymysql://imicrobe:<password>@localhost/imicrobe
    db_uri = os.environ.get('IMICROBE_DB_URI')
    imicrobe_engine = sa.create_engine(db_uri, echo=False)
    # reflect tables
    meta = sa.MetaData()
    meta.reflect(bind=imicrobe_engine)

    Session = sessionmaker(bind=imicrobe_engine)
    session = Session()

    drop_table(SampleToUproc, engine=imicrobe_engine)
    drop_table(Uproc, engine=imicrobe_engine)
    Uproc.__table__.create(imicrobe_engine)
    load_pfam_table(session=session, engine=imicrobe_engine)
    # how many rows in the Uproc table?
    uproc_row_count = session.query(Uproc).count()
    print('{} rows in the uproc table after inserting data from pfamA.txt.gz'.format(uproc_row_count))
    load_dead_pfam(session=session, engine=imicrobe_engine)
    # how many rows in the Uproc table?
    uproc_row_count = session.query(Uproc).count()
    print('{} rows in the uproc table after inserting data from dead_family.txt.gz'.format(uproc_row_count))


def drop_table(table, engine):
    # delete the relationship table first
    try:
        table.__table__.drop(engine)
        print('dropped table "{}"'.format(table.__tablename__))
    except Exception as e:
        print(e)


def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(itertools.islice(iterable, n))


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def load_pfam_table(session, engine):
    debug = False
    line_group_length = 2000
    pfamA_fp = 'data/pfamA.txt.gz'
    # had problems on myo with U+009D in PF01298 description
    # not a problem with imicrobe-vm on my laptop
    # this is the error:
    #   UnicodeEncodeError: 'latin-1' codec can't encode characters in position 1089-1090: ordinal not in range(256)
    # why is 'latin-1' codec being used?
    # specifying encoding='latin-1' and errors='replace' solves the problem on myo
    with gzip.open(pfamA_fp, 'rt', encoding='latin-1', errors='replace') as pfamA_file:
        for line_group in grouper(pfamA_file.readlines(), line_group_length, fillvalue=None):
            line_counter = 0
            t0 = time.time()
            for line in (line_ for line_ in line_group if line_ is not None):
                line_counter += 1
                pfam_acc, pfam_identifier, pfam_aliases, pfam_name, _, _, _, _, description, *the_rest = line.strip().split('\t')

                if debug:
                    print('pfam accession  : {}'.format(pfam_acc))
                    print('pfam identifier : {}'.format(pfam_identifier))
                    print('pfam aliases    : {}'.format(pfam_aliases))
                    print('pfam name       : {}'.format(pfam_name))
                    print('description     : {}'.format(description))

                if session.query(Uproc).filter(Uproc.accession==pfam_acc).one_or_none():
                    pass
                    #print('{} is already in the database'.format(pfam_acc))
                else:
                    # insert
                    session.add(
                        Uproc(
                            accession=pfam_acc,
                            identifier=pfam_identifier,
                            name=pfam_name,
                            description=description))

            session.commit()
            print(
                'committed {} rows in {:5.1f}s'.format(
                    line_counter,
                    time.time()-t0))

    print('table "{}" has {} rows'.format(Uproc.__tablename__, session.query(Uproc).count()))


def load_dead_pfam(session, engine):
    # there are some strange rows in this file
    debug = False
    dead_pfam_fp = 'data/dead_family.txt.gz'
    with gzip.open(dead_pfam_fp, 'rt') as dead_pfam_file:
        for line in dead_pfam_file:
            dead_pfam_accession, pfam_identifier, pfam_cause_of_death, *_ = line.strip().split('\t')
            if debug:
                print('************* line:\n\t{}'.format(line))
                print(dead_pfam_accession)
                print(pfam_identifier)
                print(pfam_cause_of_death)
                print('\n')
            if session.query(Uproc).filter(Uproc.accession == dead_pfam_accession).one_or_none():
                print('dead Pfam accession "{}" is already in table uproc'.format(dead_pfam_accession))
            else:
                # insert
                session.add(
                    Uproc(
                        accession=dead_pfam_accession,
                        identifier=pfam_identifier,
                        name='dead',
                        description=pfam_cause_of_death))

        session.commit()

    print('table "{}" has {} rows'.format(Uproc.__tablename__, session.query(Uproc).count()))


    """
    pfam_url = 'http://pfam.xfam.org'
    pfam_family_url = urllib.parse.urljoin(pfam_url, '/family')

    for sample_uproc_id_i in session.query(models.Sample_uproc.uproc_id).order_by(models.Sample_uproc.uproc_id).distinct().limit(10):
        print(sample_uproc_id_i)
        # is the PFAM annotation already in the database?
        if session.query(Uproc).filter(Uproc.pfam_annot_id == sample_uproc_id_i.uproc_id).one_or_none() is None:
            response = requests.get(
                url=pfam_family_url,
                params={'acc': sample_uproc_id_i, 'output': 'xml'})
            response_root = ET.fromstring(response.text)
            description = response_root[0][1].text
            pfam_annot_i = Uproc(pfam_acc=sample_uproc_id_i, annot=description)
            session.add(pfam_annot_i)
        else:
            print('{} is already in Uproc table'.format(sample_uproc_id_i))

    session.commit()
    session.close()
    """


if __name__ == '__main__':
    main()
