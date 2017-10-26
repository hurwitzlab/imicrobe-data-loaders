import gzip
import itertools
import os
import time

import sqlalchemy as sa
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects import mysql

from imicrobe_model import models
from uproc_models import SampleToUproc, Uproc


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
    SampleToUproc.__table__.create(imicrobe_engine)
    load_sample_to_uproc_table(session=session, engine=imicrobe_engine)


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


def load_sample_to_uproc_table(session, engine):
    debug = False
    start_time = time.time()
    line_group_length = 1000
    with gzip.open('data/uproc_results.txt.gz', 'rt') as uproc_results_file:
        for line_group in grouper(uproc_results_file, line_group_length, fillvalue=None):
            line_counter = 0
            t0 = time.time()
            for line in (line_ for line_ in line_group if line_ is not None):
                line_counter += 1
                project_id, sample_id, pfam_accession, read_count = line.strip().split('\t')
                if debug:
                    print('{}\t{}\t{}\t{}'.format(project_id, sample_id, pfam_accession, read_count))

                uproc_result = session.query(
                    Uproc).filter(
                        Uproc.accession == pfam_accession).one_or_none()

                if uproc_result is None:
                    print('failed to find Pfam accession "{}"'.format(pfam_accession))
                else:
                    x = SampleToUproc(
                        sample_id=int(sample_id),
                        uproc_id=uproc_result.uproc_id,
                        read_count=int(read_count))
                    session.add(x)

            session.commit()
            print(
                'committed {} rows to "{}" table in {:5.1f}s'.format(
                    line_counter,
                    SampleToUproc.__tablename__,
                    time.time()-t0))

    print('finished loading table "{}" in {:5.1}s'.format(SampleToUproc.__tablename__, time.time()-start_time))
    print('table "{}" has {} rows'.format(SampleToUproc.__tablename__, session.query(SampleToUproc).count()))


if __name__ == '__main__':
    main()
