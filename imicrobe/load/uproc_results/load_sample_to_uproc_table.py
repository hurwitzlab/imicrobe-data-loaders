import argparse
import itertools
import os
import sys
import time

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from imicrobe.uproc_results.uproc_models import SampleToUproc, Uproc


def get_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--results-root-dp', help='path to root of results directory tree')
    argparser.add_argument('--uproc-results-fp', help='path to one file of UProC results')
    argparser.add_argument('--line-limit', default=None, type=int, help='number of lines to print')
    argparser.parse_args()

    args = argparser.parse_args()

    return args


def main():
    args = get_args()
    #print(args)

    # connect to database on server
    # e.g. mysql+pymysql://load:<password>@localhost/load
    db_uri = os.environ.get('IMICROBE_DB_URI')
    imicrobe_engine = sa.create_engine(db_uri, echo=False)
    # reflect tables
    meta = sa.MetaData()
    meta.reflect(bind=imicrobe_engine)

    Session = sessionmaker(bind=imicrobe_engine)
    session = Session()

    if args.results_root_dp:
        drop_table(SampleToUproc, engine=imicrobe_engine)
        SampleToUproc.__table__.create(imicrobe_engine)
        #load_sample_to_uproc_table(session=session, engine=imicrobe_engine)
        write_command_file_from_directory_tree(
            dir_root=args.results_root_dp,
            session=session,
            engine=imicrobe_engine)
    elif args.uproc_results_fp:
        load_sample_to_uproc_table_from_file(
            uproc_results_fp=args.uproc_results_fp,
            session=session,
            engine=imicrobe_engine)
    else:
        print('specify either --results-root-dp or --uproc-results-fp')


def drop_table(table, engine):
    # delete the relationship table first
    try:
        table.__table__.drop(engine)
        sys.stderr.write('dropped table "{}"\n'.format(table.__tablename__))
    except Exception as e:
        sys.stderr.write(e)
        sys.stderr.write('\n')


def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(itertools.islice(iterable, n))


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def write_command_file_from_directory_tree(dir_root, session, engine):
    start_time = time.time()
    file_count = 0
    for root, dirs, files in os.walk(dir_root):
        for file in files:
            if file.endswith('.uproc'):
                file_count += 1

                uproc_results_fp = os.path.join(root, file)
                # send output down a hole because GNU Parallel will run this
                print('python load_sample_to_uproc_table.py --uproc-results-fp {} >& /dev/null'.format(uproc_results_fp))

    sys.stderr.write('wrote {} lines in {:5.1f}s\n'.format(file_count, time.time()-start_time))


def load_sample_to_uproc_table_from_file(uproc_results_fp, session, engine):
    debug = True
    if debug:
        print('reading UProC results from "{}"'.format(uproc_results_fp))
    t0 = time.time()
    with open(uproc_results_fp, 'rt') as uproc_results_file:
        line_count = 0
        # uproc_results_fp looks like
        #   /home/u26/jklynch/usr/local/imicrobe/data/uproc/projects/148/samples/3486/ERR906934.fasta.uproc
        p, sample_id = os.path.split(os.path.dirname(uproc_results_fp))
        sample_id = int(sample_id)

        for line in uproc_results_file:
            line_count += 1
            pfam_accession, read_count = line.strip().split(',')

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
    if debug:
        print(
            '  committed {} rows to "{}" table in {:5.1f}s'.format(
                line_count,
                SampleToUproc.__tablename__,
                time.time() - t0))


if __name__ == '__main__':
    main()
