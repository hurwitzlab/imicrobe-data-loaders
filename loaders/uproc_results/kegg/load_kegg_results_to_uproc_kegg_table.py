"""
Run with --drop-kegg-table to drop the KEGG table

Run with --create-kegg-table to create the KEGG table

Run with --results-root-dp to write a job file for GNU Parallel.

Run with --uproc-kegg-results-fp to load one file of UProC results in to the iMicrobe database.


"""
import argparse
from collections import defaultdict
from contextlib import contextmanager
import io
import itertools
import os
import re
import sys
import time

import requests

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from loaders.uproc_results.kegg.models import Kegg_annotation, Uproc_kegg_result


def get_args():
    argparser = argparse.ArgumentParser()

    argparser.add_argument('--drop-tables',
                           action='store_true',
                           default=False,
                           help='drop UProC KEGG result table and KEGG annotation table')

    argparser.add_argument('--drop-uproc-kegg-result-table',
                           action='store_true',
                           default=False,
                           help='drop UProC KEGG result table')

    argparser.add_argument('--create-tables',
                           action='store_true',
                           default=False,
                           help='create the KEGG tables')

    argparser.add_argument('--list', action='store_true', default=False,
                           help='list row of uproc_kegg_result table')

    argparser.add_argument('--results-root-dp',
                           help='path to root of results directory tree')

    argparser.add_argument('--load-results-root-dp',
                           help='path to root of results directory tree')

    argparser.add_argument('--line-limit',
                           default=None,
                           type=int,
                           help='number of lines to print in job file')

    argparser.add_argument('--uproc-results-fp', help='path to one file of UProC results')
    argparser.parse_args()

    args = argparser.parse_args()

    return args


def main():
    args = get_args()
    #print(args)

    # connect to database on server
    # e.g. mysql+pymysql://imicrobe:<password>@localhost/imicrobe
    db_uri = os.environ.get('IMICROBE_DB_URI')
    imicrobe_engine = sa.create_engine(db_uri, echo=False)
    # reflect tables
    meta = sa.MetaData()
    meta.reflect(bind=imicrobe_engine)

    Session_class = sessionmaker(bind=imicrobe_engine)

    if args.drop_tables:
        drop_table('uproc_kegg_result', meta, imicrobe_engine)
        drop_table('kegg_annotation', meta, imicrobe_engine)
    elif args.drop_uproc_kegg_result_table:
        drop_table('uproc_kegg_result', meta, imicrobe_engine)
    elif args.create_tables:
        create_table('kegg_annotation', meta, imicrobe_engine)
        create_table('uproc_kegg_result', meta, imicrobe_engine)
    elif args.list:
        list_uproc_kegg_result_rows(Session_class, imicrobe_engine)
    elif args.results_root_dp:
        ##drop_table(SampleToUpro, engine=imicrobe_engine)
        ##SampleToUproc.__table__.create(imicrobe_engine)
        #load_sample_to_uproc_table(session=session, engine=imicrobe_engine)
        write_command_file_from_directory_tree(dir_root=args.results_root_dp,)
    elif args.load_results_root_dp:
        load_all_samples_to_uproc_kegg_table_from_directory_tree(
            dir_root=args.load_results_root_dp,
            session_class=Session_class,
            engine=imicrobe_engine,
            line_limit=args.line_limit)
    elif args.uproc_results_fp:
        load_sample_to_uproc_table_from_file(
            uproc_results_fp=args.uproc_results_fp,
            session_class=Session_class,
            engine=imicrobe_engine)
    else:
        print('specify either --results-root-dp or --uproc-results-fp')


def drop_table(table_name, meta, engine):
    # delete the relationship table first
    if table_name in meta.tables:
        meta.tables[table_name].drop(engine)
        sys.stderr.write('dropped table "{}"\n'.format(table_name))
    else:
        sys.stderr.write('table "{}" does not exist\n'.format(table_name))


def create_table(table_name, meta, engine):
    #from loaders.uproc_results.kegg.models import Kegg_annotation, Uproc_kegg_result

    #print([table for table in meta.tables])
    if table_name in meta.tables:
        print('table "{}" already exists'.format(table_name))
    elif table_name == 'kegg_annotation':
        print('creating table "{}"'.format(Kegg_annotation.__tablename__))
        Kegg_annotation.__table__.create(engine)
    elif table_name == 'uproc_kegg_result':
        print('creating table "{}"'.format(Uproc_kegg_result.__tablename__))
        Uproc_kegg_result.__table__.create(engine)
    else:
        raise Exception('unknown table "{}"'.format(table_name))


@contextmanager
def session_(session_class):
    """Provide a transactional scope around a series of operations."""
    session = session_class()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def list_uproc_kegg_result_rows(session_class, engine):
    with session_(session_class) as session:
        for row in session.query(Uproc_kegg_result).all():
            print('id: {}, kegg: {}, sample: {}, read_count: {}'.format(
                row.uproc_kegg_result_id,
                row.kegg_annotation_id,
                row.sample_id,
                row.read_count))


def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(itertools.islice(iterable, n))


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def write_command_file_from_directory_tree(dir_root):
    start_time = time.time()
    file_count = 0
    for root, dirs, files in os.walk(dir_root):
        for file in files:
            if file.endswith('.uproc.kegg'):
                file_count += 1

                uproc_results_fp = os.path.join(root, file)
                # send output down a hole because GNU Parallel will run this
                #print('python kegg/load_kegg_results_to_uproc_kegg_table.py --uproc-results-fp {} >& /dev/null'.format(uproc_results_fp))
                print('python kegg/load_kegg_results_to_uproc_kegg_table.py --uproc-results-fp {}'.format(uproc_results_fp))

    sys.stderr.write('wrote {} lines in {:5.1f}s\n'.format(file_count, time.time()-start_time))


def load_all_samples_to_uproc_kegg_table_from_directory_tree(dir_root, session_class, engine, line_limit):
    #from loaders.uproc_results.kegg.models import Kegg_annotation, Uproc_kegg_result

    # load the kegg_annotations table first
    start_time = time.time()
    t0 = time.time()
    file_count = 0
    kegg_ids = set()
    for root, dirs, files in os.walk(dir_root):
        for file_name in files:
            if file_name.endswith('.uproc.kegg'):
                file_count += 1
                uproc_kegg_results_fp = os.path.join(root, file_name)
                with open(uproc_kegg_results_fp, 'rt') as uproc_kegg_results_file:

                    # UProC results files look like this:
                    #   K01467,4208
                    #   K01990,660
                    #   K07481,434
                    #   ... and so on ...
                    t00 = time.time()
                    file_line_count = 0
                    for line in take(line_limit, uproc_kegg_results_file):
                        kegg_id, read_count = line.strip().split(',')
                        file_line_count += 1

                        kegg_ids.add(kegg_id)

                    print('{:<10.1f}s: parsed {} line(s) of file {}: "{}" in {:5.1f}s'.format(
                        time.time()-t0, file_line_count, file_count, file_name, time.time()-t00))

    print('found {} KEGG ids'.format(len(kegg_ids)))
    print(sorted(kegg_ids)[:10])

    # get all KEGG ids for the annotations that are already in the database
    with session_(session_class) as session:
        downloaded_kegg_annotations = {s[0] for s in session.query(Kegg_annotation.kegg_annotation_id).all()}
    print('found {} KEGG annotations in the database'.format(len(downloaded_kegg_annotations)))
    print(sorted(downloaded_kegg_annotations)[:10])

    kegg_annotations_needed = set()

    t0 = time.time()
    for kegg_id in kegg_ids:
        if kegg_id in downloaded_kegg_annotations:
            # no need to check the database for this kegg annotation
            #print('KEGG id {} has already been downloaded'.format(kegg_id))
            pass
        else:
            kegg_annotations_needed.add(kegg_id)

    print('need to download {} KEGG annotations'.format(len(kegg_annotations_needed)))
    print(sorted(kegg_annotations_needed)[:10])

    download_failed_kegg_ids = set()
    for kegg_id_group_ in grouper(sorted(kegg_annotations_needed), n=10, fillvalue=None):

        with session_(session_class) as session:

            kegg_id_group = [k for k in kegg_id_group_ if k is not None]
            ko_id_list = '+'.join(['ko:{}'.format(k) for k in kegg_id_group])
            kegg_annotation_response = requests.get('http://rest.kegg.jp/get/{}'.format(ko_id_list))
            if kegg_annotation_response.status_code == 200:
                ko_annotations = parse_kegg_response(kegg_annotation_response.text)
                # it can happen that some ko_ids are not found
                # in these cases there is no entry for the ko_id
                for kegg_id in sorted(kegg_id_group):
                    if kegg_id in ko_annotations:
                        downloaded_kegg_annotations.add(kegg_id)
                        session.add(
                            Kegg_annotation(
                                kegg_annotation_id=kegg_id,
                                name=ko_annotations[kegg_id]['NAME'],
                                definition=ko_annotations[kegg_id]['DEFINITION'],
                                pathway=ko_annotations[kegg_id].get('PATHWAY', ''),
                                module=ko_annotations[kegg_id].get('MODULE', '')))
                        if len(downloaded_kegg_annotations) % 100 == 0:
                            print('{} KEGG annotations downloaded in {:10.1f}s'.format(
                                len(downloaded_kegg_annotations), time.time() - t0))

                        #if len(downloaded_kegg_annotations) % 1000 == 0:
                        #    print('committing')
                        #    session.commit()
                    else:
                        download_failed_kegg_ids.add(kegg_id)
                        print('  DOWNLOAD FAILED for "{}"'.format(kegg_id))
            else:
                print('status code {} for "{}"'.format(
                    kegg_annotation_response.status_code,
                    kegg_annotation_response.url))
                download_failed_kegg_ids.update(kegg_id_group)

    print('downloaded {} KEGG ids'.format(len(downloaded_kegg_annotations)))

    print('downloaded and inserted {} KEGG annotations in {:5.1f}s\n'.format(
        len(downloaded_kegg_annotations), time.time()-start_time))

    t0 = time.time()
    uproc_kegg_results_files = []
    # load the uproc_kegg_results table last
    for root, dirs, files in os.walk(dir_root):
        for file_name in files:
            if file_name.endswith('.uproc.kegg'):
                file_count += 1
                uproc_kegg_results_fp = os.path.join(root, file_name)
                # uproc_kegg_results_fp looks like
                #   /home/u26/jklynch/usr/local/imicrobe/data/uproc/projects/148/samples/3486/ERR906934.fasta.uproc.kegg
                # get the sample id from the last directory name
                _, sample_id = os.path.split(os.path.dirname(uproc_kegg_results_fp))
                sample_id = int(sample_id)

                t00 = time.time()
                file_results_count = 0
                try:
                    with open(uproc_kegg_results_fp, 'rt') as uproc_kegg_results_file, session_(session_class) as session:

                        # UProC results files look like this:
                        #   K01467,4208
                        #   K01990,660
                        #   K07481,434
                        #   ... and so on ...
                        for line in take(line_limit, uproc_kegg_results_file):
                            kegg_id, read_count = line.strip().split(',')
                            if kegg_id in download_failed_kegg_ids:
                                pass
                            elif kegg_id in downloaded_kegg_annotations:
                                session.add(
                                    Uproc_kegg_result(
                                        sample_id=sample_id,
                                        kegg_annotation_id=kegg_id,
                                        read_count=int(read_count)))
                                file_results_count += 1
                            else:
                                print('what happened? "{}"'.format(kegg_id))

                        uproc_kegg_results_files.append(file_name)

                    print('finished parsing file {}: "{}" with {} results in {:5.1f}s'.format(
                        len(uproc_kegg_results_files), uproc_kegg_results_fp, file_results_count, time.time()-t00))

                except Exception as e:
                    # database integrity errors land here
                    print(e)
                    print('failed to insert data from file "{}"'.format(uproc_kegg_results_fp))

    print('inserted {} UProC KEGG results in {:5.1f}s\n'.format(
        len(downloaded_kegg_annotations), time.time()-t0))

    print('failed to download {} annotation(s):\n\t{}'.format(
        len(download_failed_kegg_ids), '\n\t'.join(download_failed_kegg_ids)))

    print('total time: {:5.1f}s'.format(time.time()-start_time))


def load_sample_to_uproc_table_from_file(uproc_results_fp, session_class, engine):
    #from loaders.uproc_results.kegg.models import Kegg_annotation, Uproc_kegg_result

    debug = True
    if debug:
        print('reading UProC results from "{}"'.format(uproc_results_fp))
    t0 = time.time()
    with open(uproc_results_fp, 'rt') as uproc_results_file:
        line_count = 0
        # uproc_results_fp looks like
        #   /home/u26/jklynch/usr/local/imicrobe/data/uproc/projects/148/samples/3486/ERR906934.fasta.uproc.kegg
        p, sample_id = os.path.split(os.path.dirname(uproc_results_fp))
        sample_id = int(sample_id)

        # UProC results files look like this:
        #   K01467,4208
        #   K01990,660
        #   K07481,434
        #   ... and so on ...
        for line in uproc_results_file:
            line_count += 1
            kegg_id, read_count = line.strip().split(',')

            kegg_result = session.query(
                Kegg_annotation).filter(
                    Kegg_annotation.kegg_annotation_id == kegg_id).one_or_none()

            if kegg_result is None:
                print('failed to find KEGG id "{}"'.format(kegg_id))
                print('  downloading KEGG annotation')

                kegg_annotation_response = requests.get('http://rest.kegg.jp/get/ko:{}'.format(kegg_id))
                if kegg_annotation_response.status_code == 200:
                    name, definition, pathway, module = parse_kegg_response(kegg_annotation_response.text)
                    session.add(
                        Kegg_annotation(
                            kegg_annotation_id=kegg_id,
                            name=name,
                            definition=definition,
                            pathway=pathway,
                            module=module))
                    session.commit() # errors without this
                    session.add(
                        Uproc_kegg_result(
                            sample_id=int(sample_id),
                            kegg_annotation_id=kegg_id,
                            read_count=int(read_count)))
                else:
                    print('failed to get KEGG response for "{}"'.format(kegg_id))
            else:
                # already have annotation for kegg_id in database
                session.add(
                    Uproc_kegg_result(
                        sample_id=int(sample_id),
                        kegg_annotation_id=kegg_result.kegg_annotation_id,
                        read_count=int(read_count)))

    session.commit()
    if debug:
        print(
            '  committed {} rows to "{}" table in {:5.1f}s'.format(
                line_count,
                Uproc_kegg_result.__tablename__,
                time.time() - t0))


kegg_orthology_field_re = re.compile(r'^(?P<field_name>[A-Z]+)?(\s+)(?P<field_value>.+)$')

def parse_kegg_response(response):
    """ response looks like this:
            ENTRY       K01467                      KO
            NAME        ampC
            DEFINITION  beta-lactamase class C [EC:3.5.2.6]
            PATHWAY     ko01501  beta-Lactam resistance
                        ko02020  Two-component system
            MODULE      M00628  beta-Lactam resistance, AmpC system
            ...
            ENTRY       K00154                      KO
            NAME        E1.2.1.68
            DEFINITION  coniferyl-aldehyde dehydrogenase [EC:1.2.1.68]
            BRITE       Enzymes [BR:ko01000]
                         1. Oxidoreductases
                          1.2  Acting on the aldehyde or oxo group of donors
                           1.2.1  With NAD+ or NADP+ as acceptor
                            1.2.1.68  coniferyl-aldehyde dehydrogenase
                             K00154  E1.2.1.68; coniferyl-aldehyde dehydrogenase
            DBLINKS     COG: COG1012
                        GO: 0050269
            GENES       GQU: AWC35_21175
                        CED: LH89_09310 LH89_19560
                        SMW: SMWW4_v1c32370
                        SMAR: SM39_2711
                        SMAC: SMDB11_2482
            ...

    return: a dictionary of dictionaries that looks like this
        {
            'K01467': {
                'ENTRY': 'K01467                      KO',
                'NAME': 'ampC',
                'DEFINITION': '',
                'PATHWAY': '',
                'MODULE': '',
                ...
            },
            'K00154': {
                'ENTRY': 'K00154                      KO',
                'NAME': 'E1.2.1.68',
                'DEFINITION': '',
                'PATHWAY': '',
                'MODULE': '',
                ...
            }

        }
    """

    debug = False

    all_entries = defaultdict(lambda : defaultdict(list))
    kegg_id = None
    field_name = None
    for line in io.StringIO(response).readlines():
        field_match = kegg_orthology_field_re.search(line.rstrip())
        if field_match is None:
            # this line is /// to separate entries
            pass
        elif 'field_name' in field_match.groupdict():
            field_name = field_match.group('field_name')
            field_value = field_match.group('field_value')
            if field_name == 'ENTRY':
                kegg_id, *_ = field_value.split(' ')
                #print('KEGG id: "{}"'.format(kegg_id))
        else:
            # just a field value is present
            pass

        all_entries[kegg_id][field_name].append(field_value)

    return all_entries


if __name__ == '__main__':
    main()
