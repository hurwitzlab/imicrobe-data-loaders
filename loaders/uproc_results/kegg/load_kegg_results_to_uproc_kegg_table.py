"""
Run with --drop-kegg-table to drop the KEGG table

Run with --create-kegg-table to create the KEGG table

Run with --results-root-dp to write a job file for GNU Parallel.

Run with --uproc-kegg-results-fp to load one file of UProC results in to the iMicrobe database.


"""
import argparse
import io
import itertools
import os
import re
import sys
import time

import requests

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker


def get_args():
    argparser = argparse.ArgumentParser()

    argparser.add_argument('--drop-tables',
                           action='store_true',
                           default=False,
                           help='drop the KEGG table')

    argparser.add_argument('--create-tables',
                           action='store_true',
                           default=False,
                           help='create the KEGG table')

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

    Session = sessionmaker(bind=imicrobe_engine)
    session = Session()

    if args.drop_tables:
        drop_tables(meta, imicrobe_engine)
    elif args.create_tables:
        create_tables(meta, imicrobe_engine)
    elif args.results_root_dp:
        ##drop_table(SampleToUpro, engine=imicrobe_engine)
        ##SampleToUproc.__table__.create(imicrobe_engine)
        #load_sample_to_uproc_table(session=session, engine=imicrobe_engine)
        write_command_file_from_directory_tree(dir_root=args.results_root_dp,)
    elif args.load_results_root_dp:
        load_all_samples_to_uproc_kegg_table_from_directory_tree(
            dir_root=args.load_results_root_dp,
            session=session,
            engine=imicrobe_engine,
            line_limit=args.line_limit)
    elif args.uproc_results_fp:
        load_sample_to_uproc_table_from_file(
            uproc_results_fp=args.uproc_results_fp,
            session=session,
            engine=imicrobe_engine)
    else:
        print('specify either --results-root-dp or --uproc-results-fp')


def drop_tables(meta, engine):
    drop_table('uproc_kegg_result', meta, engine)
    drop_table('kegg_annotation', meta, engine)


def drop_table(table_name, meta, engine):
    # delete the relationship table first
    if table_name in meta.tables:
        meta.tables[table_name].drop(engine)
        sys.stderr.write('dropped table "{}"\n'.format(table_name))
    else:
        sys.stderr.write('table "{}" does not exist\n'.format(table_name))


def create_tables(meta, engine):
    from loaders.uproc_results.kegg.models import Kegg_annotation, Uproc_kegg_result

    Kegg_annotation.__table__.create(engine)
    Uproc_kegg_result.__table__.create(engine)


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


def load_all_samples_to_uproc_kegg_table_from_directory_tree(dir_root, session, engine, line_limit):
    from loaders.uproc_results.kegg.models import Kegg_annotation, Uproc_kegg_result

    # load the kegg_annotations table first
    start_time = time.time()
    t0 = time.time()
    file_count = 0
    downloaded_kegg_annotations = set()
    download_failed_kegg_ids = set()
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
                    file_downloaded_kegg_annotations = []
                    for line in take(line_limit, uproc_kegg_results_file):
                        kegg_id, read_count = line.strip().split(',')
                        file_line_count += 1

                        if kegg_id in downloaded_kegg_annotations:
                            # no need to check the database for this kegg annotation
                            #print('KEGG id {} has already been downloaded'.format(kegg_id))
                            pass
                        elif kegg_id in download_failed_kegg_ids:
                            print('KEGG id {} can not be downloaded')
                        else:
                            kegg_annotation = session.query(
                                Kegg_annotation).filter(
                                    Kegg_annotation.kegg_annotation_id == kegg_id).one_or_none()
                            if kegg_annotation is not None:
                                # no need to insert this kegg_id in the kegg_annotations table
                                # why was it not in downloaded_kegg_annotations?
                                pass
                            else:
                                # download and insert
                                #print('failed to find KEGG id "{}"'.format(kegg_id))
                                #print('  downloading KEGG annotation {}'.format(len(downloaded_kegg_annotations) + 1))

                                kegg_annotation_response = requests.get('http://rest.kegg.jp/get/ko:{}'.format(kegg_id))
                                if kegg_annotation_response.status_code == 200:
                                    name, definition, pathway, module = parse_kegg_response(
                                        kegg_annotation_response.text)
                                    session.add(
                                        Kegg_annotation(
                                            kegg_annotation_id=kegg_id,
                                            name=name,
                                            definition=definition,
                                            pathway=pathway,
                                            module=module))

                                    file_downloaded_kegg_annotations.append(kegg_id)
                                    downloaded_kegg_annotations.add(kegg_id)

                                else:
                                    print('  DOWNLOAD FAILED for "{}"'.format(kegg_id))
                                    download_failed_kegg_ids.add(kegg_id)
                    print('{:<10.1f}s: parsed {} line(s) of file {}: "{}" in {:5.1f}s'.format(
                        time.time()-t0, file_line_count, file_count, file_name, time.time()-t00))
                    print('  downloaded {} of {} KEGG ids'.format(
                        len(file_downloaded_kegg_annotations), file_line_count))
    # commit all kegg annotations at the end
    session.commit()

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
                with open(uproc_kegg_results_fp, 'rt') as uproc_kegg_results_file:

                    # UProC results files look like this:
                    #   K01467,4208
                    #   K01990,660
                    #   K07481,434
                    #   ... and so on ...
                    for line in take(line_limit, uproc_kegg_results_file):
                        kegg_id, read_count = line.strip().split(',')
                        if kegg_id in download_failed_kegg_ids:
                            pass
                        else:
                            session.add(
                                Uproc_kegg_result(
                                    sample_id=sample_id,
                                    kegg_annotation_id=kegg_id,
                                    read_count=int(read_count)))
                            file_results_count += 1

                    uproc_kegg_results_files.append(file_name)

                print('finished file {}: "{}" with {} results in {:5.1f}s'.format(
                    len(uproc_kegg_results_files), file_name, file_results_count, time.time()-t00))

        # commit after parsing each file
        session.commit()

    print('downloaded and inserted {} UProC KEGG results in {:5.1f}s\n'.format(
        len(downloaded_kegg_annotations), time.time()-t0))

    print('failed to download {} annotation(s):\n\t{}'.format(
        len(download_failed_kegg_ids), '\n\t'.join(download_failed_kegg_ids)))

    print('total time: {:5.1f}s'.format(time.time()-start_time))


def load_sample_to_uproc_table_from_file(uproc_results_fp, session, engine):
    from loaders.uproc_results.kegg.models import Kegg_annotation, Uproc_kegg_result

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


name_re = re.compile(r'^NAME(\s+)(?P<name>.+)$')
definition_re = re.compile(r'^DEFINITION(\s+)(?P<definition>.+)$')
pathway_re = re.compile(r'^(PATHWAY)?(\s+)(?P<pathway>.+)$')
module_re = re.compile(r'^(MODULE)?(\s+)(?P<module>.+)$')

def parse_kegg_response(response):
    """ response looks like this:
          ENTRY       K01467                      KO
          NAME        ampC
          DEFINITION  beta-lactamase class C [EC:3.5.2.6]
          PATHWAY     ko01501  beta-Lactam resistance
                      ko02020  Two-component system
          MODULE      M00628  beta-Lactam resistance, AmpC system
    """

    debug = False

    response_buffer = io.StringIO(response)
    entry_line = response_buffer.readline()
    name_line = response_buffer.readline()
    name = name_re.search(name_line).group('name')
    if debug: print('  name: "{}"'.format(name))
    definition_line = response_buffer.readline()
    definition = definition_re.search(definition_line).group('definition')
    if debug: print('  definition: "{}"'.format(definition))

    pathway = []
    module = []
    parsing_section = ''
    for line in response_buffer.readlines():
        line = line.rstrip()

        if line.startswith('PATHWAY'):
            parsing_section = 'pathway'
        elif line.startswith('MODULE'):
            parsing_section = 'module'
        elif line.startswith('BRITE'):
            break
        elif line.startswith('DBLINKS'):
            break
        elif line.startswith('DISEASE'):
            break
        else:
            pass

        if debug: print(line)
        if parsing_section == 'pathway':
            pathway.append(pathway_re.search(line).group('pathway'))
        elif parsing_section == 'module':
            module.append(module_re.search(line).group('module'))
        else:
            print('something is wrong')
            quit()

    pathway = '\n'.join(pathway)
    module = '\n'.join(module)

    if debug: print('  pathway: "{}"'.format(pathway))
    if debug: print('  module: "{}"'.format(module))

    return name, definition, pathway, module


if __name__ == '__main__':
    main()
