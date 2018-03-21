"""
Read sample documents from mongo db and write sample metadata files
to iRODS.
"""
import argparse
from itertools import islice
import os
import pprint
import re
import sys
import time

import pymongo

import imicrobe.util.irods as irods


def write_sample_metadata_files(target_root, file_limit):
    """
    This script is intended to run on a system with access to the iMicrobe MongoDB.
    For each document in the 'sample' collection of the 'imicrobe' database write
    the document contents as a CSV file to iRODS.
    """

    print('target iRODS directory is "{}"'.format(target_root))
    with irods.irods_session_manager() as irods_session:
        if irods.irods_collection_exists(irods_session, target_root):
            print('  target directory exists')
        else:
            print('  target directory does not exist')
            exit(1)

    print('\nsearching for samples in Mongo DB')

    sequence_file_extensions = re.compile('\.(fa|fna|fasta|fastq)(\.tar)?(\.gz)?$')
    t0 = time.time()
    samples = {}
    samples_missing_specimen_file = []
    samples_missing_fasta_file = []
    for sample_metadata in pymongo.MongoClient().imicrobe.sample.find(limit=file_limit):
        sample_fn = None
        #if sample_metadata is None:
        #    print('what is this all about?')
        #    raise Exception()
        if 'specimen__file' in sample_metadata:
            specimen_files = sample_metadata['specimen__file'].split()
            ##print('specimen__file:\n\t"{}"'.format('\n\t'.join(specimen_files)))
            # find the FASTA file
            for fp in specimen_files:

                if not fp.startswith('/iplant/')
                    # avoid ftp
                    pass
                elif sequence_file_extensions.search(fp) is None:
                    pass
                else:
                    sample_dp, sample_fn = os.path.split(fp)
                    metadata_fp = sequence_file_extensions.sub('.json', fp)
                    samples[metadata_fp] = sample_metadata
                    break

            if sample_fn is None:
                samples_missing_fasta_file.append(sample_metadata)
                print('{}: no FASTA file in "{}"'.format(
                    len(samples_missing_fasta_file),
                    pprint.pformat(sample_metadata)))
            else:
                pass
                #print('FASTA file: "{}"'.format(sample_fn))
        else:
            samples_missing_specimen_file.append(sample_metadata)
            print('{}: no specimen__file in "{}"'.format(
                len(samples_missing_specimen_file),
                pprint.pformat(sample_metadata['_id'])))

    print('found {} samples in {:5.2f}s'.format(len(samples), time.time()-t0))
    print('  {} samples have no specimen__file'.format(len(samples_missing_specimen_file)))
    print('  {} samples have no FASTA file'.format(len(samples_missing_fasta_file)))

    t0 = time.time()
    print('which files already exist?')
    files_to_be_written = {}
    with irods.irods_session_manager() as irods_session:
        for metadata_fp, sample_metadata in sorted(samples.items()):
            print('checking for "{}"'.format(metadata_fp))
            if irods.irods_data_object_exists(irods_session, metadata_fp):
                pass
            else:
                files_to_be_written[metadata_fp] = sample_metadata
    print('found {} files to be written in {:5.2f}s'.format(len(files_to_be_written), time.time()-t0))

    print('\nwriting {} files'.format(len(files_to_be_written)))

    #for source_fp, target_fp in sorted(files_to_be_copied.items()):
    #    print('copying\n\t"{}"\nto \n\t"{}"'.format(source_fp, target_fp))
    #    t0 = time.time()
    #    copy_file_to_irods(source_fp, target_fp)
    #    print('finished copy in {:5.2f}s'.format(time.time()-t0))


def main(argv):
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--target-root', default='/iplant/home/shared/imicrobe/projects')
    arg_parser.add_argument('--file-limit', type=int, default=None, required=False)

    args = arg_parser.parse_args(args=argv)

    write_sample_metadata_files(
        target_root=args.target_root,
        file_limit=args.file_limit)


def cli():
    main(sys.argv[1:])


if __name__ == '__main__':
    cli()
