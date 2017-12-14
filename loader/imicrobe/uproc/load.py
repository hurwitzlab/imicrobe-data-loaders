"""
First task:
on Stampede2 copy UProC output files from TACC to IRODS

Second task:
on myo read UProC files from IRODS and load imicrobe database
"""
import argparse
import os
import sys

import loader.util.irods as irods


def copy_uproc_output_to_irods(source_root, target_root):
    """
    This is intended to run on Stampede2 in the imicrobe account.
    Walk the imicrobe directory and copy each UProC output file to IRODS /iplant/share/imicrobe/
    :return:
    """

    print('source directory is "{}"'.format(source_root))
    if os.path.exists(source_root):
        print('  found source directory')
    else:
        print('  source directory does not exist')
        exit(1)

    print('target directory is "{}"'.format(target_root))
    with irods.irods_session_manager() as irods_session:
        if irods.irods_collection_exists(target_root):
            print('  target directory exists')
        else:
            print('  target directory does not exist')
            exit(1)

    for parent_dir, child_dirs, files in os.walk(source_root):
        for f in files:
            if f.contains('.uproc.'):
                print('found a UProC output file:\n\t"{}"'.format(f))
            else:
                pass



def cli(argv):
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--source-root')
    arg_parser.add_argument('--target-root')

    args = arg_parser.parse_args(args=argv)

    copy_uproc_output_to_irods(source_root=args.source_root, target_root=args.target_root)


if __name__ == '__main__':
    cli(sys.argv[1:])