"""
First task:
on Stampede2 copy UProC output files from TACC to IRODS.
It took about 900s on Stampede2 to check that all ~8900 files were
already present in /iplant/home/shared/imicrobe/projects

Second task:
on myo read UProC files from IRODS and load imicrobe database
"""
import argparse
import os
import sys
import time

import loader.util.irods as irods


def copy_uproc_output_to_irods(source_root, target_root, file_limit):
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
        if irods.irods_collection_exists(irods_session, target_root):
            print('  target directory exists')
        else:
            print('  target directory does not exist')
            exit(1)

    print('\nsearching for UProC results files in "{}"'.format(source_root))

    t0 = time.time()
    uproc_results_files = {}
    for parent_dir, child_dirs, files in os.walk(source_root):
        for f in files:
            if '.uproc.' in f:
                #print('parent dir {}'.format(parent_dir))
                #print('found a UProC output file:\n\t"{}"'.format(f))
                uproc_source_fp = os.path.join(parent_dir, f)
                # combine target_root such as "/iplant/home/shared/imicrobe/projects/"
                # with the section of parent_dir following the source_root, for example
                # if source_root is "/work/05066/imicrobe/iplantc.org/data/imicrobe/projects"
                # and parent_dir is "/work/05066/imicrobe/iplantc.org/data/microbe/projects/193/samples/4078/"
                # then we want to take "193/samples/4078/" from parent_dir and append it to target_root to form
                # "/iplant/home/shared/imicrobe/projects/193/samples/4078/"
                iplant_target_fp = os.path.join(target_root, parent_dir[len(source_root)+1:], f)
                if uproc_source_fp in uproc_results_files:
                    print('ERROR: already found "{}"'.format(uproc_source_fp))
                    exit(1)
                else:
                    uproc_results_files[uproc_source_fp] = iplant_target_fp
            else:
                pass

        if file_limit is not None and len(uproc_results_files) >= file_limit:
           print('stopping after finding {} UProC output files'.format(len(uproc_results_files)))
           #print(uproc_results_files)
           break
        else:
           pass

    print('found {} UProC results files in {:5.2f}s'.format(len(uproc_results_files), time.time()-t0))

    t0 = time.time()
    print('which files are already in "{}"?'.format(target_root))
    files_to_be_copied = {}
    with irods.irods_session_manager() as irods_session:
        for source_fp, target_fp in sorted(uproc_results_files.items()):
            if irods.irods_data_object_exists(irods_session, target_fp):
                pass
            else:
                files_to_be_copied[source_fp] = target_fp
    print('found {} files to be copied in {:5.2f}s'.format(len(files_to_be_copied), time.time()-t0))

    print('\ncopying {} files to "{}"'.format(len(files_to_be_copied), target_root))

    for source_fp, target_fp in sorted(files_to_be_copied.items()):
        print('copying\n\t"{}"\nto \n\t"{}"'.format(source_fp, target_fp))
        t0 = time.time()
        copy_file_to_irods(source_fp, target_fp)
        print('finished copy in {:5.2f}s'.format(time.time()-t0))


def copy_file_to_irods(source_fp, target_fp):
    with irods.irods_session_manager() as irods_session:
        if irods.irods_data_object_exists(irods_session, target_fp):
            pass
        else:
            irods.irods_put(irods_session, source_fp, target_fp)


def main(argv):
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--source-root')
    arg_parser.add_argument('--target-root')
    arg_parser.add_argument('--file-limit', type=int, default=None, required=False)

    args = arg_parser.parse_args(args=argv)

    copy_uproc_output_to_irods(
        source_root=args.source_root,
        target_root=args.target_root,
        file_limit=args.file_limit)


def cli():
    main(sys.argv[1:])


if __name__ == '__main__':
    cli()
