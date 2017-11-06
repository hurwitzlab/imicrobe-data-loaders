"""
Parse original UProC output directory tree to write a launcher job file that will copy UProC KEGG results
to the shared iMicrobe storage system using iRODS.

The shared iMicrobe storage system is at /iplant/home/shared/imicrobe
"""
import argparse
import os
import sys


def get_args(argv):
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument('--job-file', required=True)
    arg_parser.add_argument('--fs-source-dir', required=True)
    arg_parser.add_argument('--irods-target-dir', required=True)
    arg_parser.add_argument('--job-limit', default=None, type=int)

    args = arg_parser.parse_args(args=argv)
    print(args)

    return args


def main():
    write_job_file(sys.argv[1:])


def write_job_file(argv):
    args = get_args(argv)

    job_count = 0
    with open(args.job_file, 'wt') as job_file:
        for root, dir_names, file_names in os.walk(top=args.fs_source_dir):
            for file_name in file_names:
                if file_name.endswith('.uproc.kegg'):
                    # construct the irods target file path
                    # by cutting the fs-source directory off of root
                    # and joining what remains to irods-target, for example
                    #   root = '/work/imicrobe/data/projects/1/sample/1'
                    #   fs-source = '/work/imicrobe/data/projects'
                    #   irods-target = '/iplant/home/shared/imicrobe/data/projects
                    #
                    #   irods_target_dir_path = '/iplant/home/shared/imicrobe/data/projects' + '/1/sample/1'

                    fs_source_file_path = os.path.join(root, file_name)

                    irods_target_dir_path = os.path.join(
                        args.irods_target_dir,
                        root[len(args.fs_source_dir):])

                    irods_target_file_path = os.path.join(irods_target_dir_path, file_name)

                    job_file.write('imkdir {}; iput -K {} {}\n'.format(
                        irods_target_dir_path,
                        fs_source_file_path,
                        irods_target_file_path))

                    job_count += 1
                    if args.job_limit and args.job_limit == job_count:
                        break

                else:
                    # ignore this file
                    pass

    print('wrote {} jobs to "{}"'.format(job_count, args.job_file))


if __name__ == '__main__':
    main()