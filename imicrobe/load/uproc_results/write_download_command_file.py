"""

Usage:
    python write_download_command_file.py --results-target-dp ${HOME}/usr/local/imicrobe/projects > data/download-command-file.txt

"""

import argparse
import itertools
import os
import sys


def get_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--irods-source-fp', default='data/ils_imicrobe_project_listing.txt')
    argparser.add_argument('--results-target-dp', required=True, help='absolute path for results directory tree')
    argparser.add_argument('--line-limit', default=None, type=int, help='number of lines to print')
    argparser.parse_args()

    args = argparser.parse_args()

    if not os.path.isabs(args.results_target_dp):
        print('command line argument --results-target-dp "{}" must be an absolute path'.format(args.results_target_dp))
        quit()
    else:
        pass

    return args


def main():
    args = get_args()
    sys.stderr.write('command line arguments: {}\n'.format(args))

    irods_source_fp = args.irods_source_fp
    results_target_dp = args.results_target_dp
    line_limit = args.line_limit

    with open(irods_source_fp, 'rt') as ils_listing:
        """ ils -r output looks like this:
        /iplant/home/shared/load/projects:
          rename.sh.uproc
          C- /iplant/home/shared/load/projects/1
        /iplant/home/shared/load/projects/1:
          CAM_PROJ_AcidMine.asm.fa
          CAM_PROJ_AcidMine.csv
          CAM_PROJ_AcidMine.read.fa
          CAM_PROJ_AcidMine.read_pep.fa
          centrifuge.csv
          centrifuge.png
          sample-attr.tab
          C- /iplant/home/shared/load/projects/1/samples
        /iplant/home/shared/load/projects/1/samples:
          C- /iplant/home/shared/load/projects/1/samples/1
        /iplant/home/shared/load/projects/1/samples/1:
          JGI_AMD_5WAY_IRNMTN_SMPL_20020301.centrifuge.sum
          JGI_AMD_5WAY_IRNMTN_SMPL_20020301.centrifuge.tsv
          JGI_AMD_5WAY_IRNMTN_SMPL_20020301.fa
          JGI_AMD_5WAY_IRNMTN_SMPL_20020301.fa.msh
          JGI_AMD_5WAY_IRNMTN_SMPL_20020301.fa.uproc
          centrifuge.csv
          centrifuge.png
          C- /iplant/home/shared/load/projects/1/samples/2
        """
        # the first line is the top-level directory
        # slice off the colon
        uproc_kegg_results_count = 0
        uproc_pfam_results_count = 0
        current_collection = None
        imicrobe_root = ils_listing.readline().strip()[:-1]
        for line in take(line_limit, (line_.strip() for line_ in ils_listing)):
            if line.startswith('C-'):
                pass

            elif line.startswith(imicrobe_root):
                # slice off the colon
                current_collection = line.strip()[:-1]

            elif line.endswith('.uproc.kegg') and current_collection is not None:
                # we have a UProC KEGG result file
                uproc_kegg_results_count += 1
                uproc_results_irods_path = os.path.join(current_collection, line)
                local_project_sample_dp = os.path.join(results_target_dp, current_collection[len(imicrobe_root)+1:])
                local_uproc_results_fp = os.path.join(local_project_sample_dp, line)

                if os.path.exists(local_uproc_results_fp):
                    sys.stderr.write('local file exists: {}\n'.format(local_uproc_results_fp))
                else:
                    sys.stdout.write(
                        'mkdir -p {dir};chmod agu+rx {dir};'.format(dir=os.path.dirname(local_uproc_results_fp)))
                    sys.stdout.write(
                        'iget -K {source} {target};chmod agu+r {target}\n'.format(
                            source=uproc_results_irods_path, target=local_uproc_results_fp))

            elif line.endswith('.uproc.pfam28') and current_collection is not None:
                # we have a UProC Pfam result file
                uproc_pfam_results_count += 1
                uproc_results_irods_path = os.path.join(current_collection, line)
                local_project_sample_dp = os.path.join(results_target_dp, current_collection[len(imicrobe_root) + 1:])
                local_uproc_results_fp = os.path.join(local_project_sample_dp, line)

                if os.path.exists(local_uproc_results_fp):
                    sys.stderr.write('local file exists: {}\n'.format(local_uproc_results_fp))
                else:
                    sys.stdout.write(
                        'mkdir -p {dir};chmod agu+rx {dir};'.format(dir=os.path.dirname(local_uproc_results_fp)))
                    sys.stdout.write(
                        'iget -K {source} {target};chmod agu+r {target}\n'.format(source=uproc_results_irods_path,
                                                                              target=local_uproc_results_fp))
            else:
                # a file of no interest
                pass

        sys.stderr.write('found {} files with UProC KEGG results\n'.format(uproc_kegg_results_count))
        sys.stderr.write('found {} files with UProC Pfam results\n'.format(uproc_pfam_results_count))


def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(itertools.islice(iterable, n))


if __name__ == '__main__':
    main()
