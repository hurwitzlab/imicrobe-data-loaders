import argparse
import concurrent.futures
import glob
import sys
import time

from Bio import SeqIO
from Bio import Alphabet
from Bio.Alphabet import IUPAC


def get_args(argv):
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--fasta-glob', required=True, help='glob for FASTA files to be validated')
    arg_parser.add_argument('--max-workers', required=True, type=int, help='number of processes')

    args = arg_parser.parse_args(argv)
    print('command line arguments:\n\t{}'.format(args))

    return args


def main():
    fasta_validate(**vars(get_args(sys.argv[1:])))


def fasta_validate(fasta_glob, max_workers):
    print('glob: "{}"'.format(fasta_glob))

    fasta_list = glob.glob(fasta_glob, recursive=True)
    print('  found {} files'.format(len(fasta_list)))

    good = []
    bad = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_fasta_fp = {executor.submit(parse_fasta, fasta_fp): fasta_fp for fasta_fp in fasta_list}
        for future in concurrent.futures.as_completed(future_to_fasta_fp):
            fasta_fp = future_to_fasta_fp[future]
            try:
                read_count, t = future.result()
            except Exception as exc:
                bad.append(fasta_fp)
            else:
                good.append((fasta_fp, read_count, t))

        print('problematic files:')
        for i, fp in enumerate(sorted(bad)):
            print('\t{}: {}'.format(i+1, fp))

        #print('good files:')
        #for i, (fp, read_count, t) in enumerate(sorted(good)):
        #    print('\t{}: {} {} read(s) in {:5.2f}s'.format(i+1, fp, read_count, t))


def parse_fasta(fasta_fp):
    t0 = time.time()
    read_count = 0
    for record in SeqIO.parse(fasta_fp, format='fasta', alphabet=IUPAC.ambiguous_dna):
        if Alphabet._verify_alphabet(record.seq):
            pass
        else:
            raise Exception()

        if len(record.seq) == 0:
            raise Exception()
        read_count += 1

    if read_count == 0:
        raise Exception()

    t = time.time() - t0
    return read_count, t


if __name__ == '__main__':
    main()