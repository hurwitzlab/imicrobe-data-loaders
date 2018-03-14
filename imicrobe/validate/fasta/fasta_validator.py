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
    arg_parser.add_argument('-i', '--fasta-glob', required=True, help='glob for FASTA files to be validated')
    arg_parser.add_argument('--max-workers', type=int, default=1, help='number of processes')

    args = arg_parser.parse_args(argv)
    print('command line arguments:\n\t{}'.format(args))

    return args


def main():
    fasta_validate(**vars(get_args(sys.argv[1:])))


def fasta_validate(fasta_glob, max_workers):
    fasta_list = glob.glob(fasta_glob, recursive=True)
    print('glob "{}" matched {} files'.format(fasta_glob, len(fasta_list)))

    good = []
    bad = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_fasta_fp = {executor.submit(parse_fasta, fasta_fp): fasta_fp for fasta_fp in fasta_list}
        for future in concurrent.futures.as_completed(future_to_fasta_fp):
            fasta_fp = future_to_fasta_fp[future]
            try:
                read_count, t = future.result()
            except Exception as exc:
                bad.append((fasta_fp, exc))
            else:
                good.append((fasta_fp, read_count, t))

        print('\n{} valid FASTA file(s)\n'.format(len(good)))

        sorted_bad = sorted(bad)
        print('{} problematic file(s):'.format(len(bad)))
        print('\n'.join([b[0] for b in sorted_bad]))

        print('\nfailures:')
        print('\n\n'.join([str(b[1]) for b in sorted_bad]))


def parse_fasta(fasta_fp):
    t0 = time.time()
    alphabet = set(IUPAC.ambiguous_dna.letters)
    read_count = 0
    for record in SeqIO.parse(fasta_fp, format='fasta', alphabet=IUPAC.ambiguous_dna):
        read_count += 1
        seq_letters = set(str(record.seq))
        if len(seq_letters.difference(alphabet)) == 0:
            pass
        else:
            msg ='{}: Failed to parse sequence {}\nid: {}\nsequence: {}'.format(fasta_fp, read_count, record.id, record.seq)
            raise Exception(msg)

        if len(record.seq) == 0:
            msg = '{}: Record {} has 0-length sequence\nid: {}'.format(fasta_fp, read_count, record.id)
            raise Exception(msg)

    if read_count == 0:
        msg = '{} is empty'.format(fasta_fp)
        raise Exception(msg)

    t = time.time() - t0
    return read_count, t


if __name__ == '__main__':
    main()