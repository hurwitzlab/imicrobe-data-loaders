"""
Read lines from standard input and pass them to standard output. Filter out lines that match those in
the specified file.
"""
import argparse
import itertools
import operator
import sys


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-n', '--split-count', type=int, required=True, help='Number of files into which stdin will be split')
    arg_parser.add_argument('--prefix', required=True, help='File path prefix for split files')

    return arg_parser.parse_args()


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def main():
    args = get_args()

    line_groups = [list() for _ in range(args.split_count)]
    for line_group, line in zip(itertools.cycle(line_groups), sys.stdin.readlines()):
        #print('line_groups: {}'.format(line_groups))
        #print('line group: {}'.format(line_group))
        #print('line: "{}"'.format(line))
        line_group.append(line)

    group_id_iter = itertools.starmap(operator.add, itertools.product('abcdefghijklmnopqrstuvwxyz', repeat=2))
    for group_id, line_group in zip(group_id_iter, line_groups):
        file_name = args.prefix + group_id
        with open(file_name, 'w') as split_file:
            split_file.write(''.join(line_group))


if __name__ == '__main__':
    main()