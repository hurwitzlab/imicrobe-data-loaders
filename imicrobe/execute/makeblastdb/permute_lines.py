"""
Read all lines from standard input, permute them, and pass them to standard output.
"""
import random
import sys


def main():
    lines = [line for line in sys.stdin]
    random.shuffle(lines)
    sys.stdout.write(''.join(lines))


if __name__ == '__main__':
    main()
