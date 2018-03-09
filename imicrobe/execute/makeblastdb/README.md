# Run makeblastdb on iMicrobe samples

1. Generate a list of 'bad' FASTA files using `imicrobe/validate/fasta/stampede2/validate-fa.slurm`. This takes about
an hour. Copy the list of 'bad' files to a file such as `bad-imicrobe-fasta-files.txt`.

2. Create a list of all iMicrobe FASTA files minus the 'bad' ones:
    ```
    $ find /work/05066/imicrobe/iplantc.org/data/imicrobe/projects \
        -type f \
        -regextype posix-egrep \
        -regex ".+\.(fa|fna|fasta)$" \
        -size +0c \
        | sort \
        | uniq \
        python filter_lines.py bad-imicrobe-fasta-files.txt > imicrobe-fasta-list.txt
    ```
    
3. Run `makeblastdb` 