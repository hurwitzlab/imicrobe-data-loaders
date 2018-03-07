#!/bin/bash

#SBATCH -A iPlant-Collabs
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -t 04:00:00
#SBATCH -p skx-normal
#SBATCH -J validate-fasta
#SBATCH --mail-type BEGIN,END,FAIL
#SBATCH --mail-user jklynch@email.arizona.edu

module load python3

cd /work/05066/imicrobe/stampede2/jklynch/imicrobe-data-loaders
source venv/bin/activate

python3 imicrobe/validate/fasta/fasta_validator.py \
    --fasta-glob "/work/05066/imicrobe/iplantc.org/data/imicrobe/projects/**/*.fa" \
    --max-workers 48

python3 imicrobe/validate/fasta/fasta_validator.py \
    --fasta-glob "/work/05066/imicrobe/iplantc.org/data/imicrobe/projects/**/*.fna" \
    --max-workers 48

python3 imicrobe/validate/fasta/fasta_validator.py \
    --fasta-glob "/work/05066/imicrobe/iplantc.org/data/imicrobe/projects/**/*.fasta" \
    --max-workers 48
