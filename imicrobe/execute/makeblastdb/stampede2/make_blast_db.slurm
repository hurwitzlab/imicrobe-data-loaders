#!/bin/bash

#SBATCH -A iPlant-Collabs
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -t 48:00:00
#SBATCH -p skx-normal
#SBATCH -J make-blast-db
#SBATCH --mail-type BEGIN,END,FAIL
#SBATCH --mail-user jklynch@email.arizona.edu

module load blast
module load launcher
module load tacc-singularity

IMG=../../../../singularity/imdl.img
IMICROBE_PROJECTS=/work/05066/imicrobe/iplantc.org/data/imicrobe/projects

export BLAST_DB_OUTPUT=/work/05066/imicrobe/iplantc.org/data/blast/one-db

#cd /work/05066/imicrobe/iplantc.org/data/blast/one-db

find ${IMICROBE_PROJECTS} \
    -type f \
    -regextype posix-egrep \
    -regex ".+\.(fa|fna|fasta)$" \
    -size +0c \
    | sort \
    | uniq \
    | singularity exec ${IMG} filter_lines bad-fasta-files.txt \
    | singularity exec ${IMG} permute_lines \
    > imicrobe-fasta-list.txt


FILE_GROUPS=16

cat imicrobe-fasta-list.txt | singularity exec ${IMG} split_lines -n ${FILE_GROUPS} --prefix imicrobe-

#split --lines=550 imicrobe-fasta-list.txt imicrobe-

wc -l imicrobe-*

#
# go!
#

# no need to change these
export LAUNCHER_PLUGIN_DIR=$LAUNCHER_DIR/plugins
export LAUNCHER_RMI=SLURM

# must define LAUNCHER_WORKDIR somewhere
export LAUNCHER_WORKDIR=`pwd`
echo "LAUNCHER_WORKDIR: $LAUNCHER_WORKDIR"

# by default LAUNCHER_PPN is 8 (?)
# use LAUNCHER_PPN to control the number of jobs on each node
export LAUNCHER_PPN=${FILE_GROUPS}

export LAUNCHER_SCHEDULER=interleaved

export LAUNCHER_JOB_FILE=jobs.launcher
$LAUNCHER_DIR/paramrun
