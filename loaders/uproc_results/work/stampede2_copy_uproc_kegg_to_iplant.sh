#!/bin/bash
#----------------------------------------------------
# Sample SLURM job script
#   for TACC Stampede2 KNL nodes
#
#   *** Serial Job on Normal Queue ***
#
# Last revised: 27 Jun 2017
#
# Notes:
#
#   -- Copy/edit this script as desired.  Launch by executing
#      "sbatch knl.serial.slurm" on a Stampede2 login node.
#
#   -- Serial codes run on a single node (upper case N = 1).
#        A serial code ignores the value of lower case n,
#        but slurm needs a plausible value to schedule the job.
#
#   -- For a good way to run multiple serial executables at the
#        same time, execute "module load launcher" followed
#        by "module help launcher".

#----------------------------------------------------

#SBATCH -J copy-uproc-kegg-results      # Job name
#SBATCH -o copy-uproc-kegg-results.o%j  # Name of stdout output file
#SBATCH -e copy-uproc-kegg-results.e%j  # Name of stderr error file
#SBATCH -p normal                       # Queue (partition) name
#SBATCH -N 1                            # Total # of nodes (must be 1 for serial)
#SBATCH -n 1                            # Total # of mpi tasks (should be 1 for serial)
#SBATCH -t 12:00:00                     # Run time (hh:mm:ss)
#SBATCH --mail-user=jklynch@email.arizona.edu
#SBATCH --mail-type=all                 # Send email at begin and end of job
#SBATCH -A iPlant-Collabs               # Allocation name (req'd if you have more than 1)

module list
pwd
date

module load irods

#module load launcher
export LAUNCHER_DIR=~/hl-launcher

echo "LAUNCHER_DIR: $LAUNCHER_DIR"

# no need to change these
export LAUNCHER_PLUGIN_DIR=$LAUNCHER_DIR/plugins
export LAUNCHER_RMI=SLURM

# point LAUNCHER_JOB_FILE at your job file
export LAUNCHER_JOB_FILE=copy_uproc_kegg_to_iplant.joblist

# must define LAUNCHER_WORKDIR somewhere
export LAUNCHER_WORKDIR=`pwd`
echo "LAUNCHER_WORKDIR: $LAUNCHER_WORKDIR"

# by default LAUNCHER_PPN is 8 (?)
# use LAUNCHER_PPN to control the number of jobs on each node
export LAUNCHER_PPN=4

$LAUNCHER_DIR/paramrun

echo " "
echo "Launcher Job Complete"
echo " "

# ---------------------------------------------------