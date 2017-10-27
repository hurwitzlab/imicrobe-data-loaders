# imicrobe-load-uproc-results
Scripts to load UProC results into the iMicrobe database.

## Requirements
These scripts require iRODS, `make`, GNU `parallel`, a Python 3.6+ interpreter, and ORM classes generated
by [imicrobe-python-orm](https://github.com/hurwitzlab/imicrobe-python-orm).
See the project [README](https://github.com/hurwitzlab/imicrobe-python-orm) for installation instructions.

## Installation
Clone this repository to run the scripts.

## Usage
Execute `make ils-imicrobe-projects` to create a file list of the iRODS iMicrobe project directories.

Execute `make write-download-command-file` to create a file of commands suitable for GNU Parallel.

Execute `make parallel-iget-uproc-results` to do the deed. This should take less than an hour. Try `-j 100` for fun.

Run `make download-pfam-data` to get the necessary Pfam files.

Execute `python load_pfam_table.py` to load Pfam annotations into the uproc table.
This will first drop the uproc table and delete all rows from the sample_to_uproc table.

Run `make write-load-sample-to-uproc-command-file` to create a file of commands for
GNU Parallel. This will also drop and create the sample_to_uproc table.

Run 'make parallel-load-sample-to-uproc' to load the sample_to_uproc table.
