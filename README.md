# iMicrobe Data Loaders
Scripts to load various data into the iMicrobe database.

  + camera_envo
  + uproc_results

## camera_envo
Load CAMERA metadata.


## imicrobe-load-uproc-results
Scripts to load UProC results into the iMicrobe database.

### Requirements
These scripts require iRODS, `make`, GNU `parallel`, a Python 3.6+ interpreter, and ORM classes generated
by [imicrobe-python-orm](https://github.com/hurwitzlab/imicrobe-python-orm).
See the imicrobe-python-orm  [README](https://github.com/hurwitzlab/imicrobe-python-orm) for instructions on generating the ORM classes.

### Installation
Clone this repository to run the scripts.

### Usage: Load UProC Pfam results
Execute `make ils-imicrobe-projects` to create a file list of the iRODS iMicrobe project directories. By default the list will be written to the `data` directory. This step took 83 minutes on a laptop, 4 minutes on myo.

Execute `make write-download-command-file` to create a file of `iget` commands suitable for GNU `parallel`.

Execute `make parallel-iget-uproc-results` to do the deed. This should take less than an hour. Try `-j 100` for fun.

Execute `make download-pfam-data` to get the necessary Pfam files.

Execute `python load_pfam_table.py` to load Pfam annotations into the uproc table.
This will first drop the uproc table and delete all rows from the sample_to_uproc table.

Run `make write-load-sample-to-uproc-command-file` to create a file of commands for
GNU Parallel. This will also drop and create the sample_to_uproc table.

Run 'make parallel-load-sample-to-uproc' to load the sample_to_uproc table.

### Usage: Load UProC KEGG results

If UProC KEGG results need to be copied from TACC to /iplant/ execute `make copy-uproc-kegg-results-to-iplant` from stampede2.

If the UProC KEGG results have not been copied to myo execute `make ils-imicrobe-projects write-download-command-file parallel-iget-uproc-results`.

If the UProC KEGG database tables do not exist yet execute `make drop-and-create-uproc-kegg-tables`.

To load the UProC KEGG database tables execute `make write-load-uproc-kegg-tables-command-file` followed by `make `
