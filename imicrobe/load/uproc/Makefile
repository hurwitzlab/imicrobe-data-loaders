# run on Stampede2
test-copy-uproc-results-to-irods:
	python3 copy_uproc_results_to_iplant_imicrobe.py \
		--source-root /work/05066/imicrobe/iplantc.org/data/imicrobe/projects \
		--target-root /iplant/home/shared/imicrobe/projects \
		--file-limit 100

# run on Stampede2
copy-uproc-results-to-irods:
	python3 copy_uproc_results_to_iplant_imicrobe.py \
		--source-root /work/05066/imicrobe/iplantc.org/data/imicrobe/projects \
		--target-root /iplant/home/shared/imicrobe/projects

# run on Myo
test-load-all-tables:
	python3 load.py -u $(IMICROBE_DB_URI) --sample-limit 10 &> test-load-all-tables.log

# run on Myo
load-all-tables:
	python3 load.py -u $(IMICROBE_DB_URI) &> load-all-tables.log
