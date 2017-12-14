copy-uproc-results-to-irods:
	python3 load.py \
		--source-root /work/05066/imicrobe/iplantc.org/data/imicrobe/projects \
		--target-root /iplant/home/shared/imicrobe/projects \

download-pfam-data:
	wget ftp://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/database_files/pfamA.txt.gz data/pfamA.txt.gz
	wget ftp://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/database_files/dead_family.txt.gz data/dead_family.txt.gz

myo-rsync-dry-run:
	rsync -n -arvzP --delete --exclude-from=rsync.exclude -e "ssh -A -t hpc ssh -A -t myo" ./ :project/imicrobe/imicrobe-load-uproc-results

myo-rsync:
	rsync -arvzP --delete --exclude-from=rsync.exclude -e "ssh -A -t hpc ssh -A -t myo" ./ :project/imicrobe/imicrobe-load-uproc-results

lytic-rsync-dry-run:
	rsync -n -arvzP --delete --exclude-from=rsync.exclude -e "ssh -A -t hpc ssh -A -t lytic" ./ :project/imicrobe/imicrobe-load-uproc-results

lytic-rsync:
	rsync -arvzP --delete --exclude-from=rsync.exclude -e "ssh -A -t hpc ssh -A -t lytic" ./ :project/imicrobe/imicrobe-load-uproc-results

stampede-scp-results:
	scp imicrobe@stampede.tacc.utexas.edu:/home1/05066/imicrobe/jkl/process_uproc_results/uproc_results.txt.gz data/uproc_results.txt.gz

# run copy-uproc-kegg-results-to-iplant on Stampede2
test-copy-uproc-kegg-results-to-iplant:
	python kegg/write_copy_uproc_kegg_results_to_iplant_job_file.py \
		--job-file work/copy_uproc_kegg_to_iplant.joblist \
		--fs-source /work/05066/imicrobe/iplantc.org/data/imicrobe/projects \
		--irods-target /iplant/home/shared/imicrobe/projects \
		--job-limit 10
	cd work; sbatch stampede2_copy_uproc_kegg_to_iplant.sh

# run copy-uproc-kegg-results-to-iplant on Stampede2
copy-uproc-kegg-results-to-iplant:
	python kegg/write_copy_uproc_kegg_results_to_iplant_job_file.py \
		--job-file work/copy_uproc_kegg_to_iplant.joblist \
		--fs-source /work/05066/imicrobe/iplantc.org/data/imicrobe/projects \
		--irods-target /iplant/home/shared/imicrobe/projects
	cd work; sbatch stampede2_copy_uproc_kegg_to_iplant.sh

drop-and-create-uproc-kegg-tables:
	python kegg/load_kegg_results_to_uproc_kegg_table.py --drop-tables
	python kegg/load_kegg_results_to_uproc_kegg_table.py --create-tables

write-load-uproc-kegg-tables-command-file:
	python kegg/load_kegg_results_to_uproc_kegg_table.py --results-root-dp ${HOME}/usr/local/imicrobe/data/uproc/projects > data/load-uproc-kegg-tables-command-file.txt

parallel-load-uproc-kegg-tables:
	cat data/load-uproc-kegg-tables-command-file.txt | parallel --eta -j 5 --load 80% --noswap '{}'

test-direct-load-uproc-kegg-tables:
	python kegg/load_kegg_results_to_uproc_kegg_table.py \
		--load-results-root-dp ${HOME}/usr/local/imicrobe/data/uproc/projects \
		--line-limit 5

direct-load-uproc-kegg-tables:
	python kegg/load_kegg_results_to_uproc_kegg_table.py \
			--load-results-root-dp ${HOME}/usr/local/imicrobe/data/uproc/projects \
			> data/direct_load_uproc_kegg_tables.log
