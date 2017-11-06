download-pfam-data:
	wget ftp://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/database_files/pfamA.txt.gz data/pfamA.txt.gz
	wget ftp://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/database_files/dead_family.txt.gz data/dead_family.txt.gz

ils-imicrobe-projects:
	mkdir -p data
	ils -r /iplant/home/shared/imicrobe/projects > data/ils_imicrobe_project_listing.txt

test-write-download-command-file:
	python write_download_command_file.py \
		--line-limit 1000 \
		--results-target-dp ${HOME}/usr/local/imicrobe/data/uproc/projects > data/download-command-file.txt

write-download-command-file:
	python write_download_command_file.py --results-target-dp ${HOME}/usr/local/imicrobe/data/uproc/projects > data/download-command-file.txt

parallel-iget-uproc-results:
	cat data/download-command-file.txt | parallel --eta -j 5 --load 80% --noswap '{}'

write-load-sample-to-uproc-command-file:
	python load_sample_to_uproc_table.py --results-root-dp ${HOME}/usr/local/imicrobe/data/uproc/projects > data/load-sample-to-uproc-command-file.txt

parallel-load-sample-to-uproc:
	cat data/load-sample-to-uproc-command-file.txt | parallel --eta -j 2 --noswap '{}'

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
