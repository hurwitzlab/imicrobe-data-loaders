download-pfam-data:
	wget ftp://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/database_files/pfamA.txt.gz data/pfamA.txt.gz
	wget ftp://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/database_files/dead_family.txt.gz data/dead_family.txt.gz

ils-imicrobe-projects:
	mkdir -p data
	ils -r /iplant/home/shared/imicrobe/projects > data/ils_imicrobe_project_listing.txt

write-download-command-file:
	python write_download_command_file.py --results-target-dp ${HOME}/usr/local/imicrobe/projects > data/download-command-file.txt

parallel-iget-uproc-results:
	cat data/download-command-file.txt | parallel --eta -j 10 --noswap '{}'

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
