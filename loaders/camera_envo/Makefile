all: download-camera-envo-data load_camera_envo

download-camera-envo-data:
	wget https://github.com/hurwitzlab/imicrobe-lib/raw/master/docs/mapping_files/CameraMetadata_ENVO_working_copy.csv

load_camera_envo:
	python load_camera_envo.py CameraMetadata_ENVO_working_copy.csv

myo-rsync-dry-run:
	rsync -n -arvzP --delete --exclude-from=rsync.exclude -e "ssh -A -t hpc ssh -A -t myo" ./ :project/imicrobe/imicrobe-load-uproc-results

myo-rsync:
	rsync -arvzP --delete --exclude-from=rsync.exclude -e "ssh -A -t hpc ssh -A -t myo" ./ :project/imicrobe/imicrobe-load-uproc-results
