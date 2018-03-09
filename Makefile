container:
	rm -f singularity/imdl.img
	sudo singularity create --size 2000 singularity/imdl.img
	sudo singularity bootstrap singularity/imdl.img singularity/imdl.def
	sudo chown --reference=singularity/imdl.def singularity/imdl.img

iput-container:
	iput -fK singularity/imdl.img

iget-container:
	cd singularity; iget -fK imdl.img
	irm imdl.img

