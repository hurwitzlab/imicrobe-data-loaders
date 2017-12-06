"""
Copy HL2A DeLong data from muSCOPE to iMicrobe.

Copy samples that look like these:

 CSHLIID00-20a-S06C001-0015
 CSHLIIR00-20a-S06C001-0015
 CSHLIID20-03a-S06C001-0015

In the iMicrobe database a sample has 'sample_acc' and 'sample_name'.
The 'sample_acc' will be 'CSHLIID00-20a' and 'sample_name' will be
'CSHLIID00-20a-S06C001-0015'.

Find these samples in the muSCOPE database
Create corresponding entries in the iMicrobe database.
Copy data files from
    /iplant/home/scope/data/delong/HL2A
to
    /iplant/home/shared/imicrobe/projects/266
"""
import os

import imicrobe.models as im
import muscope_loader.models as mu
from orminator import session_manager_from_db_uri


hl2a_delong_project_id = 266

with session_manager_from_db_uri(db_uri=os.environ.get('MUSCOPE_DB_URI')) as mu_session:
    with session_manager_from_db_uri(db_uri=os.environ.get('IMICROBE_DB_URI')) as im_session:

        mu_samples = mu_session.query(mu.Sample).filter(
            mu.Sample.sample_name.like(
                'CSHLII%%0-%%a-S%%C%%%-0015')).all()

        print('found {} results'.format(len(mu_samples)))
        for mu_sample in mu_samples:
            print(mu_sample.sample_name)

            im_sample = im_session.query(im.Sample).filter(
                im.Sample.project_id == hl2a_delong_project_id).filter(
                im.Sample.sample_acc == mu_sample.sample_name).one_or_none()

            if im_sample is None:
                print('sample {} does not exist'.format(mu_sample.sample_name))
            else:
                print('deleting sample {}'.format(im_sample.sample_name))
                im_session.delete(im_sample)
                # force the delete or things go bad when the new sample is added
                im_session.flush()

            im_sample = im.Sample(
                project_id=hl2a_delong_project_id,
                sample_acc=mu_sample.sample_name,
                sample_name = mu_sample.sample_name,
                sample_type='archaea,bacteria,virus',
                latitude=mu_sample.latitude_start,
                longitude=mu_sample.longitude_start,
                taxon_id=0,
                url='none')
            im_session.add(im_sample)

            # need sample_id to derive sample file path
            im_sample_id = im_session.query(im.Sample.sample_id).filter(
                im.Sample.project_id == hl2a_delong_project_id).filter(
                im.Sample.sample_acc == mu_sample.sample_name).one()[0]
            print('im_sample_id: {}'.format(im_sample_id))

            # copy attributes
            # first verify the current im sample has no attributes
            # since it was just created
            im_existing_sample_attr = im_session.query(im.Sample_attr).filter(
                im.Sample_attr.sample_id == im_sample_id).all()
            if len(im_existing_sample_attr) > 0:
                print('the current sample should have no attributes')
                raise Exception()
            else:
                for mu_sample_attr in mu_sample.sample_attr_list:
                    im_sample_attr_type = im_session.query(im.Sample_attr_type).filter(
                        im.Sample_attr_type.type_ == mu_sample_attr.sample_attr_type.type_).one_or_none()
                    if im_sample_attr_type is None:
                        print('sample attribute type "{}" does not exist in iMicrobe'.format(
                            mu_sample_attr.sample_attr_type.type_))
                    else:
                        print('setting sample attribute with type "{}" to "{}"'.format(
                            mu_sample_attr.sample_attr_type.type_, mu_sample_attr.value))

                        im_sample_attr = im.Sample_attr()
                        im_sample_attr.sample = im_sample
                        im_sample_attr.sample_attr_type = im_sample_attr_type
                        im_sample_attr.attr_value = mu_sample_attr.value

            im_existing_sample_files = im_session.query(im.Sample_file).filter(
                im.Sample_file.sample_id == im_sample_id).all()
            if len(im_existing_sample_files) > 0:
                print('the current sample should have no associated files')
                raise Exception()
            else:
                # copy files
                im_sample_file_type_reads = im_session.query(im.Sample_file_type).filter(
                    im.Sample_file_type.type_ == 'Reads').one()
                for mu_sample_file in mu_sample.sample_file_list:
                    if mu_sample_file.sample_file_type.type_ == 'Reads' \
                            and (mu_sample_file.file_.endswith('_001.fastq') or
                                 mu_sample_file.file_.endswith('readpool.fastq.gz')):
                        im_sample_file_path = '/iplant/home/shared/imicrobe/projects/{}/sample/{}/{}'.format(
                            hl2a_delong_project_id,
                            im_sample_id,
                            os.path.basename(mu_sample_file.file_))
                        im_sample_file = im.Sample_file(file_=im_sample_file_path)
                        im_sample_file.sample = im_sample
                        im_sample_file.sample_file_type = im_sample_file_type_reads

                        print('  + copying file "{}"'.format(mu_sample_file.file_))
                        print('    to file "{}"'.format(im_sample_file_path))
                    else:
                        print('  - ignoring file "{}"'.format(mu_sample_file.file_))