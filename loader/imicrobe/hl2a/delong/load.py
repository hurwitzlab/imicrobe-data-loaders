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

import loader.imicrobe.models as im
from loader.util.irods import \
    irods_copy, irods_create_collection, irods_data_object_checksums_match, \
    irods_data_object_exists, irods_delete, irods_delete_collection, irods_session_manager
import muscope_loader.models as mu
from orminator import session_manager_from_db_uri


delete_imicrobe_samples = False
copy_files = True
hl2a_delong_project_id = 266

with session_manager_from_db_uri(db_uri=os.environ.get('MUSCOPE_DB_URI')) as mu_session:

    mu_samples = mu_session.query(mu.Sample).filter(
        mu.Sample.sample_name.like(
            'CSHLII%%0-%%a-S%%C%%%-0015')).all()

    print('found {} results'.format(len(mu_samples)))
    for i, mu_sample in enumerate(mu_samples):
        #if i > 0:
        #    break

        with session_manager_from_db_uri(db_uri=os.environ.get('IMICROBE_DB_URI')) as im_session:
            print('\n{}'.format(mu_sample.sample_name))

            im_sample = im_session.query(im.Sample).filter(
                im.Sample.project_id == hl2a_delong_project_id).filter(
                im.Sample.sample_acc == mu_sample.sample_name).one_or_none()

            if im_sample is None:
                print('sample "{}" does not exist in imicrobe database'.format(mu_sample.sample_name))
            else:
                print('found sample "{}" in imicrobe database'.format(im_sample.sample_name))
                if delete_imicrobe_samples:
                    print('  deleting sample "{}" from imicrobe database'.format(im_sample.sample_name))
                    im_session.delete(im_sample)
                    # force the delete or things go bad when the new sample is added
                    im_session.flush()
                    im_sample = None
                else:
                    pass

            if im_sample is None:
                print('creating sample "{}" in imicrobe database'.format(mu_sample.sample_name))
                im_sample = im.Sample(
                    project_id=hl2a_delong_project_id,
                    sample_acc=mu_sample.sample_name,
                    sample_name=mu_sample.sample_name,
                    sample_type='archaea,bacteria,virus',
                    latitude=mu_sample.latitude_start,
                    longitude=mu_sample.longitude_start,
                    taxon_id=0,
                    url='none')
                im_session.add(im_sample)
            else:
                print('sample "{}" unchanged in imicrobe database'.format(im_sample.sample_name))

            # need sample_id to find attributes and derive sample file path
            im_sample_id = im_session.query(im.Sample.sample_id).filter(
                im.Sample.project_id == hl2a_delong_project_id).filter(
                im.Sample.sample_acc == mu_sample.sample_name).one()[0]
            print('im_sample_id: {}'.format(im_sample_id))

            # copy attributes from muscope sample to imicrobe sample
            # if they do not already exist
            # if attributes do not match update the imicrobe attribute to match the muscope attribute
            for mu_sample_attr in mu_sample.sample_attr_list:
                im_sample_attr_type = im_session.query(im.Sample_attr_type).filter(
                    im.Sample_attr_type.type_ == mu_sample_attr.sample_attr_type.type_).one_or_none()
                if im_sample_attr_type is None:
                    print('sample attribute type "{}" does not exist in imicrobe'.format(
                        mu_sample_attr.sample_attr_type.type_))
                else:
                    print('  setting imicrobe sample attribute with type "{}" to "{}"'.format(
                        mu_sample_attr.sample_attr_type.type_, mu_sample_attr.value))

                    im_sample_attr = im_session.query(im.Sample_attr).filter(
                        im.Sample_attr.sample == im_sample).filter(
                        im.Sample_attr.sample_attr_type == im_sample_attr_type).one_or_none()
                    if im_sample_attr is None:
                        print('  creating imicrobe sample attribute with type "{}" and value "{}" for sample "{}"'.format(
                            mu_sample_attr.sample_attr_type.type_,
                            mu_sample_attr.value,
                            im_sample.sample_name))
                        im_sample_attr = im.Sample_attr()
                        im_sample_attr.sample = im_sample
                        im_sample_attr.sample_attr_type = im_sample_attr_type
                        im_sample_attr.attr_value = mu_sample_attr.value
                    elif im_sample_attr.attr_value == mu_sample_attr.value:
                        print('  imicrobe sample "{}" has attribute "{}" with value "{}" matching muscope sample attribute value "{}"'.format(
                            im_sample.sample_name,
                            im_sample_attr.sample_attr_type.type_,
                            im_sample_attr.attr_value,
                            mu_sample_attr.value
                        ))
                    else:
                        print('  imicrobe sample "{}" has attribute "{}" with value "{}" NOT matching muscope sample attribute value "{}"'.format(
                            im_sample.sample_name,
                            im_sample_attr.sample_attr_type.type_,
                            im_sample_attr.attr_value,
                            mu_sample_attr.value
                        ))
                        print('    imicrobe sample attribute will be updated to match muscope attribute')
                        im_sample_attr.attr_value = mu_sample_attr.value

            # copy sample files from muscope to imicrobe

            # if there are already sample files in /iplant/share/imicrobe associated
            # with this sample then check they have the expected sample_id
            # if not then delete the sample file(s)
            if len(im_sample.sample_file_list) == 0:
                print('  imicrobe sample "{}" has no associated sample files yet'.format(im_sample.sample_name))
            else:
                print('  {} sample files are associated with imicrobe sample "{}"'.format(
                    len(im_sample.sample_file_list), im_sample.sample_name))
                with irods_session_manager() as irods_session:
                    # sample files are in collections such as this:
                    # /iplant/shared/imicrobe/projects/266/samples/5307
                    # it is possible that the sample id for an existing file does not match
                    # the sample_id we have currently so check that now
                    irods_delete_collection_set = set()
                    for im_existing_sample_file in im_sample.sample_file_list:
                        sample_id_collection_path, file_name = os.path.split(im_existing_sample_file.file_)
                        sample_collection_path, sample_id = os.path.split(sample_id_collection_path)
                        if im_sample.sample_id == int(sample_id):
                            # this sample file is in the expected collection
                            print('    found sample file "{}" in the expected collection "{}"'.format(
                                file_name, sample_id_collection_path))
                        else:
                            print('    found sample file "{}" in the wrong collection "{}"'.format(
                                file_name, sample_id_collection_path))
                            print('    deleting "{}"'.format(im_existing_sample_file.file_))
                            irods_delete(irods_session, im_existing_sample_file.file_)
                            irods_delete_collection_set.add(sample_id_collection_path)

                    # irods_collection will be empty if no files were deleted
                    for irods_collection in irods_delete_collection_set:
                        print('    deleting collection "{}"'.format(irods_collection))
                        irods_delete_collection(irods_session, irods_collection)

            # copy files if they have not been copied previously
            im_sample_file_type_reads = im_session.query(im.Sample_file_type).filter(
                im.Sample_file_type.type_ == 'Reads').one()
            for mu_sample_file in mu_sample.sample_file_list:
                # at this time only reads will be copied
                if mu_sample_file.sample_file_type.type_ == 'Reads' \
                        and (mu_sample_file.file_.endswith('_001.fastq') or
                             mu_sample_file.file_.endswith('readpool.fastq.gz')):

                    im_sample_collection_path = '/iplant/home/shared/imicrobe/projects/{}/sample/{}'.format(
                        hl2a_delong_project_id,
                        im_sample_id)
                    im_sample_file_path = '{}/{}'.format(
                        im_sample_collection_path,
                        os.path.basename(mu_sample_file.file_))

                    # is im_sample already associated with the imicrobe version of this file?
                    mu_sample_file_basename = os.path.basename(mu_sample_file.file_)
                    print('searching for imicrobe sample file with basename "{}"'.format(mu_sample_file_basename))
                    im_sample_file = im_session.query(im.Sample_file).filter(
                        im.Sample_file.sample == im_sample).filter(
                        im.Sample_file.sample_file_type == im_sample_file_type_reads).filter(
                        im.Sample_file.file_.like('%{}'.format(mu_sample_file_basename))).one_or_none()

                    if im_sample_file is None:
                        print('  creating imicrobe sample file "{}"'.format(im_sample_file_path))

                        im_sample_file = im.Sample_file(file_=im_sample_file_path)
                        im_sample_file.sample = im_sample
                        im_sample_file.sample_file_type = im_sample_file_type_reads

                    else:
                        print('  imicrobe sample file "{}" already exists'.format(im_sample_file_path))

                    print('  + copying file "{}"'.format(mu_sample_file.file_))
                    print('    to file "{}"'.format(im_sample_file_path))

                    if copy_files:
                        with irods_session_manager() as irods_session:
                            print('creating collection "{}"'.format(im_sample_collection_path))
                            irods_create_collection(
                                irods_session,
                                target_collection_path=im_sample_collection_path)

                            if irods_data_object_exists(irods_session, im_sample_file) and \
                                    irods_data_object_checksums_match(irods_session, mu_sample_file.file_, im_sample_file.file_):
                                print('  imicrobe sample file "{}" matches muscope sample file "{}"'.format(
                                    im_sample_file.file_, mu_sample_file.file_))
                            else:
                                print('copying')
                                irods_copy(
                                    irods_session,
                                    src_path=mu_sample_file.file_,
                                    dest_path=im_sample_file.file_)
                    else:
                        print('*** file copy is disabled ***')

                else:
                    print('  - ignoring file "{}"'.format(mu_sample_file.file_))

