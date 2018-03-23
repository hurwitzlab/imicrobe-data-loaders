import os
import re

import imicrobe.models as models
import imicrobe.util.irods as irods
from orminator import session_manager_from_db_uri


def find_missing_sample_files():

    for project, samples in irods.get_project_sample_collection_paths(sample_limit=100).items():
        print('project "{}"'.format(project))
        print('failed to find:')
        for sample in samples:
            for parent_collection, child_collections, data_objects in irods.walk(sample):
                for data_object in data_objects:
                    #print('searching table sample_file for "{}"'.format(data_object.path))
                    with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
                        s = imicrobe_db_session.query(
                            models.Sample_file).filter(
                                models.Sample_file.file_ == data_object.path).one_or_none()

                        if s is None:
                            yield data_object.path


path_pattern = re.compile(r'projects/(?P<project_id>\d+)/samples/(?P<sample_id>\d+)')
for missing_sample_file in find_missing_sample_files():
    print('  {}'.format(missing_sample_file))
    path_match = path_pattern.search(missing_sample_file)
    if path_match is None:
        raise Exception()
    elif missing_sample_file.endswith('.json'):
        print('    project: {}'.format(path_match.group('project_id')))
        print('    sample: {}'.format(path_match.group('sample_id')))
        sample_id = int(path_match.group('sample_id'))
        with session_manager_from_db_uri(os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
            sample = imicrobe_db_session.query(models.Sample).filter(models.Sample.sample_id == sample_id).one()
            print('    sample has {} sample files'.format(len(sample.sample_file_list)))
            sample_file = models.Sample_file()
            sample_file.file_ = missing_sample_file
            sample_file.sample_file_type = imicrobe_db_session.query(
                models.Sample_file_type).filter(
                    models.Sample_file_type.type_ == 'Meta').one()
            sample.sample_file_list.append(sample_file)
    else:
        print('  not a JSON file: {}'.format(missing_sample_file))
