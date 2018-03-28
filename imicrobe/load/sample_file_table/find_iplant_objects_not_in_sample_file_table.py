import argparse
import os
import re
import time

import imicrobe.models as models
import imicrobe.util.irods as irods
from orminator import session_manager_from_db_uri


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--sample-limit', type=int, default=None, required=False)
    args = arg_parser.parse_args()
    print(args)
    return args


def find_missing_sample_files(sample_limit=100):

    for project, samples in irods.get_project_sample_collection_paths(sample_limit=sample_limit).items():
        print('project "{}"'.format(project))
        print('failed to find:')
        for sample in samples:
            for parent_collection, child_collections, data_objects in irods.walk(sample):
                for data_object in data_objects:
                    #print('searching table sample_file for "{}"'.format(data_object.path))
                    with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
                        s = imicrobe_db_session.query(
                            models.Sample_file).filter(
                                models.Sample_file.file_ == data_object.path).all()
                        if len(s) == 0:
                            # this path is not in the database
                            print('  {}'.format(data_object.path))
                            yield data_object.path
                        elif len(s) == 1:
                            # this path is in the database
                            pass
                        else:
                            # this path is in the database more than once
                            print('WARNING: "{}" was found {} times'.format(data_object.path, len(s)))
# why?
# collection "/iplant/home/shared/imicrobe/projects/95/samples" does not exist

args = get_args()
path_pattern = re.compile(r'projects/(?P<project_id>\d+)/samples/(?P<sample_id>\d+)')
t0 = time.time()
new_sample_file_count = 0
for missing_sample_file in find_missing_sample_files(sample_limit=args.sample_limit):
    print('  {}'.format(missing_sample_file))
    path_match = path_pattern.search(missing_sample_file)
    if path_match is None:
        raise Exception()
    elif missing_sample_file.endswith('.json'):
        print('    project: {}'.format(path_match.group('project_id')))
        print('    sample: {}'.format(path_match.group('sample_id')))
        sample_id = int(path_match.group('sample_id'))
        with session_manager_from_db_uri(os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
            sample_file_type_meta_id = imicrobe_db_session.query(
                models.Sample_file_type).filter(
                    models.Sample_file_type.type_ == 'Meta').one().sample_file_type_id

            sample = imicrobe_db_session.query(models.Sample).filter(models.Sample.sample_id == sample_id).one()
            #print('    sample has {} sample files'.format(len(sample.sample_file_list)))

            sample_file = models.Sample_file(
                file_=missing_sample_file,
                sample_id=sample.sample_id,
                sample_file_type_id=sample_file_type_meta_id)
            imicrobe_db_session.add(sample_file)
            #sample_file.file_ = missing_sample_file
            sample_file.sample_file_type = imicrobe_db_session.query(
                models.Sample_file_type).filter(
                    models.Sample_file_type.type_ == 'Meta').one()
            sample.sample_file_list.append(sample_file)
            new_sample_file_count += 1

    else:
        print('  not a JSON file: {}'.format(missing_sample_file))

print('inserted {} sample_file rows in {:5.2f}s'.format(new_sample_file_count, time.time()-t0))
