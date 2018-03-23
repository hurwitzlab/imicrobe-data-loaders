import pprint

import imicrobe.models as models
import imicrobe.util.irods as irods
from orminator import session_manager_from_db_uri


for project, samples in irods.get_project_sample_collection_paths(sample_limit=100).items():
    print('project "{}"'.format(project))
    for sample in samples:
        for parent_collection, child_collections, data_objects in irods.walk(sample):
            for data_object in data_objects:
                with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
                    s = imicrobe_db_session.query(
                        models.Sample_file).filter(
                            models.Sample_file.file_ == data_object.path).one_or_none()

                    if s is None:
                        print('failed to find "{}" in imicrobe sample_file table'.format(data_object.path))
