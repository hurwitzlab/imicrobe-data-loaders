import os

import imicrobe.models as models
import imicrobe.util as util
import imicrobe.util.irods as irods
from orminator import session_manager_from_db_uri


# check every row in table sample_file
sample_file_i = 0
with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
    for sample_file_group in util.grouper(imicrobe_db_session.query(models.Sample_file).all(), n=100):

        with irods.irods_session_manager() as irods_session:
            for sample_file in [f for f in sample_file_group if f is not None]:
                sample_file_i += 1
                print('{}: "{}"'.format(sample_file_i, sample_file.file_))
                if irods.irods_data_object_exists(irods_session=irods_session, target_path=sample_file.file_):
                    pass
                else:
                    print('  found "{}" in table sample_file but not in /iplant data store'.format(sample_file.file_))
                    quit()