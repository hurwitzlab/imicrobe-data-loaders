import os

import imicrobe.models as models
import imicrobe.util.irods as irods
from orminator import session_manager_from_db_uri


# check every row in table sample_file

with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
    for sample_file in imicrobe_db_session.query(models.Sample_file).limit(10):
        print(dir(sample_file))
        print(sample_file.file_)
        print(sample_file.sample_file_type)

        with irods.irods_session_manager() as irods_session:
            if irods.irods_data_object_exists(irods_session=irods_session, target_path=sample_file.file_):
                pass
            else:
                print('found "{}" in table sample_file but not in /iplant data store')
