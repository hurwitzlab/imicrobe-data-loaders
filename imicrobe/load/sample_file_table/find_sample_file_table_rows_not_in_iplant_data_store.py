import os
import time

import imicrobe.models as models
import imicrobe.util as util
import imicrobe.util.irods as irods
from orminator import session_manager_from_db_uri


# check every row in table sample_file
t0 = time.time()
with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
    sample_file_list = [
        sample_file.file_
        for sample_file
        in imicrobe_db_session.query(models.Sample_file).all()]

print('found {} sample file table rows in {:5.2f}s'.format(len(sample_file_list), time.time()-t0))

t0 = time.time()
sample_file_i = 0
for sample_file_group in util.grouper(sample_file_list, n=100):
        with irods.irods_session_manager() as irods_session:
            for sample_file in [f for f in sample_file_group if f is not None]:
                sample_file_i += 1
                #print('{}: "{}"'.format(sample_file_i, sample_file.file_))
                if irods.irods_data_object_exists(irods_session=irods_session, target_path=sample_file):
                    pass
                elif irods.irods_collection_exists(irods_session=irods_session, collection_path=sample_file):
                    pass
                else:
                    print('{} found "{}" in table sample_file but not in /iplant data store'.format(sample_file_i, sample_file))

print('done in {:5.2f}s'.format(time.time()-t0))
