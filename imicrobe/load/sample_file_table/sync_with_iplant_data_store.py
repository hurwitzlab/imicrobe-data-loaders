import os

import imicrobe.models as models
from orminator import session_manager_from_db_uri


# check every row in table sample_file

with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
    for sample_file in imicrobe_db_session.query(models.Sample_file).all():
        print(dir(sample_file))
        print(sample_file.file_)
        print(sample_file.sample_file_type)
        break
