import os

import pytest
import sqlalchemy as sa

from imicrobe.load import models
from orminator import session_manager_from_db_uri

@pytest.fixture()
def test_session():
    engine = sa.create_engine(os.environ['IMICROBE_DB_URI'], echo=False)

    try:
        with engine.connect() as connection:
            connection.execute('DROP DATABASE imicrobe_test')
    except:
        pass

    with engine.connect() as connection:
        connection.execute('CREATE DATABASE imicrobe_test')

    test_db_uri = os.environ['IMICROBE_DB_URI'] + '_test'
    test_engine = sa.create_engine(test_db_uri, echo=False)
    models.Model.metadata.create_all(test_engine)
    with session_manager_from_db_uri(test_db_uri) as test_session:
        yield test_session

    with engine.connect() as connection:
        connection.execute('DROP DATABASE imicrobe_test')
