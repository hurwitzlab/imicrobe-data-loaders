import sqlite3

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import loaders.uproc_results.kegg.models as models


def test_duplicate_kegg_annotation_id():
    engine = create_engine('sqlite:///:memory:', echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    a = models.Kegg_annotation(
        kegg_annotation_id='K00001',
        name='kegg 00111',
        description='',
        pathway='',
        module='')

    session.commit(a)

    b= models.Uproc_kegg_result(

    )