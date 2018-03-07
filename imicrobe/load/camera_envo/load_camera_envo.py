from collections import defaultdict
import os

import pandas as pd

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

import imicrobe_model.models as models


def main():
    # connect to database on server
    # e.g. mysql+pymysql://load:<password>@localhost/load
    db_uri = os.environ.get('IMICROBE_DB_URI')
    engine = sa.create_engine(db_uri, echo=False)

    Session = sessionmaker(bind=engine)
    session = Session()

    load(engine, session)

def load(engine, session):

    camera_metadata_df = pd.read_csv(
        filepath_or_buffer='CameraMetadata_ENVO_working_copy.csv')
    #print(camera_metadata_df.head())

    # check for duplicate SAMPLE_ACC
    sample_acc_occurences = defaultdict(list)
    duplicate_sample_acc_list = []
    for r, row in camera_metadata_df.iterrows():
        sample_acc_occurences[row.SAMPLE_ACC].append((r, row))

    for k, v in sample_acc_occurences.items():
        if len(v) == 1:
            pass
        else:
            r_0, row_0 = v[0]
            print('first occurence of SAMPLE_ACC "{}" is on row {}'.format(
                row_0.SAMPLE_ACC, r_0))
            for r, row in v[1:]:
                print('  duplicate SAMPLE_ACC "{}" on row {}'.format(
                                    row.SAMPLE_ACC, r))
                if row.equals(row_0):
                    pass
                    #print('    ha! the rows are equal')
                else:
                    print('    the rows are different')
                    for i, r_0_i, r_i in zip(row_0.index, row_0, row):
                        if str(r_0_i) == 'nan' and str(r_i) == 'nan':
                            pass
                        elif r_0_i == r_i:
                            pass
                        else:
                            print('      "{}": "{}" and "{}" are unequal'.format(i, r_0_i, r_i))

    print('{} SAMPLE_ACC are duplicated'.format(len([k for k, v in sample_acc_occurences.items() if len(v) > 1])))



if __name__ == '__main__':
    main()
