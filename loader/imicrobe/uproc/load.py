"""
Read UProC results files from an IRODS collections and load the imicrobe database.
"""
import gzip
import itertools
import os
import re
import subprocess
import time

import pandas as pd
import pandas.errors
import sqlalchemy

from orminator import session_manager_from_db_uri

import loader.imicrobe.uproc.tables as uproc_tables
from loader.util import grouper
from loader.util.irods import get_project_sample_collection_paths, irods_session_manager
from loader.util.kegg import get_kegg_annotations


def main():
    drop_tables()
    drop_annotation_tables()
    create_tables()
    load_protein_type_table()
    load_protein_evidence_type_table()

    download_pfam_file()

    load_annotations()


def create_tables():
    engine = sqlalchemy.create_engine(os.environ['IMICROBE_DB_URI'], echo=False)

    uproc_tables.Protein_type.__table__.create(bind=engine, checkfirst=True)
    uproc_tables.Protein.__table__.create(bind=engine, checkfirst=True)
    uproc_tables.Protein_evidence_type.__table__.create(bind=engine, checkfirst=True)
    uproc_tables.Sample_to_protein.__table__.create(bind=engine, checkfirst=True)

    # is this table needed?
    uproc_tables.Protein_evidence.__table__.create(bind=engine, checkfirst=True)


def drop_annotation_tables():
    engine = sqlalchemy.create_engine(os.environ['IMICROBE_DB_URI'], echo=False)

    drop(engine=engine, table=uproc_tables.Protein.__table__)
    drop(engine=engine, table=uproc_tables.Protein_type.__table__)


def drop_tables():
    engine = sqlalchemy.create_engine(os.environ['IMICROBE_DB_URI'], echo=False)

    drop(engine=engine, table=uproc_tables.Protein_evidence.__table__)
    drop(engine=engine, table=uproc_tables.Sample_to_protein.__table__)

    drop(engine=engine, table=uproc_tables.Protein_evidence_type.__table__)


def drop(engine, table):
    print('dropping table "{}"'.format(table.name))
    try:
        with engine.connect() as connection:
            for fk in table.foreign_key_constraints:
                try:
                    print('  dropping foreign key "{}"'.format(fk.name))
                    connection.execute('ALTER TABLE {} DROP FOREIGN KEY {}'.format(
                        table.name, fk.name))
                    print('    dropped foreign key')
                    print('  dropping index {}""'.format(fk.name))
                    connection.execute('ALTER TABLE {} DROP INDEX {}'.format(
                        table.name, fk.name))
                    print('    dropped index')
                except:
                    print('  failed to drop foreign key "{}"'.format(fk.name))
                    #print('  dropping key "{}"'.format(fk.name))
                    #connection.execute('ALTER TABLE {} DROP KEY {}'.format(
                    #    table.name, fk.name))
                    #print('    dropped key')
        print('  drop table "{}"'.format(table.name))
        table.drop(bind=engine, checkfirst=True)
        print('dropped table "{}"'.format(table.name))
    except Exception as e:
        print(e)


def load_protein_type_table():
    with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
        protein_types = imicrobe_db_session.query(uproc_tables.Protein_type).all()
        protein_type_names = [protein_type.type_ for protein_type in protein_types]
        print(protein_type_names)
        for p in ('KEGG', 'PFAM'):
            if p in protein_type_names:
                print('protein_type "{}" already exists'.format(p))
            else:
                print('inserting protein_type "{}"'.format(p))
                imicrobe_db_session.add(uproc_tables.Protein_type(type_=p))


def load_protein_evidence_type_table():
    with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
        protein_evidence_types = imicrobe_db_session.query(uproc_tables.Protein_evidence_type).all()
        protein_evidence_type_names = [protien_evidence_type.type_ for protien_evidence_type in protein_evidence_types]
        print(protein_evidence_type_names)
        for p in ('UProC', 'InterPro'):
            if p in protein_evidence_type_names:
                print('protein_evidence_type "{}" already exists'.format(p))
            else:
                print('inserting protein_evidence_type "{}"'.format(p))
                imicrobe_db_session.add(uproc_tables.Protein_evidence_type(type_=p))


def download_pfam_file():
    pfam_fp = 'pfamA.txt.gz'
    if os.path.exists(pfam_fp):
        print('PFam file {} already exists'.format(pfam_fp))
    else:
        t0 = time.time()
        subprocess.run(['wget', 'ftp://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/database_files/pfamA.txt.gz', 'pfamA.txt.gz'])
        print('downloaded PFam file in {:5.2f}s'.format(time.time() - t0))


def load_annotations():
    """Read UProC KEGG results files. Load KEGG annotations as needed.

    :return:
    """

    uproc_results_file_name_re = re.compile(r'\.uproc\.(kegg|pfam\d+)$')

    uproc_results_service = UProCResultsService()

    uproc_results_service.insert_pfam_annotations_from_file(pfamA_fp='pfamA.txt.gz')

    uproc_results_limit = 10000
    uproc_results_count = 0
    imicrobe_project_root = '/iplant/home/shared/imicrobe/projects'
    project_to_sample_collection_paths = get_project_sample_collection_paths(
        collection_root=imicrobe_project_root)

    sample_count = sum([len(s) for p, s in project_to_sample_collection_paths.items()])
    sample_index = 0
    for project_collection_path, sample_collection_paths in project_to_sample_collection_paths.items():
        print('project collection: "{}"'.format(project_collection_path))

        with irods_session_manager() as irods_session:
            for sample_collection_path in sample_collection_paths:
                sample_index += 1
                print('inserting results for sample {} of {}'.format(sample_index, sample_count))
                sample_collection = irods_session.collections.get(sample_collection_path)
                sample_data_objects = sample_collection.data_objects

                # put all parsed data frames for this sample into a list
                # in most cases there will be only one data frame
                sample_uproc_results_data_object_list = []
                for data_object in sample_data_objects:
                    #print('\t' + data_object.name)
                    m = uproc_results_file_name_re.search(data_object.name)
                    if m is None:
                        pass
                    elif data_object.size == 0:
                        print('{} is empty'.format(data_object.path))
                    else:
                        # it can happen that a sample has more than one sample file
                        # there will be one UProC result file for each sample file
                        # all UProC results for a single sample must be combined
                        #print(data_object.path)
                        sample_uproc_results_data_object_list.append(data_object)

                if len(sample_uproc_results_data_object_list) == 0:
                    print('  found no UProC results in\n\t{}'.format(sample_collection_path))
                elif uproc_results_service.count_uproc_results_for_sample(sample_id=sample_collection.name) > 0:
                    # assume all data for this sample has been inserted
                    print('* results for sample {} have been loaded'.format(sample_collection.name))
                else:
                    print('  reading UProC results for sample {}\n\t{}'.format(
                        sample_collection.name,
                        '\n\t'.join([s.name for s in sample_uproc_results_data_object_list])))
                    sample_uproc_results_df_list = []
                    for sample_uproc_results_data_object in sample_uproc_results_data_object_list:
                        uproc_results_df = parse_uproc_results(sample_uproc_results_data_object)
                        sample_uproc_results_df_list.append(uproc_results_df)
                        #print(uproc_results_df.head())

                    # combine the dataframes and insert the UProC results values
                    if len(sample_uproc_results_df_list) == 0:
                        pass
                    else:
                        combined_df = sample_uproc_results_df_list[0]
                        for df in sample_uproc_results_df_list[1:]:
                            combined_df = combined_df.add(df, fill_value=0.0)

                        combined_df.sort_values(by='read_count', inplace=True, ascending=False)
                        print('  combined data {}:\n{}'.format(combined_df.shape, combined_df.head()))
                        t0 = time.time()

                        uproc_results_service.insert_kegg_annotations_for_sample(
                            annotation_results_df=combined_df[
                                [accession.startswith('K') for accession in combined_df.index]])

                        uproc_results_service.insert_uproc_results_for_sample(
                            sample_id=sample_collection.name,
                            uproc_results_df=combined_df)

                        insertion_count = uproc_results_service.count_uproc_results_for_sample(
                            sample_id=sample_collection.name)

                        print('  inserted {} annotations for sample in {:5.2f}s'.format(
                            insertion_count, time.time()-t0))
                        # count samples rather than files to decide if we should stop early
                        uproc_results_count += 1

            if uproc_results_limit <= 0:
                # there is no limit
                pass
            elif uproc_results_limit < uproc_results_count:
                # there is a limit and it has not been met
                pass
            else:
                print('UProC result limit {} has been met'.format(uproc_results_limit))
                break

    with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
        sample_to_protein_count = imicrobe_db_session.query(uproc_tables.Sample_to_protein).count()
        print('loaded {} rows in sample_to_protein table'.format(sample_to_protein_count))

        kegg_result_count = imicrobe_db_session.query(uproc_tables.Sample_to_protein).join(
            uproc_tables.Protein).join(uproc_tables.Protein_type).filter(
                uproc_tables.Protein_type.type_ == 'KEGG').count()

        print('{} rows of sample_to_protein table reference KEGG annotations'.format(kegg_result_count))

        pfam_result_count = imicrobe_db_session.query(uproc_tables.Sample_to_protein).join(
            uproc_tables.Protein).join(uproc_tables.Protein_type).filter(
                uproc_tables.Protein_type.type_ == 'PFAM').count()

        print('{} rows of sample_to_protein table reference PFam annotations'.format(pfam_result_count))

    print('{} bad accessions'.format(
        len(uproc_results_service.bad_accessions)))
        #'\t\n'.join(sorted(list(uproc_results_service.bad_accessions)))))


def parse_uproc_results(data_object):
    """Parse a UProC result file to a pandas.DataFrame, which will look like this:
                    read_count
        accession
        K00525        1793
        K06237         701
        K04077         655
        K00526         509
        K02703         428

    :param data_object: IRODS data object
    :return: pandas.DataFrame
    """
    with data_object.open('r+') as d:
        try:
            uproc_results_df = pd.read_csv(
                filepath_or_buffer=d,
                index_col=0,
                header=None,
                names=('accession', 'read_count'))
            return uproc_results_df
        except pd.errors.EmptyDataError:
            print('data object "{}" is empty'.format(data_object.path))
            return pd.DataFrame()


class UProCResultsService:
    """
    Handle querying KEGG REST for annotations and inserting UProC results
    in iMicrobe database.
    """
    def __init__(self):
        """Build a cache of KEGG annotations. Initialize it with annotations
        already in the iMicrobe database. As new annotations are downloaded and
        inserted into the iMicrobe database also add them to the cache.
        """
        self.bad_accessions = set()

        self.annotation_db_ids = {}
        with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
            protein_list = imicrobe_db_session.query(uproc_tables.Protein).all()
            for protein in protein_list:
                self.annotation_db_ids[protein.accession] = protein.protein_id
            print('found {} protein annotations in imicrobe database'.format(len(self.annotation_db_ids)))


    def insert_kegg_annotations_for_sample(self, annotation_results_df):
        """Insert all protein annotations for annotation_results_df that are not
        already in the database.

        :param annotation_results_df: pandas.DataFrame indexed by KEGG or PFAM accessions
        :return:
        """

        with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
            t0 = time.time()

            missing_accession_list = itertools.filterfalse(
                lambda kegg_id:
                    kegg_id in self.annotation_db_ids or kegg_id in self.bad_accessions,
                annotation_results_df.index)

            kegg_annotations, bad_kegg_ids = get_kegg_annotations(missing_accession_list)

            if len(bad_kegg_ids) > 0:
                print('** bad KEGG ids: {}'.format(bad_kegg_ids))
                self.bad_accessions.update(bad_kegg_ids)
                print('bad_accessions has {} element(s)'.format(len(self.bad_accessions)))

            for accession, annotation in kegg_annotations.items():

                if accession is None:
                    print('why is accession None?')
                else:
                    description = '{}\n{}\n{}\n{}'.format(
                        annotation['NAME'],
                        annotation['DEFINITION'],
                        annotation.get('PATHWAY', ''),
                        annotation.get('MODULE', ''))
                    new_protein_annotation = uproc_tables.Protein(
                        accession=accession,
                        description=description,
                        protein_type_id=imicrobe_db_session.query(
                            uproc_tables.Protein_type.protein_type_id).filter(
                            uproc_tables.Protein_type.type_ == self.get_protein_type(accession)).one()[0])
                    imicrobe_db_session.add(new_protein_annotation)
                    imicrobe_db_session.flush()

                    self.annotation_db_ids[accession] = new_protein_annotation.protein_id

        print('downloaded {} annotations in {:5.2f}s'.format(len(kegg_annotations), time.time()-t0))


    def insert_pfam_annotations_from_file(self, pfamA_fp):
        with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
            t0 = time.time()

            debug = False
            line_group_length = 2000
            insert_count = 0
            print('checking for PFam annotations missing from database')
            with gzip.open(pfamA_fp, 'rt', encoding='latin-1', errors='replace') as pfamA_file:
                for line_group in grouper(pfamA_file, line_group_length, fillvalue=None):
                    line_counter = 0
                    t00 = time.time()
                    for line in (line_ for line_ in line_group if line_ is not None):
                        line_counter += 1
                        pfam_acc, pfam_identifier, pfam_aliases, pfam_name, _, _, _, _, description, *_ = line.strip().split('\t')

                        if debug:
                            print('pfam accession  : {}'.format(pfam_acc))
                            print('pfam identifier : {}'.format(pfam_identifier))
                            print('pfam aliases    : {}'.format(pfam_aliases))
                            print('pfam name       : {}'.format(pfam_name))
                            print('description     : {}'.format(description))


                        protein_id_results = imicrobe_db_session.query(uproc_tables.Protein.protein_id).filter(
                                uproc_tables.Protein.accession == pfam_acc).one_or_none()
                        if protein_id_results is not None:
                            protein_id = protein_id_results[0]
                            # print('{} is already in the database'.format(pfam_acc))
                        else:
                            # insert
                            new_protein = uproc_tables.Protein(
                                accession=pfam_acc,
                                description=description,
                                protein_type_id=imicrobe_db_session.query(
                                    uproc_tables.Protein_type.protein_type_id).filter(
                                    uproc_tables.Protein_type.type_ == self.get_protein_type(pfam_acc)).one()[0]
                            )
                            imicrobe_db_session.add(new_protein)
                            insert_count += 1
                            imicrobe_db_session.flush()
                            protein_id = new_protein.protein_id

                        self.annotation_db_ids[pfam_acc] = protein_id

                    imicrobe_db_session.commit()
                    print('committed {} rows in {:5.1f}s ({:5.1f}s)'.format(
                        insert_count,
                        time.time() - t0,
                        time.time() - t00))

                print('table "{}" has {} PFAM rows'.format(
                    uproc_tables.Protein.__tablename__,
                    imicrobe_db_session.query(
                        uproc_tables.Protein).join(
                            uproc_tables.Protein_type).filter(
                                uproc_tables.Protein_type.type_ == 'PFAM').count()))


    def insert_uproc_results_for_sample(self, sample_id, uproc_results_df):
        """

        :param sample_id:
        :param uproc_results_df:
        :return:
        """
        print('inserting UProC results for sample_id {}'.format(sample_id))
        with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
            for accession, uproc_result_row in uproc_results_df.iterrows():
                # is the protein annotation already in table protein?
                #print('r: "{}" uproc_result_row:\n{}'.format(accession, uproc_result_row))
                if accession in self.annotation_db_ids:
                    protein_id = self.annotation_db_ids[accession]
                    #print('found accession "{}" in cache'.format(accession))
                    sample_to_protein = uproc_tables.Sample_to_protein(
                        protein_id=protein_id,
                        sample_id=sample_id,
                        protein_evidence_type_id=imicrobe_db_session.query(
                            uproc_tables.Protein_evidence_type.protein_evidence_type_id).filter(
                                uproc_tables.Protein_evidence_type.type_ == 'UProC').one()[0],
                        read_count=str(uproc_result_row.read_count))
                    imicrobe_db_session.add(sample_to_protein)
                else:
                    #print('annotation for "{}" is missing from cache'.format(accession))
                    self.bad_accessions.add(accession)


    def count_uproc_results_for_sample(self, sample_id):
        with session_manager_from_db_uri(db_uri=os.environ['IMICROBE_DB_URI']) as imicrobe_db_session:
            return imicrobe_db_session.query(uproc_tables.Sample_to_protein).filter(
                uproc_tables.Sample_to_protein.sample_id == sample_id).count()


    def get_protein_annotation(self, accession):
        if accession.startswith('P'):
            raise Exception('PFAM is not supported yet!')
        elif accession.startswith('K'):
            kegg_annotation = self.parse_kegg_annotation(accession=accession)
            protein_annotation = '{}\n{}\n{}\n{}'.format(
                kegg_annotation['NAME'],
                kegg_annotation['DEFINITION'],
                kegg_annotation.get('PATHWAY', ''),
                kegg_annotation.get('MODULE', ''))

        else:
            raise Exception('unknown accession "{}"'.format(accession))

        return protein_annotation


    def get_protein_type(self, accession):
        if accession.startswith('P'):
            return 'PFAM'
        elif accession.startswith('K'):
            return 'KEGG'
        else:
            raise Exception('failed to recognize accession "{}"'.format(accession))


if __name__ == '__main__':
    main()
