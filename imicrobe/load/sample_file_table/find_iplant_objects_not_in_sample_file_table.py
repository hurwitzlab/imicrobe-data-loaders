import imicrobe.util.irods as irods


with irods.irods_session_manager() as irods_session:
    t = irods.get_project_sample_collection_paths(sample_limit=100)
