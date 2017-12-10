import os

from irods.keywords import FORCE_FLAG_KW
from irods.session import iRODSSession
from irods.exception import CAT_NO_ROWS_FOUND, CollectionDoesNotExist


def irods_session_manager():
    return iRODSSession(irods_env_file=os.path.expanduser('~/.irods/irods_environment.json'))


def irods_create_collection(irods_session, target_collection_path):
    """Create the specified collection and all parent collections
    that do not exist.

    :param irods_session:
    :param target_collection_path:
    :return:
    """
    collection_list = []
    parent = target_collection_path
    child = 'just to get started'
    while len(child) > 0:
        try:
            irods_session.collections.get(parent)
            break
        except CollectionDoesNotExist:
            collection_list.insert(0, parent)
            parent, child = os.path.split(parent)
            print('parent: {}'.format(parent))
            print('child : {}'.format(child))

    while len(collection_list) > 0:
        collection_path = collection_list.pop(0)
        print('creating collection "{}"'.format(collection_path))
        irods_session.collections.create(collection_path)


def irods_data_object_checksums_match(irods_session, path_1, path_2):
    data_object_1 = irods_session.data_objects.get(path_1)
    data_object_2 = irods_session.data_objects.get(path_2)
    print('data object "{}" has checksum {}'.format(path_1, data_object_1.checksum))
    print('data object "{}" has checksum {}'.format(path_2, data_object_2.checksum))
    return data_object_1.checksum == data_object_2.checksum


def irods_copy(irods_session, src_path, dest_path):
    irods_session.data_objects.copy(src_path=src_path, dest_path=dest_path, **{FORCE_FLAG_KW: True})


def irods_delete(irods_session, target_path):
    try:
        irods_session.data_objects.unlink(path=target_path, force=True)
    except CAT_NO_ROWS_FOUND:
        print('unable to delete data_object "{}" because it does not exist'.format(target_path))


def irods_delete_collection(irods_session, target_collection_path):
    try:
        irods_session.collections.remove(target_collection_path)
    except CAT_NO_ROWS_FOUND:
        print('unable to delete collection "{}" because it does not exist'.format(target_collection_path))
