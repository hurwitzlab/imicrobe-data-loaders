from loader.imicrobe.uproc import tables


def test_protein(test_session):
    protein_type = tables.Protein_type()
    test_session.add(protein_type)
    test_session.flush()
    print('protein_type_id: {}'.format(protein_type.protein_type_id))

    all_proteins = test_session.query(tables.Protein).all()
    assert len(all_proteins) == 0

    protein = tables.Protein(
        accession='accession',
        description='description')
    protein.protein_type_id = protein_type.protein_type_id
    test_session.add(protein)
    test_session.flush()

    all_proteins = test_session.query(tables.Protein).all()
    assert len(all_proteins) == 1

    # test cascade delete
    test_session.delete(protein_type)
    all_proteins = test_session.query(tables.Protein).all()
    assert len(all_proteins) == 0
