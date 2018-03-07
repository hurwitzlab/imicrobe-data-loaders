import io
import re
import time
from collections import defaultdict

import requests
import requests_cache

from imicrobe.util import grouper


requests_cache.install_cache('kegg_api_cache')


def get_kegg_annotations(kegg_ids):
    all_kegg_annotations = {}
    all_bad_kegg_ids = set()
    # the missing_accessions_groups_of_10 generator returns groups of 10 KEGG ids
    # that are not already in the database and that are not 'bad' KEGG ids
    # the last group may be padded with 'None' if there are fewer than 10 KEGG ids
    for group_of_10 in grouper(sorted(kegg_ids), n=10):
        t0 = time.time()
        kegg_id_list = [k for k in group_of_10 if k is not None]
        #print(kegg_id_list)
        print('requesting {} KEGG annotation(s)'.format(len(kegg_id_list)))
        kegg_annotations, bad_kegg_ids = get_10_kegg_annotations(kegg_id_list)
        print('    received {} in {:5.2f}s'.format(len(kegg_annotations), time.time()-t0))
        all_kegg_annotations.update(kegg_annotations)
        all_bad_kegg_ids.update(bad_kegg_ids)

    return all_kegg_annotations, all_bad_kegg_ids


kegg_orthology_field_re = re.compile(r'^(?P<field_name>[A-Z]+)?(\s+)(?P<field_value>.+)$')


def get_10_kegg_annotations(kegg_ids):
    """ Request annotations for up to 10 KEGG ids. If a bad id is given there will be no response for it.

    The response from the KEGG API looks like this:
            ENTRY       K01467                      KO
            NAME        ampC
            DEFINITION  beta-lactamase class C [EC:3.5.2.6]
            PATHWAY     ko01501  beta-Lactam resistance
                        ko02020  Two-component system
            MODULE      M00628  beta-Lactam resistance, AmpC system
            ...
            ENTRY       K00154                      KO
            NAME        E1.2.1.68
            DEFINITION  coniferyl-aldehyde dehydrogenase [EC:1.2.1.68]
            BRITE       Enzymes [BR:ko01000]
                         1. Oxidoreductases
                          1.2  Acting on the aldehyde or oxo group of donors
                           1.2.1  With NAD+ or NADP+ as acceptor
                            1.2.1.68  coniferyl-aldehyde dehydrogenase
                             K00154  E1.2.1.68; coniferyl-aldehyde dehydrogenase
            DBLINKS     COG: COG1012
                        GO: 0050269
            GENES       GQU: AWC35_21175
                        CED: LH89_09310 LH89_19560
                        SMW: SMWW4_v1c32370
                        SMAR: SM39_2711
                        SMAC: SMDB11_2482
            ...

    return: a dictionary of dictionaries that looks like this
        {
            'K01467': {
                'ENTRY': 'K01467                      KO',
                'NAME': 'ampC',
                'DEFINITION': '',
                'PATHWAY': '',
                'MODULE': '',
                ...
            },
            'K00154': {
                'ENTRY': 'K00154                      KO',
                'NAME': 'E1.2.1.68',
                'DEFINITION': '',
                'PATHWAY': '',
                'MODULE': '',
                ...
            }

        }
    and a (possibly empty) set of KEGG ids for which no annotation was returned

    """

    debug = False

    ko_id_list = '+'.join(['ko:{}'.format(k) for k in kegg_ids])
    response = requests.get('http://rest.kegg.jp/get/{}'.format(ko_id_list))
    if response.status_code == 404:
        print('no annotations returned')
        all_entries = {}
        bad_kegg_ids = set(kegg_ids)
        return all_entries, bad_kegg_ids
    if response.status_code != 200:
        error_msg = 'ERROR: response to "{}" is {}'.format(response.url, response.status_code)
        print(error_msg)
        raise Exception(error_msg)
    else:
        all_entries = defaultdict(lambda: defaultdict(list))
        kegg_id = None
        field_name = None
        for line in io.StringIO(response.text).readlines():
            field_match = kegg_orthology_field_re.search(line.rstrip())
            if field_match is None:
                # this line separates entries
                kegg_id = None
                field_name = None
            else:
                field_value = field_match.group('field_value')
                if 'field_name' in field_match.groupdict():
                    field_name = field_match.group('field_name')
                    if field_name == 'ENTRY':
                        kegg_id, *_ = field_value.split(' ')
                        # print('KEGG id: "{}"'.format(kegg_id))
                else:
                    # just a field value is present
                    pass

                all_entries[kegg_id][field_name].append(field_value)

        # were any of the KEGG ids bad?
        bad_kegg_ids = {k for k in kegg_ids} - {k for k in all_entries.keys()}

        return all_entries, bad_kegg_ids