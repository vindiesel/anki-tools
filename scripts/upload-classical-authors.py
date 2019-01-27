#!/usr/bin/env python3
# -*- coding=utf8 -*-

# standard library
import argparse
import json
import itertools


# third party
import pandas as pd
import requests


ANKI_VERSION_NUMBER = 6
RAW_CLASSICS_URL = 'https://think.cs.vt.edu/corgis/csv/classics/classics.csv'


def chunk(iterable, n=100):
    it = iter(iterable)
    batch = tuple(itertools.islice(it, n))
    while batch:
        yield batch
        batch = tuple(itertools.islice(it, n))


def csv_column_to_field(pd_row, column):
    return str(pd_row.loc[column]).strip()


def post_anki(json):
    resp = requests.post('http://localhost:8765', json=json, headers={'Accept': 'application/json'})
    resp.raise_for_status()
    return resp.json()


def assert_key_in_dict(k, d):
    assert k in d, f"key={k} is not in dictionary={d}"


def main():
    # Steps
    # 1. Confirm API is still up
    # 2. Load periodic data and convert into notes
    # 3. Upload notes
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config-path', type=str, required=True)
    args = parser.parse_args()

    # Check API is up
    version_check = post_anki({'version': 6, 'action': 'version'})
    assert version_check['error'] is None
    assert version_check['result'] == 6
    print('ANKI api is up')

    # Load config
    with open(args.config_path, 'rt') as infile:
        config = json.load(infile)
    assert_key_in_dict('deck_name', config)
    deck_name = config['deck_name']
    assert_key_in_dict('note_type', config)
    note_type = config['note_type']

    # Confirm deck exists
    deck_check = post_anki({'version': ANKI_VERSION_NUMBER, 'action': 'deckNames'})
    assert deck_check['error'] is None
    assert deck_name in deck_check['result']
    print(f'Deck "{deck_name}" exists')

    # Confirm note type exists
    note_type_check = post_anki({'version': ANKI_VERSION_NUMBER, 'action': 'modelNames'})
    assert note_type_check['error'] is None
    assert note_type in note_type_check['result'], f'Note Type "{note_type}" is not in "{note_type_check}"'
    print(f'Note Type "{note_type}" exists')

    # columns -> fields mapping
    assert_key_in_dict('csv_columns_to_note_fields', config)
    columns_to_fields_map = config['csv_columns_to_note_fields']

    # Load the periodic table CSV and convert to json for upload
    fields_to_upload = [
        {field: csv_column_to_field(row, column) for (column, field) in columns_to_fields_map.items()}
        for _, row in pd.read_csv(RAW_CLASSICS_URL, encoding='utf-8').iterrows()
    ]
    notes_to_upload = [
        {
            'deckName': deck_name,
            'modelName': note_type,
            'fields': fields,
            'options': {
                'allowDuplicate': False
            },
            'tags': []
        }
        for fields in fields_to_upload
    ]
    print(f'{len(notes_to_upload)} notes to upload')

    # Can we upload these notes to ANKI?
    for notes_batch in chunk(notes_to_upload, n=100):
        can_add_notes_check = post_anki({
            'version': ANKI_VERSION_NUMBER,
            'action': 'canAddNotes',
            'params': {
                'notes': notes_batch
            }
        })
        assert can_add_notes_check['error'] is None
        assert all(can_add_notes_check['result']), f'{sum(can_add_notes_check["result"])} / {len(can_add_notes_check["result"])} notes are valid'
        print(f'{len(notes_batch)} notes confirmed to be added')

    # Upload notes
    for notes_batch in chunk(notes_to_upload, n=100):        
        add_notes_resp = post_anki({
            'version': ANKI_VERSION_NUMBER,
            'action': 'addNotes',
            'params': {
                'notes': notes_batch
            }
        })
        if add_notes_resp['error'] is not None:
            print(f'Error "{add_notes_resp["error"]}" adding notes')
        num_failures = sum(result is None for result in add_notes_resp['result'])
        if num_failures > 0:
            print(f'Failed to add {num_failures} / {len(add_notes_resp["result"])} notes')
            for (note, result) in zip(notes_to_upload, add_notes_resp['result']):
                if not result:
                    print(f'\tNote: "{note}"')
        else:
            print(f'{len(notes_batch)} notes added')


if __name__ == '__main__':
    main()