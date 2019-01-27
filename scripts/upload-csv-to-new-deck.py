#!/usr/bin/env python3
# -*- coding=utf8 -*-

"""
usage: python upload-csv-to-new-deck.py -c <config_path>
description: Download a flat CSV file, transform (rename, clean, filter) fields in that CSV and upload to an ANKI notebook 
"""

# standard library
import argparse
import json
import itertools
import os
import sys
import typing


# third party
import pandas as pd
import requests


ANKI_VERSION_NUMBER = 6


def chunk(iterable, n=100):
    it = iter(iterable)
    batch = tuple(itertools.islice(it, n))
    while batch:
        yield batch
        batch = tuple(itertools.islice(it, n))


def pd_row_to_note_fields(pd_row, columns_to_note_fields):
    note = dict()
    for (column, field) in columns_to_note_fields.items():
        raw_value = pd_row.loc[column]
        if pd.notnull(raw_value) and str(raw_value).strip():
            note[field] = str(raw_value).strip()
    return note


def post_anki(json):
    resp = requests.post('http://localhost:8765', json=json, headers={'Accept': 'application/json'})
    resp.raise_for_status()
    return resp.json()


def assert_key_in_dict(k, d):
    assert k in d, f"key={k} is not in dictionary={d}"


class ScriptConfig(typing.NamedTuple):
    csv_path: str
    deck_name: str
    note_type: str
    columns_to_note_fields: dict
    allow_duplicates: bool
    index_field: str


def parse_config(config_path):
    assert os.path.exists(config_path), f'Config Path "{config_path}" does not exist'
    with open(config_path, 'rt') as infile:
        config_dict = json.load(infile)
    fields = ('csv_path', 'deck_name', 'note_type', 'columns_to_note_fields', 'allow_duplicates', 'index_field')
    for field in fields:
        assert_key_in_dict(field, config_dict)
    return ScriptConfig(**{k:v for k, v in config_dict.items() if k in fields})


def main():
    # Steps
    # 1. Confirm API is still up
    # 2. Load periodic data and convert into notes
    # 3. Upload notes
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=parse_config, required=True)
    config = parser.parse_args().config
    print(f'Running script with Config "{config}"')

    # Check API is up
    version_check = post_anki({'version': 6, 'action': 'version'})
    assert version_check['error'] is None
    assert version_check['result'] == 6
    print('ANKI api is up')

    # Confirm deck exists
    deck_check = post_anki({'version': ANKI_VERSION_NUMBER, 'action': 'deckNames'})
    assert deck_check['error'] is None
    assert config.deck_name in deck_check['result']
    print(f'Deck "{config.deck_name}" exists')

    # Confirm note type exists
    note_type_check = post_anki({'version': ANKI_VERSION_NUMBER, 'action': 'modelNames'})
    assert note_type_check['error'] is None
    assert config.note_type in note_type_check['result']
    print(f'Note Type "{config.note_type}" exists')

    # Load csv
    df_input = pd.read_csv(config.csv_path, encoding='utf-8')
    print(f'{df_input.shape[0]} rows, {df_input.shape[1]} in input csv')

    # Convert each row in CSV to dictionary and filter out rows without a valid index element
    fields_to_upload = list(
        filter(
            lambda f: config.index_field in f,
            map(
                lambda r: pd_row_to_note_fields(r[1], config.columns_to_note_fields),
                df_input.iterrows()
            )
        )
    )
    print(f'{len(fields_to_upload)} rows after parsing')

    # if not allow duplicates, then make sure there are no duplicates
    if not config.allow_duplicates:
        index_field_seen = set()
        for fields in fields_to_upload:
            if fields[config.index_field] in index_field_seen:
                print(f'Index Field "{fields[config.index_field]}" appears multiple times. Exiting.')
                sys.exit(1)
            index_field_seen.add(fields[config.index_field])

    # json for uploading notes
    notes_to_upload = [
        {
            'deckName': config.deck_name,
            'modelName': config.note_type,
            'fields': fields,
            'options': {
                'allowDuplicate': config.allow_duplicates
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
        if not all(can_add_notes_check['result']):
            for (note, result) in zip(notes_batch, can_add_notes_check['result']):
                if not result:
                    print(f"\tFailed Note Fields: {note['fields']}")
            print(f'{sum(can_add_notes_check["result"])} / {len(can_add_notes_check["result"])} notes are valid. Exiting')
            sys.exit(1)
        print(f'{len(notes_batch)} notes confirmed to be added')

    # Upload notes
    notes_added_num_successes, notes_added_num_failures = 0, 0
    for notes_batch in chunk(notes_to_upload, n=100):        
        add_notes_resp = post_anki({
            'version': ANKI_VERSION_NUMBER,
            'action': 'addNotes',
            'params': {
                'notes': notes_batch
            }
        })
        if add_notes_resp['error'] is not None:
            print(f'Error {add_notes_resp["error"]} adding notes.')
        notes_batch_outcomes = [result is not None for result in add_notes_resp['result']]
        notes_batch_num_successes = sum(notes_batch_outcomes)
        notes_batch_num_failures  = len(notes_batch_outcomes) - notes_batch_num_successes
        if notes_batch_num_failures > 0:
            print(f'Failed to add {num_failures} / {len(add_notes_resp["result"])} notes')
            for (note, added_successfully) in zip(notes_batch, notes_batch_outcomes):
                if not added_successfully:
                    print(f'\tNote: {note}')
        notes_added_num_successes += notes_batch_num_successes
        notes_added_num_failures += notes_batch_num_failures
        print(f'{notes_added_num_successes} notes added successfully, {notes_added_num_failures} notes failed to add')


if __name__ == '__main__':
    main()