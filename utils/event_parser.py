from datetime import datetime
import json

from s3_utils import get_s3_file

# Metadata can have arbitrary keys based on the event type
# This map will maintain the list of keys specific to an event type to provide additional context
# Note: Update the map for any new_event_type or adding new metdata key to an
# event for it to be added to final message
# TODO: make it a config

METADATA_KEYS_DICT = {
                        'change_operator_identity' : ['action'],
                        'page_visit' : ['path'],
                        'update_bot': ['job_title']
                    }

EVENT_KEYS = ['user_email', 'created_at', 'ip', 'event_name']

def _clean_up(data):
    '''
    :param data:
    :return:
    '''
    # TODO: Implement clean up operations on string
    return data

def _parse_event(event_raw_json):
    '''
    Parse event json record to create a generic event message

    :param event_raw_json: Raw event json # {"id": "a2b6604c-913b-4598-858e-285253fe2721", "created_at": "2020-01-30 00:10:35", "user_email": "wallsdenise@thompson.com", "ip": "209.154.163.207", "event_name": "message_saved", "metadata": {"message_id": 66417}}
    :return:
    '''
    event_dict = {}

    try:
        for key in EVENT_KEYS:
            event_dict[key] = event_raw_json.get(key)

        # Parse metadata
        if event_raw_json.get('metadata') and \
                (event_raw_json.get('event_name') and event_raw_json.get('event_name') in METADATA_KEYS_DICT):
            meta = {}
            metadata = event_raw_json.get('metadata')

            for metadata_key in METADATA_KEYS_DICT.get(event_raw_json.get('event_name')):
                meta[metadata_key] = _clean_up(metadata.get(metadata_key))

            event_dict['metadata'] = meta
    except Exception as e:
        print 'Error parsing event for id: {}. Exception: e'.format(event_raw_json.get('id'), e)

    return event_dict

def parse_event_log(self, file_key):
    '''
    Read and parse event log corresponding to file_key

    :param file_key:
    :return: JSON response containing the events list from the file_key
    '''
    print 'Reading file for key: {}'.format(file_key)
    start_time = datetime.now()

    file_obj = get_s3_file(file_key)
    events = []

    # reading s3 file stream line by line to handle parse issues with invalid event logs
    row = file_obj._raw_stream.readline()

    while row:
        # Handle bad rows
        try:
            event_json = json.loads(row)
            event = _parse_event(event_json)

            if event:
                events.append(event)
        except Exception as e:
            print 'Error converting event message [{}] to JSON. skipping row'.format(row)

        row = file_obj._raw_stream.readline()

    end_time = datetime.now()
    print 'Finished reading file. Time taken: {}'.format(end_time - start_time)

    # Convert to json
    events_response = json.dumps({'file_key': file_key, 'events' : events })

    return events_response

