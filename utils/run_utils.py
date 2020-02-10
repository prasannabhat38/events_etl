from datetime import datetime
import json
import os


ETL_RUN_DIR = 'events_etl/run'
OUTPUT_DIR = 'events_etl/output'

def get_etl_run_dir():
    return os.path.join(os.getcwd(), '../' + ETL_RUN_DIR)

def get_run_log_filename_for_key(file_key):
    run_file_name = '-'.join(file_key.split('/')[0:-1]) + '.log'
    path = os.path.join(get_etl_run_dir(), run_file_name)

    return path

def update_run_log(file_key):
    '''
    Create/Update run file for file_key.
    Each run file name is a date prefix in s3_key and contains all the keys (file)
    with the same date prefix (organised by dated directory)

    for e.g: file_key = 2020/01/01/0f6e0555e9f34063b711a676d13e38f9.json
    run_log_file = 2020-01-01.log and will contain the entry 0f6e0555e9f34063b711a676d13e38f9.json

    :param file_key:
    :return:
    '''
    run_file_name = '-'.join(file_key.split('/')[0:-1]) + '.log'
    s3_event_file_name = file_key.split('/')[-1]

    path = os.path.join(get_etl_run_dir(), run_file_name)

    try:
        if not os.path.isfile(path):
            # create new run file
            print 'Creating new run file for date: {}'.format(run_file_name)

            with open(path, 'w') as f:
                f.write(s3_event_file_name)
        else:
            with open(path, 'a') as f:
                f.write('\n')
                f.write(s3_event_file_name)
    except Exception as e:
        print 'Error updating run log file'.format(e)


def is_event_log_processed(file_key):
    '''
    Checks if the file_key is already processed by the ETL processor.

    Each run_log file name is a date prefix in s3_key and contains all the keys (file)
    with the same date prefix (organised by dated directory). See update_run_log() above

    for e.g: file_key = 2020/01/01/0f6e0555e9f34063b711a676d13e38f9.json
    run_log_file = 2020-01-01.log and will contain the entry 0f6e0555e9f34063b711a676d13e38f9.json

    :param file_key:
    :return: True if event_log correspond to file_key is not processed else False
    :return:
    '''
    run_log_file = get_run_log_filename_for_key(file_key)
    file_key_processed = False

    print 'Checking if the run file exists for path: {}'.format(run_log_file)
    if os.path.isfile(run_log_file):
        # If the dated run log file exists check if the key corresponding to S3 event log (.json) exists
        # If exists then the event log is processed else needs to be processed in this run
        with open(run_log_file, 'r') as f:
            s3_event_file_name = file_key.split('/')[-1]
            line = f.readline()

            while line:
                if line == s3_event_file_name:
                    file_key_processed = True
                    break
                line = f.readline()

    return file_key_processed

def update_run_status(last_run_status):
    '''
    Updates run status to status_log file

    :param status:
    :return:
    '''
    run_time_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    message = run_time_stamp + ' | ' + last_run_status

    file_name = os.path.join(get_etl_run_dir(), 'last_run_status.log')

    try:
        with open(file_name, 'w') as f:
            f.write(message)
    except Exception as e:
        print 'Error updating run status file. {}'.format(e)

def dump_output_to_file(file_key, event_response):
    output_dir = os.path.join(os.getcwd(), '../' + OUTPUT_DIR)
    file = os.path.join(output_dir, file_key.split('/')[-1])

    print 'Writing output to file: {}'.format(file)

    with open(file, 'w') as f:
        json.dump(event_response, f)
