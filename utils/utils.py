from datetime import datetime
import os

ETL_RUN_DIR = 'events_etl/run'

def get_etl_run_dir():
    return os.path.join(os.getcwd(), '../' + ETL_RUN_DIR)

def update_run_log(file_key):
    '''
    Create/Update run file for file_key.
    Each run file name is a date prefix in s3_key and contains all the keys (file)
    with the same date prefix (organised by dated directory)

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
                f.write(s3_event_file_name)
    except Exception as e:
        print 'Error updating run log file'.format(e)

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

def format_event(raw_event):
    pass


def is_event_log_processed(file_key):
    '''
    Checks if the file_key is already processed by the ETL processor.

    :param file_key:
    :return: True if log correspond to file_key is not processed else False
    :return:
    '''
    # Run file name will be dated from the s3 file_key
    # e.g. 2020/01/01/<file>.json will correspond to 2020-01-01.log in run dir

    run_file_name = '-'.join(file_key.split('/')[0:-1]) + '.log'
    s3_event_file_name = file_key.split('/')[-1]

    path = os.path.join(get_etl_run_dir(), run_file_name)

    print 'Checking if the run file exists for path: {}'.format(path)

    file_key_processed = False

    if os.path.isfile(path):
        # If the dated run log file exists check if the key corresponding to S3 event log (.son) exists
        # If exists then the event log is processed else needs to be processed in this run

        with open(path, 'r') as f:

            line = f.readline()

            while line:
                if line == s3_event_file_name:
                    file_key_processed = True

    return file_key_processed

