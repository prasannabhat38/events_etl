import boto3
import json
import os

AWS_CONFIG_PATH = '../events_etl/config/aws_config.json'
S3_EVENTS_CONFIG_PATH = '../events_etl/config/s3_events_config.json'

def _get_aws_config():
    file_path = os.path.join(os.getcwd(), AWS_CONFIG_PATH)

    with open(file_path, 'r') as f:
        aws_config = json.load(f)

    return aws_config

def _get_s3_events_config(key=None):
    file_path = os.path.join(os.getcwd(), S3_EVENTS_CONFIG_PATH)

    with open(file_path, 'r') as f:
        config = json.load(f)

    if key:
        return config[key]

    return config

def _get_s3_connection():
    print 'Connecting to S3 bucket'
    aws_config = _get_aws_config()

    s3_conn = boto3.client('s3',
                            aws_access_key_id = aws_config['access_key_id'],
                            aws_secret_access_key = aws_config['secret_access_key'])
    return s3_conn

def _get_s3_objects(path_prefix):
    '''
    :param path_prefix: prefix for directory from which the keys to be listed by default returns from base path
    :return: list of all keys corresponding to the event logs in s3 bucket.
    '''
    s3_events_config = _get_s3_events_config()
    bucket = s3_events_config['bucket']
    prefix = s3_events_config['base_path']

    prefix += path_prefix if path_prefix and path_prefix != '/' else ''

    print 'Getting S3 objects from bucket: {} and prefix: {}'.format(bucket, prefix)

    s3 = _get_s3_connection()
    s3_objects = s3.list_objects_v2(Bucket=bucket,
                                    Prefix=prefix)

    return s3_objects

def get_s3_keys_for_events_log(path_prefix=None, suffix='json'):
    '''
    :param path_prefix: prefix of the directory to load events logs. if None uses the base path for events (for this exercise)
    :param suffix: file type

    :return: list of keys corresponding to event log files in S3
    '''
    print 'Fetching S3 keys from events directory'
    keys = []

    s3_objs = _get_s3_objects(path_prefix)

    s3_base_path = _get_s3_events_config(key='base_path')

    for obj in s3_objs['Contents']:
        if obj['Key'].endswith(suffix):
            key = obj['Key'][len(s3_base_path):]
            keys.append(key)

    return keys

def get_s3_file(s3_key):
    '''
    :param s3_key: key corresponding to the s3 file. (The base path of the event dir is prefixed)
    :return: fileStream
    '''
    s3_events_config = _get_s3_events_config()
    s3_conn = _get_s3_connection()

    s3_file_key = s3_events_config['base_path'] + s3_key
    file_obj = s3_conn.get_object(Bucket=s3_events_config['bucket'], Key=s3_file_key)

    if not file_obj:
        raise Exception('Did not find {key} in S3'.format(s3_file_key))

    return file_obj['Body']
