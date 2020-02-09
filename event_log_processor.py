from datetime import datetime
import json
from utils.s3_utils import get_s3_keys_for_events_log
from utils.s3_utils import get_s3_file
from utils.utils import is_event_log_processed
import traceback

class EventLogProcessor:
    '''
    Class to process application events log and load to Redshift.
    '''

    def _read_events_log(self, file_key):
        file_obj = get_s3_file(file_key)

        row = file_obj._raw_stream.readline()

        while row:
            # Handle bad rows
            try:
                event_json = json.loads(row)
            except Exception as e:
                print 'Error converting event message [{}] to JSON. skipping row'.format(row)

            print event_json['id']
            # event = self._format_event(event_json)
            row = file_obj._raw_stream.readline()


    def _process_events_log(self, s3_keys):
        '''

        :param s3_keys:
        :return:
        '''
        print 'Processing {} events log'.format(len(s3_keys))

        for key in s3_keys:
            print 'Processing events for key: {}'.format(key)

            try:
                is_processed = is_event_log_processed(key)

                if not is_processed:
                    self._read_events_log(key)
            except Exception as e:
                print 'Exception: error occurred processing key: {}.'.format(key)
                traceback.print_exc()

            break

    def process(self):
        '''
        Start processing events log from S3.

        1. Checks if there are new files to be processed in S3 events log
        2. Read file and parse the events message
        3. Update run_dir for new files processed and update last_run timestamp

        :return: list of processed Event messages
        '''
        print 'Starting events processor. start_time: {}'.format(datetime.now())
        start_time = datetime.now()

        try:
            event_log_keys = get_s3_keys_for_events_log()

            self._process_events_log(event_log_keys)
        except Exception as e:
            print 'Error processing events log.'.format(e)
            traceback.print_exc(e)

        end_time = datetime.now()

        print 'Finished processing events. end_time: {}. Time taken: {}'.format(end_time, end_time-start_time)

