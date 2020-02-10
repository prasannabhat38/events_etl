from datetime import datetime
import json
import traceback

from utils.s3_utils import get_s3_keys_for_events_log
from utils.event_parser import parse_event_log
from utils.utils import is_event_log_processed
from utils.utils import update_run_log
from utils.utils import update_run_status

class EventLogProcessor:
    '''
    Class to process application events log and load to Redshift.
    '''
    def _push_events(self, events):
        '''
        Push events to DB (Redshift) or it could be any further downstream processor

        :return:
        '''

        # TODO:

    def _process_events_log(self, s3_keys):
        '''
        Start processing events log.

        :param s3_keys: list of s3_keys from the events_log directory that need to be processed
        :return:
        '''
        print 'Processing {} events log'.format(len(s3_keys))
        run_status = True

        for key in s3_keys:
            print 'Processing events for key: {}'.format(key)
            # Handling exception to continue processing all the keys even if processing breaks for some other files due to invalid msgs
            try:
                is_processed = is_event_log_processed(key)

                if not is_processed:
                    events_json = parse_event_log(key)
                    self._push_events(events_json)
            except Exception as e:
                run_status = False
                print 'Exception: error occurred processing key: {}.'.format(key)
                traceback.print_exc()

            update_run_log(key)

            break

        return 'SUCCESS' if run_status else 'FAILED'

    def process(self):
        '''
        Start processing events log from S3.

        1. Read keys from S3 bucket for event logs
        2. Check if there are new files to be processed
        3. Read file (by key) and parse the events message
        4. Update run_dir for new files processed and
        5. Update last_run timestamp

        :return: outputs processed Event messages
        '''
        print 'Starting events processor. start_time: {}'.format(datetime.now())
        start_time = datetime.now()

        try:
            event_log_keys = get_s3_keys_for_events_log(path_prefix='/')
            run_status = self._process_events_log(event_log_keys)
            update_run_status(run_status)
        except Exception as e:
            print 'Error processing events log.'.format(e)
            traceback.print_exc(e)
            update_run_status('FAILED')

        end_time = datetime.now()

        print 'Finished processing events. end_time: {}. Time taken: {}'.format(end_time, end_time-start_time)
