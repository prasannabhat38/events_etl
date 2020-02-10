from event_log_processor import EventLogProcessor

class TestEventLogProcessor():
    '''
    Test EventLogProcessor ETL process
    '''

    def __init__(self):
        self.event_log_processor = EventLogProcessor()

    def run(self):
        self.event_log_processor.process(clear_prev_run=True, output_to_file=True)

if __name__ == '__main__':
    test = TestEventLogProcessor()

    test.run()
