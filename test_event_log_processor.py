from event_log_processor import EventLogProcessor

class TestEventLogProcessor():
    '''

    '''

    def __init__(self):
        self.event_log_processor = EventLogProcessor()

    def run(self):
        self.event_log_processor.process()


if __name__ == '__main__':
    test = TestEventLogProcessor()

    test.run()
