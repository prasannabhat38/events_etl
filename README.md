# Events ETL

# Project brief
One of our web apps sends various events related to user behaviour (how and when they interact with the app) for example: visit to a page or use of a given feature. These events are periodically uploaded to an S3 bucket and stored there for 28 days before they’re deleted. We want to be able to query these events, so we can analyse product usage and answer various business questions.

For this challenge, we want you to create a script to prep and clean this clean event data which we could later incorporate into an ETL pipeline where we’ll load this data into a table in our Redshift cluster. You are free to choose the output format of the cleaned data but please explain your decision.

Where are the events located? 
The S3 location s3://dataeng-challenge/8uht6u8bh/events/ contains a number of event log files. The files are in JSON lines format and look something like this:

{"id": "9ba671d6-5154-41c1-93b1-41dd647b2d55", "created_at": "2020-01-13 05:10:12", "user_email": "swalker@krause.com", "ip": "204.116.116.31", "event_name": "message_saved", "metadata": {"message_id": 99979}}
{"id": "242279cc-4b5d-4a43-a3ec-aa52a8e87e63", "created_at": "2020-01-13 02:31:30", {"id": "10cd6e40-cab2-432d-9e62-a8466761e307", "created_at": "2020-01-12 13:28:31", "user_email": "bryantjulie@navarro.net", "ip": "187.87.20.198", "event_name": "meeting_scheduled", "metadata": {"meeting_id": "eafb8c01-67ed-44aa-94fb-738330fbff95", "meeting_time": "2020-01-20 16:44:26"}}

The events should contain the following keys: id, created_at, user_email, ip, event_name and metadata. The metadata is specific to each event, and may contain anything.

