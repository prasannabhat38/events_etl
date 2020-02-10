# Events ETL

# Problem Statement
One of our web apps sends various events related to user behaviour (how and when they interact with the app) for example: visit to a page or use of a given feature. These events are periodically uploaded to an S3 bucket and stored there for 28 days before they’re deleted. We want to be able to query these events, so we can analyse product usage and answer various business questions.

For this challenge, we want you to create a script to prep and clean this clean event data which we could later incorporate into an ETL pipeline where we’ll load this data into a table in our Redshift cluster. You are free to choose the output format of the cleaned data but please explain your decision.

Where are the events located? 
The S3 location s3://dataeng-challenge/8uht6u8bh/events/ contains a number of event log files. The files are in JSON lines format and look something like this:

{"id": "9ba671d6-5154-41c1-93b1-41dd647b2d55", "created_at": "2020-01-13 05:10:12", "user_email": "swalker@krause.com", "ip": "204.116.116.31", "event_name": "message_saved", "metadata": {"message_id": 99979}}
{"id": "242279cc-4b5d-4a43-a3ec-aa52a8e87e63", "created_at": "2020-01-13 02:31:30", {"id": "10cd6e40-cab2-432d-9e62-a8466761e307", "created_at": "2020-01-12 13:28:31", "user_email": "bryantjulie@navarro.net", "ip": "187.87.20.198", "event_name": "meeting_scheduled", "metadata": {"meeting_id": "eafb8c01-67ed-44aa-94fb-738330fbff95", "meeting_time": "2020-01-20 16:44:26"}}

The events should contain the following keys: id, created_at, user_email, ip, event_name and metadata. The metadata is specific to each event, and may contain anything.

# Solution:
The EventLogProcessor is the main class to process the ETL. The events logs are periodically uploaded to S3 and stored in a directory that follows a date format. The processor will fetch all the keys from the S3 bucket and events dir and pre processes it to see which are the new files to be processed. For this, it creates a 'run' directory containing a 'run_log' (e.g. run_log-2020-01-01.log) file and having all the S3_keys/files processed corresponding to the dated directory.

Below are the processing steps:
1. Read keys from S3 bucket for event logs
2. Check if there are new files to be processed
3. Read file (by key) and parse the events message
4. Update run_dir for new files processed
5. Push events downstream (to DB or message queue) (TBD)
6. Update last_run timestamp and status

Other considerations:
The parser will parse the events raw json and look for specific keys (below) that will be useful for loading to Redshift or any downstream processing. 

event: {
  *event_name*: <name of the Event>,
  ip: <source_ip_address> (This might be useful to answer queries around which regions the app is getting used etc, but is not mandatory),
  created_at: <source event timestamp>,
  user_email: <user_email> from source,
  metadata: <json containing any additional metadata for that event from source>. This is optional but you can configure it to look for specific keys for a given event type (for e.g. path for a page_visit event, action for a update_admin event)
}

Sample output from the processed S3 event log:

{
  "file_key": "/Users/prasannak/tmp/events/2020/01/01/0f6e0555e9f34063b711a676d13e38f9.json",

  "events": [
    {
      "event_name": "message_saved",
      "ip": "209.154.163.207",
      "created_at": "2020-01-30 00:10:35",
      "user_email": "wallsdenise@thompson.com"
    },
    {
      "event_name": "meeting_invite",
      "ip": "205.166.191.182",
      "created_at": "2020-01-29 20:51:59",
      "user_email": "jessica59@estrada.info"
    },
    {
      "event_name": "page_visit",
      "ip": "215.71.69.213",
      "created_at": "2020-01-29 20:38:07",
      "user_email": "lkidd@ewing-garcia.com",
      "metadata": {
        "path": "main/categories/category"
      }
    },
    {
      "event_name": "change_operator_identity",
      "ip": "217.41.25.212",
      "created_at": "2020-01-30 12:02:56",
      "user_email": "millerjesse@webster-fuentes.org",
      "metadata": {
        "action": "update"
      }
    },
  ],
}

# Set up / Testing
Update AWS access_key_id and secret_access_key in events_etl/aws_config

Run events_etl/tests.py

# Further enhancements/ optimisations

Performance improvements:
1. Asynchronous processing: Fetch the s3_keys and send each key to a Queue (SQS, ActiveMQ) for asynchronous processing
2. Download the file to local disk and process. Makes processing faster by cutting down on network latency while reading file directly from S3.
3. Use multithreading for parallelisation but will still consume a lot of local machine memory
