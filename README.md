# Twilio-CPS
_Model Calls Per Second needed for Twilio Programmable Voice_

This repository contains a suite of Python scripts that are designed to extract call volume information out of your call detail records, and answer the question, 'How many calls per second do I need?'  With that information, you can be confident that you are purchasing the right level of CPS for your Twilio account.

You can think of these scripts as a pipeline:

- `getcdrs.py` extracts CDRs from your Twilio account for a given period;
- `countcps.py` takes the CDRs and and produces counts of calls per second; and 
- `analyzecps.py` takes the CPS counts and models call queuing at a given CPS.

## Installation
If you're using a [Virtual Environment](https://realpython.com/python-virtual-environments-a-primer/), do the following in your project directory:
```
python3 -m venv ENV
source ENV/bin/activate
```
Next, install the required Python libraries (`twilio`, `numpy` and `matplotlib`):
```
pip install -r requirements.txt
```

## getcdrs.py
This script can be used to download the call records for an account, or a master account and its subaccounts, for a given period.  You can opt to get all the fields of a call, or you can be selective as to which are included in the output CSV file.

```
usage: getcdrs.py [-h] [-s START] [-e END] [--tz TZ] [-a ACCOUNT] [-p PW]
                  [--subs] [--fields FIELDS] [--version]
                  [--log {debug,info,warning}]
                  cdr_file

positional arguments:
cdr_file                    output CSV file

optional arguments:
-h, --help                  show this help message and exit
-s START, --start START     start at this date/time (YYYY-MM-DD [HH:MM:SS];
                            default: start of last month)
-e END, --end END           end before this date/time (YYYY-MM-DD [HH:MM:SS];
                            default: start of this month)
--tz TZ                     timezone as ±HHMM offset from UTC (default: timezone
                            of local machine)
-a ACCOUNT, --account ACCOUNT
                            account SID (default: TWILIO_ACCOUNT_SID env var)
-p PW, --pw PW              auth token (default: TWILIO_AUTH_TOKEN env var)
--subs                      include subaccounts
--fields FIELDS             comma-separated list of desired fields (default: all)
--version                   show program's version number and exit
--log {debug,info,warning}  set logging level
```
Add a filename to the command line prefixed by '@' if you wish to place parameters in a file, one parameter per line.  A parameter file to get a minimal set of fields necessary to do CPS analysis would contain the following line:

```
--fields=sid,date_created,direction,queue_time
```

## countcps.py
This script takes a CDR file and produces another CSV file, containing call counts for one-second slices of the time period. The script can ingest CDR files from various sources: the above `getcdrs.py` script; downloads from the Twilio console or the internal Monkey portal; Looker SQL queries on the Twilio data warehouse; or CDRs produced by external systems for customers looking to move workloads onto Twilio.

```
usage: countcps.py [-h] [-s START] [-e END] [-t {auto,header,positional}]
                [-c COLUMN] [--spread] [--queue] [--version]
                [--log {debug,info,warning}]
                cdr_file cps_file

Create a calls-per-second CSV file from a CDR file.

positional arguments:
cdr_file                    input CSV file containing call detail records
cps_file                    output CSV file containing CPS counts

optional arguments:
-h, --help                  show this help message and exit
-s START, --start START     ignore records before this date/time (YYYY-MM-DD [HH:MM:SS])
-e END, --end END           ignore records after this date/time (YYYY-MM-DD [HH:MM:SS])
-t {auto,header,positional}, --type {auto,header,positional}
                            specify format of CDR file (auto: autodetect; header:
                            has a header row; positional: no header row)
-c COLUMN, --column COLUMN  column name or number containing call start date/time
--spread                    display CPS spread
--queue                     display queue time estimates from CDRs
--version                   show program's version number and exit
--log {debug,info,warning}  set logging level
```

The program will by default attempt to auto-detect the format of the CDR file.  Twilio
Console, Looker and Monkey download formats are recognized.  Otherwise, it looks for the 
first column that is formatted as an ISO 8601 date.  If the above conditions are not true, 
then you should specify the name (if there is a header row) or number (if no header) of 
the column that contains the date/time the call was made.

Note that the program will automatically filter out non-Outgoing API calls for Console, 
Looker and Monkey CDRs; for other sources, you should make sure that the only calls 
included in the CDR file are outbound calls.

Regarding timezones: the output CPS file contains date/times that are "naive", that is
to say, have no timezone information included.  Console CDRs will generally have the
time set to the timezone of the Console user; Monkey CDRs use Pacific Time; Looker files use UTC.  
The output file will reflect the input file's timezone.

For best results, the actual times that calls were submitted to the REST API should be used. The `date_created` field tells us when the outbound call was dialed; this should ideally be modified by the `queue_time`, the number of milliseconds that the call was held in the outbound call queue.  The following Looker SQL query on Twilio's data warehouse will produce the equivalent information to the `getcdrs.py` script:

```
select call_sid, date_created, flags, queue_time from redacted_calls_kafka
where account_sid = 'ACxxxx'
and date_created >= date '2020-09-01' and date_created < date '2020-10-01'
```
The program will optionally show you the distribution of CPS and queue times that were found in the CDRs:

```
Spread
------
   1 CPS: x 10973
   2 CPS: x 3556
   3 CPS: x 1410
   4 CPS: x 721
  ...
  23 CPS: x 1
  25 CPS: x 1
  47 CPS: x 1
  63 CPS: x 1

Queue Time Estimates
--------------------
  0.00 secs: x 32367
  0.12 secs: x 1085
  0.24 secs: x 245
  0.36 secs: x 67
  ...
  3.24 secs: x 3
  3.36 secs: x 1
  3.48 secs: x 1
  3.60 secs: x 2
```

## analyzecps.py
This script takes the CPS counts and performs _what if?_ calculations on what the call queues would be like if the Twilio account CPS limit was set to a given level.  There is no single right answer to what the CPS value should be; an automated outbound notification service for, say, school closures could tolerate a much higher delay than a contact center making outbound calls.  

```
usage: analyzecps.py [-h] [-s START] [-e END] [--cps CPS] [--version]
                    [--log {debug,info,warning}]
                    cps_file

Analyze a CSV file of CPS counts to determine maximum call queuing time.

positional arguments:
cps_file                    CSV file containing CPS counts

optional arguments:
-h, --help                  show this help message and exit
-s START, --start START     ignore records before this date/time (YYYY-MM-DD [HH:MM:SS]
-e END, --end END           ignore records after this date/time (YYYY-MM-DD [HH:MM:SS])
--cps CPS                   CPS value (default: interactive)
--version                   show program's version number and exit
--log {debug,info,warning}  set logging level
```
The program may be used interactively, in which case it will prompt for the CPS value and
display the graph in response.  To try a different value, you will first have to close
the graph window.

The program will calculate the daily maximum queue lengths at a given CPS value, and produce a graph of values over the specified time period:

```
-----------------------------------------
Daily maximum call queue times at 10 CPS:
-----------------------------------------
Fri 2020-10-09:    9.2 seconds at 08:00:05
```
![analyzecps screenshot 1](https://github.com/RobWelbourn/Twilio-CPS/blob/master/images/analyzecps1.png)
