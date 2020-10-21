#!/usr/bin/env python 

"""Get the CDRs from a Twilio account for a specified time period.

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

"""


import os
import sys
import argparse
import logging
import csv
from datetime import datetime
from twilio.rest import Client
from twilio.base.exceptions import TwilioException


__version__ = "1.1"

CDR_FIELDS = [
    "sid",
    "date_created",
    "date_updated",
    "parent_call_sid",
    "account_sid",
    "to",
    "to_formatted",
    "from",
    "from_formatted",
    "phone_number_sid",
    "status",
    "start_time",
    "end_time",
    "duration",
    "price",
    "price_unit",
    "direction",
    "answered_by",
    "api_version",
    "annotation",
    "forwarded_from",
    "group_sid",
    "caller_name",
    "queue_time",
    "trunk_sid",
]

logger = logging.getLogger(__name__)


# Set up logging for the module.
def configure_logging(level=logging.INFO):
    logger.setLevel(level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d: %(message)s', datefmt='%H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# Return parsed and validated command line arguments.
def get_args():

    # Convert CSV string into list of fields.
    def field_list(str):
        fields = str.lower().split(',')
        fields = [f.strip() for f in fields]    # Remove whitespace
        fields = [f for f in fields if f]       # Remove empty elements

        if not fields: raise argparse.ArgumentTypeError("argument is empty")

        for field in fields:
            if field not in CDR_FIELDS:
                raise argparse.ArgumentTypeError(f"{field} is not a recognized CDR field")

        return fields     

    # Parse a timezone offset and return a tzinfo object.
    def tzinfo(str):
        try:
            dt = datetime.strptime(str, '%z')
            return dt.tzinfo
        except ValueError:
            raise argparse.ArgumentTypeError(
                "Timezone offset should be a signed value in the form ±HHMM")

    # Calculate start, end and timezone defaults.
    now = datetime.now()
    this_year = now.year
    this_month = now.month
    last_month = 12 if this_month == 1 else this_month - 1
    year_of_last_month = this_year - 1 if last_month == 12 else this_year
    first_of_this_month = datetime(day=1, month=this_month, year=this_year)
    first_of_last_month = datetime(day=1, month=last_month, year=year_of_last_month)
    local_timezone = now.astimezone().tzinfo

    parser = argparse.ArgumentParser(
        description="Get the CDRs from a Twilio account for a specified time period.",
        epilog=("Add a filename to the command line prefixed by '@' if you wish to place "
                "parameters in a file, one parameter per line."),
        fromfile_prefix_chars='@')
    parser.add_argument(
        'cdr_file', type=argparse.FileType('w'), 
        help="output CSV file")
    parser.add_argument(
        '-s', '--start', type=datetime.fromisoformat, default=first_of_last_month,
        help="start at this date/time (YYYY-MM-DD [HH:MM:SS]; default: start of last month)")
    parser.add_argument(
        '-e', '--end', type=datetime.fromisoformat, default=first_of_this_month,
        help="end before this date/time (YYYY-MM-DD [HH:MM:SS]; default: start of this month)")
    parser.add_argument(
        '--tz', default=local_timezone, type=tzinfo,
        help="timezone as ±HHMM offset from UTC (default: timezone of local machine)")
    parser.add_argument(
        '-a', '--account', default=os.environ.get('TWILIO_ACCOUNT_SID'),
        help="account SID (default: TWILIO_ACCOUNT_SID env var)")
    parser.add_argument(
        '-p', '--pw', 
        default=os.environ.get('TWILIO_AUTH_TOKEN'),
        help="auth token (default: TWILIO_AUTH_TOKEN env var)")
    parser.add_argument(
        '--subs', action='store_true', 
        help="include subaccounts")
    parser.add_argument(
        '--fields', default=CDR_FIELDS, type=field_list,
        help="comma-separated list of desired fields (default: all)")
    parser.add_argument('--version', action='version', version=__version__)
    parser.add_argument('--log', choices=['debug', 'info', 'warning'], default='info', 
                        help="set logging level")
    args = parser.parse_args()

    # Apply timezone offset to start and end date/times.
    args.start = args.start.replace(tzinfo=args.tz)
    args.end = args.end.replace(tzinfo=args.tz)

    # Validate arguments.
    if args.start >= args.end: parser.error("Start date is after end date")
    if not args.account: parser.error("No account SID found")
    if not args.pw: parser.error("No auth token found")

    return args


# Generator function that gets calls over the specified period for 
# the specified account, and optionally for its subaccounts.  
def calls(args):
    client = Client(args.account, args.pw)

    try:
        if args.subs:
            accounts = client.api.accounts.list()
        else:
            accounts = [client.api.accounts(args.account).fetch()]

        for account in accounts:      
            logger.info("Getting CDRs for account %s (%s)", account.sid, account.friendly_name)
            client = Client(args.account, args.pw, account.sid)
            calls = client.calls.list(start_time_after=args.start, start_time_before=args.end)
            for call in calls:
                yield call

    except TwilioException as ex:
        sys.exit(f"Unable to get CDRS: check credentials. Full message:\n{ex}")                    


def main(args):
    configure_logging(level=getattr(logging, args.log.upper()))
    logger.info("Getting CDRs for the period %s to %s", args.start, args.end)
    logger.debug("Writing CDRs...")

    with args.cdr_file as cdr_file:
        writer = csv.writer(cdr_file, args.fields)
        writer.writerow(args.fields)
        
        # Special case because 'from' is a reserved word in Python; must use 'from_' instead.
        pythonic_fields = ['from_' if field == 'from' else field for field in args.fields]

        for call in calls(args):
            cdr = [getattr(call, field) for field in pythonic_fields]
            writer.writerow(cdr)

    logger.debug("Finished writing CDRs")


if __name__ == "__main__":
    main(get_args())