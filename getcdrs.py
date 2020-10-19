#!/usr/bin/env python 

"""Get the CDRs from a Twilio account for a specified time period.
"""


import os
import sys
import argparse
import logging
import csv
from datetime import datetime
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException


__version__ = "1.0"

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
        '-k', '--key', default=os.environ.get('TWILIO_API_KEY'),
        help="API key (default: TWILIO_API_KEY env var)")
    parser.add_argument(
        '-p', '--pw', 
        default=os.environ.get('TWILIO_API_SECRET') or os.environ.get('TWILIO_AUTH_TOKEN'),
        help="API secret or auth token (default: TWILIO_API_SECRET or TWILIO_AUTH_TOKEN env var)")
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
    if args.key:
        if not args.pw: parser.error("No API secret found")
    else:
        if not args.pw: parser.error("No auth token found")

    return args


def main(args):
    configure_logging(level=getattr(logging, args.log.upper()))
    client = Client(args.key, args.pw, args.account) if args.key else Client(args.account, args.pw)
    logger.info("Getting CDRs for account %s between %s and %s", args.account, args.start, args.end)

    try:
        calls = client.calls.list(start_time_after=args.start, start_time_before=args.end)
    except TwilioRestException:
        sys.exit("Error: cannot access CDRs; please check account credentials")

    logger.debug("Writing CDRs...")

    with args.cdr_file as cdr_file:
        writer = csv.writer(cdr_file, args.fields)
        writer.writerow(args.fields)
        
        # Special case because 'from' is a reserved word in Python; must use 'from_' instead.
        pythonic_fields = ['from_' if field == 'from' else field for field in args.fields]

        for call in calls:
            cdr = [getattr(call, field) for field in pythonic_fields]
            writer.writerow(cdr)

    logger.debug("Finished writing CDRs")


if __name__ == "__main__":
    main(get_args())