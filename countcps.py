#!/usr/bin/env python 

"""Program that takes a CSV file of CDRs and produces a list of one-second intervals 
with call counts, again as a CSV file.  Optionally, the program will display the 
spread of CPS values.

    usage: countcps.py [-h] [-s START] [-e END] [-t {auto,header,positional}]
                    [-c COLUMN] [--spread] [--queue] [--version]
                    [--log {debug,info,warning}]
                    cdr_file cps_file

    Create an ordered calls-per-second CSV file from a CDR file.

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

The program will by default attempt to auto-detect the format of the CDR file.  Twilio
Console, Looker and Monkey download formats are recognized.  Otherwise, it looks for the 
first column that is formatted as an ISO 8061 date.  If the above conditions are not true, 
then you should specify the name (if there is a header row) or number (if no header) of 
the column that contains the date/time the call was made.

Note that the program will automatically filter out non-Outgoing API calls for Console, 
Looker and Monkey CDRs; for other sources, you should make sure that the only calls 
included in the CDR file are outbound calls.

Regarding timezones: the output CPS file contains date/times that are "naive", that is
to say, have no timezone information included.  Console CDRs will generally have the
time set to the timezone of the Console user; Monkey CDRs use Pacific Time.  The output
file will reflect the input file's timezone.
"""


import sys
import argparse
from datetime import datetime, timedelta
import csv
import logging
from decimal import Decimal


__version__ = "1.0"

DEFAULT_FIELDNAMES = \
    ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']     # Used to select a CDR field by position

DATETIME_FORMATS = {
    'Monkey': "%a, %d %b %Y %H:%M:%S %z",                   # e.g. "Sat, 12 Sep 2020 10:30:05 -0700"
    'Console': "%H:%M:%S %Z %Y-%m-%d",                      # e.g. "14:52:06 EDT 2020-09-10"
    'ISO': None                                             # e.g. "2020-09-10 14:52:06.000"
}

logger = logging.getLogger(__name__)


# Set up logging for the module.
def configure_logging(level=logging.INFO):
    logger.setLevel(level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d: %(message)s', datefmt='%H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# Return parsed command line arguments.
def get_args():
    col_num = None

    # Check that column number or name is possible.
    def column_id(str):
        try:
            nonlocal col_num
            col_num = int(str)
            if col_num < 1 or col_num > len(DEFAULT_FIELDNAMES): 
                raise argparse.ArgumentTypeError("Column number is out of range")
            else:
                return str
        except ValueError:
            return str

    parser = argparse.ArgumentParser(
        description="Create an ordered calls-per-second CSV file from a CDR file.",
        epilog=("We recommend defaulting the CDR file type to 'auto', unless the start "
                "date/time is not the first date/time column in the file, in which "
                "case you should specify 'column', which is the name (type='header') "
                "or number (type='positional') of the start date/time column."))
    parser.add_argument('cdr_file', type=argparse.FileType('r'), 
                        help="input CSV file containing call detail records")
    parser.add_argument('cps_file', type=argparse.FileType('w'), 
                        help="output CSV file containing CPS counts")
    parser.add_argument('-s', '--start', type=datetime.fromisoformat, 
                        help="ignore records before this date/time (YYYY-MM-DD [HH:MM:SS])")
    parser.add_argument('-e', '--end', type=datetime.fromisoformat,
                        help="ignore records after this date/time (YYYY-MM-DD [HH:MM:SS])")
    parser.add_argument('-t', '--type', choices=['auto', 'header', 'positional'], default='auto',
                        help=("specify format of CDR file (auto: autodetect; "
                              "header: has a header row; positional: no header row)"))
    parser.add_argument('-c', '--column', type=column_id,
                        help="column name or number containing call start date/time")
    parser.add_argument('--spread', action='store_true', help="display CPS spread")
    parser.add_argument('--queue', action='store_true', help="display queue time estimates from CDRs")
    parser.add_argument('--version', action='version', version=__version__)
    parser.add_argument('--log', choices=['debug', 'info', 'warning'], 
                        default='info', help="set logging level")
    args = parser.parse_args()

    if args.type == 'positional' and not col_num:
        parser.error("Start date/time field specified by position, but no column number specified")

    if args.type == 'header' and not args.column:
        parser.error("Start date/time field specified by column name, but none specified")

    return args


# Take a row of CSV values and find all those that are formatted as a datetime.
# We'll try all the known datetime formats in turn, until we find one that works.  
# Returns a tuple containing a list of the column numbers, indexed from 1,  
# the datetime format, and timezone info.
def look_for_datetime(columns):
    dt_cols = []
    tzinfo = None

    for fmt_name, fmt_string in DATETIME_FORMATS.items():
        logger.debug('Trying %s datetime format', fmt_name)
        i = 1

        for column in columns:
            try:
                if fmt_string:
                    dt = datetime.strptime(column, fmt_string)
                else:
                    dt = datetime.fromisoformat(column)
                dt_cols.append(i)
                tzinfo = dt.tzinfo
            except ValueError:
                pass
            i += 1
        
        if dt_cols: break

    if dt_cols: 
        logger.debug("Columns formatted as date/time values: %s", dt_cols)
        logger.debug("Datetime format is %s", fmt_name)
        logger.debug("Timezone in CDR file is %s", tzinfo)
    else: 
        fmt_name = None
        logger.debug("No datetime items found in row")

    return (dt_cols, fmt_string, tzinfo)


# Look for a candidate header field, choosing the first found in the given list.
def look_for_header(columns, candidates):
    for candidate in candidates:
        if candidate in columns: return candidate
    return None


# Structure containing header row and date/time format information.
class CDRinfo:
    def __init__(self):
        self.has_header = False
        self.start_col_id = None
        self.flags_col_id = None
        self.direction_col_id = None
        self.queuetime_col_id = None
        self.datetime_format = None
        self.tzinfo = None


# Returns a CDRinfo containing details of the name or position of Flags 
# and DateCreated/StartTime columns, and the date/time format.
def detect_cdr_type(args):

    # Let's initially assume the CDR file has a header, and get the field names. 
    cdr_info = CDRinfo()
    reader = csv.DictReader(args.cdr_file)
    fieldnames = reader.fieldnames

    if fieldnames is None:
        sys.exit("Error: CDR file is empty!")

    logger.debug("Header fieldnames: %s", fieldnames)

    # See whether this is a real header by determining whether any of the 
    # field names are actually datetimes.
    dt_cols, cdr_info.datetime_format, cdr_info.tzinfo = look_for_datetime(fieldnames)
    cdr_info.has_header = False if dt_cols else True

    # Next, do a little more validation.
    if args.type == 'positional' and cdr_info.has_header:
        sys.exit("Error: CDR file has header row, but start date/time was specified by position")

    if args.type == 'header' and not cdr_info.has_header:
        sys.exit("Error: CDR file has no header row, but start date/time was specified by column name")

    # If there's a header, get the next row to use as a sample. 
    if cdr_info.has_header:
        try:
            sample_row = next(reader).values()
            logger.debug("Sample row: %s", sample_row)
        except StopIteration:
            sys.exit("Error: CDR file contains no call records!")

        dt_cols, cdr_info.datetime_format, cdr_info.tzinfo = look_for_datetime(sample_row)

        if not dt_cols:
            sys.exit("Error: CDR file contains no recognizable call records!")

    # If the start date/time column is positional, check against the header row.
    if args.type == 'positional':
        if int(args.column) in dt_cols:
            cdr_info.start_col_id = args.column
            logger.info("CDR file confirmed as type 'positional'")
        else:
            sys.exit(f"Column {args.column} does not contain date/time values")

    # If the start date/time column was specified by name, check agsinst the sample row.
    elif args.type == 'header':
        try:
            column_num = fieldnames.index(args.column) + 1    # Remember, indexed from 1
        except ValueError:
            sys.exit(f"No such column name '{args.column}' in header row")

        if column_num in dt_cols:
            cdr_info.start_col_id = args.column
            logger.info("CDR file confirmed as type 'header'")
        else:
            sys.exit(f"Column {args.column} does not contain date/time values")

    # Autodetect: look for Monkey/Looker/Console headers.  If we can't find a recognized
    # start date/time header, we'll pick the first column with a datetime.
    elif args.type == 'auto':

        if cdr_info.has_header:
            # Determine whether any of the standard headers are present.
            cdr_info.flags_col_id = look_for_header(fieldnames, ['Flags', 'flags'])
            cdr_info.direction_col_id = look_for_header(fieldnames, ['Direction', 'direction'])
            cdr_info.queuetime_col_id = look_for_header(fieldnames, ['QueueTime', 'queue_time'])
            cdr_info.start_col_id = look_for_header(
                fieldnames, ['DateCreated', 'date_created', 'StartTime', 'start_time'])

            if cdr_info.flags_col_id: 
                logger.info("CDR file autodetected as likely from Monkey or Looker")
            elif cdr_info.direction_col_id:
                logger.info("CDR file autodetected as likely from Console or getcdrs.py")

            # If there's a defined start date/time header, make sure the column is a datetime.
            if cdr_info.start_col_id:
                col_num = fieldnames.index(cdr_info.start_col_id) + 1  # Indexed from 1
                if col_num not in dt_cols:
                    sys.exit(f"Column {args.column} does not contain date/time values")

            # Otherwise pick the first column with a datetime.
            else:
                cdr_info.start_col_id = fieldnames[dt_cols[0] - 1]
                logger.info("CDR file autodetected as type 'header'")

        else:
            # No headers, so pick the first datetime column.
            cdr_info.start_col_id = str(dt_cols[0])
            logger.info("CDR file autodetected as type 'positional'")

    logger.debug("Start column is '%s'", cdr_info.start_col_id)
    logger.debug("Flags column is '%s'", cdr_info.flags_col_id)
    logger.debug("Direction column is '%s'", cdr_info.direction_col_id)
    args.cdr_file.seek(0)   # Reset reader to beginning of file again.
    return cdr_info


def calculate_spread(intervals):
    logger.debug("Calculating spread...")
    spread = {}
    for value in intervals.values():
        if value in spread.keys():
            spread[value] += 1
        else:
            spread[value] = 1
    return spread


def print_spread(spread):
    print()
    print("Spread")
    print("------")
    for key in sorted(spread.keys()):
        print(f'{key:4d} CPS: x {spread[key]}')
    print()


def print_queue_times(queue_times):
    print()
    if queue_times:
        print("Queue Time Estimates")
        print("--------------------")
        for queue_time in sorted(queue_times.keys()):
            print(f'{queue_time:6.2f} secs: x {queue_times[queue_time]}')
    else:
        print("No queue times were recorded")
    print()


def main(args):
    configure_logging(level=getattr(logging, args.log.upper()))

    # Collect info about the CDR file format, and update the timezone info in the start and end times.
    cdr_info = detect_cdr_type(args)
    if args.start: args.start = args.start.replace(tzinfo=cdr_info.tzinfo)
    if args.end: args.end = args.end.replace(tzinfo=cdr_info.tzinfo)

    logger.debug("Reading CSV file...")
    intervals = {}
    queue_times = {}
    num_read = 0
    num_counted = 0
    num_written = 0

    with args.cdr_file as cdr_file:
        cdrs = csv.DictReader(cdr_file, fieldnames=None if cdr_info.has_header else DEFAULT_FIELDNAMES)
        for cdr in cdrs:
            try:
                num_read += 1

                # Filter all but Outgoing API calls, if the CDRs were exported from Monkey, Looker or 
                # Twilio Console.  If not from these sources, the CDR file should be pre-filtered.
                if cdr_info.flags_col_id and (int(cdr[cdr_info.flags_col_id]) & 0x0002 != 2): 
                    continue
                if cdr_info.direction_col_id and (cdr[cdr_info.direction_col_id] not in ['Outgoing API', 'outbound-api']): 
                    continue

                # Get the call start date/time, according to the format of the source. 
                if cdr_info.datetime_format is None:
                    call_start = datetime.fromisoformat(cdr[cdr_info.start_col_id])
                else:
                    call_start = datetime.strptime(
                        cdr[cdr_info.start_col_id], 
                        cdr_info.datetime_format)

                # If the call was queued, add it to a tally for the queue length, and adjust the start time.
                if cdr_info.queuetime_col_id:
                    queue_time = Decimal(cdr[cdr_info.queuetime_col_id]) / 1000  # Result in seconds

                    if queue_time in queue_times.keys():
                        queue_times[queue_time] += 1
                    else:
                        queue_times[queue_time] = 1

                    if queue_time > 0:
                        call_start -= timedelta(seconds=int(queue_time))
                
                # Filter records outside of the chosen period.
                if args.start and call_start < args.start: continue
                if args.end and call_start >= args.end: continue
                
                # Count the call against its CPS interval.
                num_counted += 1
                if call_start in intervals.keys():
                    intervals[call_start] += 1
                else:
                    intervals[call_start] = 1

            except Exception as err:
                logger.error("Line: %s", cdr)
                sys.exit(f"Problem parsing CDR file: {str(err)}")

    logger.debug("%s records read, %s records counted", num_read, num_counted)
    logger.debug("Writing CPS file...")

    with args.cps_file as cps_file:
        for key, value in intervals.items():
            num_written += 1
            print(f'{key},{value}', file=cps_file)

    logger.debug("%s records written", num_written)

    if args.spread:
        print_spread(calculate_spread(intervals))

    if args.queue:
        print_queue_times(queue_times)


if __name__ == "__main__":
    main(get_args())