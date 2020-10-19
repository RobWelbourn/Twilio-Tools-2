#!/usr/bin/env python 

"""Program that calculates the maximum daily call delay at a given calls-per-second rate,
and displays a graph of the delay over the chosen time period.

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

The program may be used interactively, in which case it will prompt for the CPS value and
display the graph in response.  To try a different value, you will first have to close
the graph window.

The input file is produced by the companion program countcps.py, which counts the
number of calls in each second of the period, and outputs a CSV with the date/time and
count on each line.  The dates are formatted as ISO 8061 dates in the form 
YYYY-MM-DD HH:MM:SS, without a timezone offset, separated from the count by a comma.
"""


import sys
import argparse
from datetime import datetime, timedelta
import logging
import csv
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, DayLocator, HourLocator


__version__ = "1.0"

ONE_SECOND = timedelta(seconds=1)
ONE_DAY = timedelta(days=1)
ONE_WEEK = timedelta(days=7)

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
    parser = argparse.ArgumentParser(
        description="Analyze a CSV file of CPS counts to determine maximum call queuing time.",
        epilog=("The program may be used interactively, in which case it will prompt for "
                "the CPS value and display the graph in response.  To try a different value, "
                "first close the graph window."))
    parser.add_argument('cps_file', type=argparse.FileType('r'), 
                        help="CSV file containing CPS counts")
    parser.add_argument('-s', '--start', type=datetime.fromisoformat, 
                        help="ignore records before this date/time (YYYY-MM-DD [HH:MM:SS]")
    parser.add_argument('-e', '--end', type=datetime.fromisoformat, 
                        help="ignore records after this date/time (YYYY-MM-DD [HH:MM:SS])")
    parser.add_argument('--cps', type=int, help="CPS value (default: interactive)")
    parser.add_argument('--version', action='version', version=__version__)
    parser.add_argument('--log', choices=['debug', 'info', 'warning'], default='info', 
                        help="set logging level")
    return parser.parse_args()


# Return the number of seconds in a timedelta.
def num_seconds(delta):
    return delta.days * 86400 + delta.seconds


# At the start of the period, the queue size is assumed to be zero.  For each second,
# we add the number of calls offered and subtract the CPS value (except that the queue
# size can never be below zero), and divide by the CPS value to get queue time in seconds.
def calculate_queue_time(cps_array, queue_time, cps):
    logger.debug("Calculating queue times...")
    queue_size = 0
    for i in range(len(cps_array)):
        queue_size = max(0, queue_size + cps_array[i] - cps)
        queue_time[i] = queue_size / cps


# Return a list of (datetime, float secs) tuples for the time period.  We split the
# period into 24-hour chunks, starting at the beginning value.
def get_daily_maxima(start, queue_time):
    logger.debug("Finding daily maxima...")
    maxima = []
    start_index = 0
    while start_index < len(queue_time):
        end_index = min(start_index + 86400, len(queue_time))
        daily_max_index = start_index + int(np.argmax(queue_time[start_index:end_index]))
        daily_max = start + timedelta(seconds=daily_max_index)
        maxima.append((daily_max, queue_time[daily_max_index]))
        start_index = end_index
    return maxima


def print_maxima(maxima, cps):
    print("-----------------------------------------")
    print(f"Daily maximum call queue times at {cps} CPS:")
    print("-----------------------------------------")
    for daily_maximum in maxima:
        dt, qt = daily_maximum
        print(f"{dt.strftime('%a %Y-%m-%d')}: {qt:6.1f} seconds at {dt.time()}")
    print('')


# Matplotlib magic happens here.
def plot_results(dt_array, queue_time, cps, start, end_dt):
    start_day = start.date()
    end_day = end_dt.date()
    period = end_day - start_day

    # Set the title and the X axis tick format according to the length of the period.
    if period <= ONE_DAY:
        period_str= start_day.strftime('%d %b %Y')
        format_str = '%H'
        locator = HourLocator()
    elif period <= ONE_WEEK:
        period_str = start_day.strftime('%d %b %Y') + ' to ' + end_day.strftime('%d %b %Y')
        format_str = '%a %d %b'
        locator = DayLocator()
    else:
        period_str = start_day.strftime('%d %b %Y') + ' to ' + end_day.strftime('%d %b %Y')
        format_str = '%a'
        locator = DayLocator()        

    fig, ax = plt.subplots(figsize=(14,8))
    ax.plot(dt_array, queue_time)
    ax.set_title(f"Call Queue Size at {cps} CPS, {period_str}")
    ax.set_ylabel('Queue Size (seconds)')
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(DateFormatter(format_str))
    plt.show()


def main(args):
    configure_logging(level=getattr(logging, args.log.upper()))
    logger.debug("Reading CPS file into memory...")

    num_read = 0
    intervals = []  # Each entry is a (datetime, int) tuple
    earliest = datetime.max 
    latest = datetime.min 

    with args.cps_file as cps_file:
        cps_lines = csv.DictReader(cps_file, fieldnames=['dt', 'cps'])
        for cps_line in cps_lines:
            num_read += 1
            dt = datetime.fromisoformat(cps_line['dt'])

            # Filter records outside of the chosen period.
            if args.start and dt < args.start: continue
            if args.end and dt >= args.end: continue            

            # We can't guarantee that the records are in order, so note the earliest and latest.
            if dt < earliest: earliest = dt
            if dt > latest: latest = dt

            cps = int(cps_line['cps'])
            intervals.append((dt, cps))

    if not intervals:
        sys.exit("No records found in the specified time period")

    logger.debug(
        "%s entries read, %s kept, earliest: %s, latest: %s", 
        num_read, len(intervals), earliest, latest)

    # Adjust start and end date/times if they were not explicitly set.
    start = args.start if args.start else earliest
    end = args.end if args.end else latest + ONE_SECOND

    # Calculate how long our CPS and queue_time arrays need to be.
    # Each entry represents a one second duration.
    num_entries = num_seconds(end - start)
    logger.debug("CPS array contains %s entries", num_entries)

    # Create the arrays and load the CPS array from the intervals read.
    dt_array = np.arange(start, end, dtype='datetime64[s]')
    cps_array = np.zeros(num_entries, dtype=np.int32)
    queue_time = np.zeros(num_entries, dtype=np.single)

    for interval in intervals:
        index = num_seconds(interval[0] - start)    # interval[0] contains a datetime
        cps_array[index] = interval[1]              # interval[1] is the corresponding count

    # If a CPS was specified, calculate the daily maxima.
    if args.cps:
        calculate_queue_time(cps_array, queue_time, args.cps)
        maxima = get_daily_maxima(start, queue_time)
        print_maxima(maxima, args.cps)
        plot_results(dt_array, queue_time, args.cps, start, end)

    # Otherwise prompt for CPS interactively.
    else:
        while True:
            response = input("Enter CPS value, or Q to quit: ").strip()
            if not response: continue
            if response[0].upper() == 'Q': break

            try:
                cps = int(response)
                calculate_queue_time(cps_array, queue_time, cps)
                maxima = get_daily_maxima(start, queue_time)
                print_maxima(maxima, cps)
                plot_results(dt_array, queue_time, cps, start, end)
            except ValueError:
                continue


if __name__ == "__main__":
    main(get_args())