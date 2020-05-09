#!/usr/bin/env python
import sys

START_YEAR = 2008
END_YEAR = 2018


def respect_condition(year):
    return year in range(START_YEAR, END_YEAR + 1)


for line in sys.stdin:
    data = line.strip().split(',')

    if len(data) == 8:
        ticker, _, close, _, _, _, volume, date = data

        if ticker != 'ticker':
            year = int(date[0:4])

            if respect_condition(year):
                print('{}\t{}\t{}\t{}'.format(ticker, date, close, volume))
