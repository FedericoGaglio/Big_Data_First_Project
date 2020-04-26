#!/usr/bin/env python

import sys

START_YEAR = 2008
END_YEAR = 2018


def respect_condition(year):
    return year in range(START_YEAR, END_YEAR+1)


for line in sys.stdin:
    # turn each row into a list of strings
    data = line.strip().split(',')

    # if there aren't all data, skip
    if len(data) == 8:
        ticker, _, close, _, low, high, volume, date = data

        # ignore file's first row
        if ticker != 'ticker':
            year = int(date[0:4])
            # check if year is in range (START_YEAR, END_YEAR)
            if respect_condition(year):
                print('{},{},{},{},{},{}'.format(ticker, date, close, low, high, volume))
