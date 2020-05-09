#!/usr/bin/env python

import sys
import csv

STOCKS_INFO_FILE = '/Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/historical_stocks.csv'
START_YEAR = 2008
END_YEAR = 2018


def respect_condition(year, ticker):
    return year in range(START_YEAR, END_YEAR + 1) and ticker in ticker_2_Sector;


ticker_2_Sector = {}

with open(STOCKS_INFO_FILE) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    firstLine = True

    for row in csv_reader:
        if not firstLine:
            ticker, _, _, sector, _ = row
            if sector != 'N/A':
                ticker_2_Sector[ticker] = sector
        else:
            firstLine = False


for line in sys.stdin:
    data = line.strip().split(',')
    if len(data) == 8:
        ticker, _, close, _, _, _, volume, date = data
        if ticker != 'ticker':
            year = int(date[0:4])

            if respect_condition(year, ticker):
                sector = ticker_2_Sector[ticker]
                print('{}\t{}\t{}\t{}\t{}'.format(sector, ticker, date, close, volume))
