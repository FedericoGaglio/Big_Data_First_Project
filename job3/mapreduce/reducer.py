#!/usr/bin/env python
import sys
import json

TICKER = 0
DATE = 1
CLOSE = 2
NAME = 3

START_YEAR = 2016
END_YEAR = 2018


class Company:
    def __init__(self, name):
        self.name = name
        self.change_year = {}

result = []

current_ticker = None
current_year = None
current_company = None
first_close_ticker_year = 0
last_close_ticker_year = 0


def parse_values(value_list):
    ticker = value_list[TICKER].strip()
    date = value_list[DATE].strip()
    close = float(value_list[CLOSE].strip())
    name = value_list[NAME].strip()
    return (ticker, date, close, name)


def initialize_variables(ticker_param, year_param, close_param):
    global first_close_ticker_year
    global last_close_ticker_year
    global current_year
    global current_ticker

    first_close_ticker_year = close_param
    last_close_ticker_year = close_param
    current_year = year_param
    current_ticker = ticker_param


def add_trend_to_company():
    current_company.change_year[current_year] = ((last_close_ticker_year - first_close_ticker_year) / first_close_ticker_year) * 100

# main script
for line in sys.stdin:
    value_list = line.strip().split('\t')

    if len(value_list) == 4:
        ticker, date, close, name = parse_values(value_list)
        year = int(date[0:4])

        if not current_ticker:
            initialize_variables(ticker, year, close)
            current_company = Company(name)
        else:
            if current_ticker == ticker and current_year == year:
                last_close_ticker_year = close
            else:
                add_trend_to_company()

                if current_ticker != ticker:
                    result.append(current_company)
                    current_company = Company(name)

                initialize_variables(ticker, year, close)

# print last computed key
if current_ticker:
    add_trend_to_company()
    result.append(current_company)

for company in result:
    try:
        print(str(company.change_year[2016]) + "\t" + str(company.change_year[2017]) + "\t" + str(company.change_year[2018]) + "\t" + str(company.name))
    except:
        pass

