#!/usr/bin/env python
import sys
import json

# field position of each row taken from stdin
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


# main script
for line in sys.stdin:
    value_list = line.strip().split(',')

    if len(value_list) == 4:
        ticker, date, close, name = parse_values(value_list)
        year = int(date[0:4])

        if not current_ticker:
            current_ticker = ticker
            current_year = year
            first_close_ticker_year = close
            last_close_ticker_year = close
            current_company = Company(name)
        else:
            if current_ticker == ticker:
                if current_year == year:
                    last_close_ticker_year = close
                else:
                    current_company.change_year[current_year] = ((last_close_ticker_year - first_close_ticker_year) / first_close_ticker_year) * 100
                    current_year = year
                    first_close_ticker_year = close
                    last_close_ticker_year = close
            else:
                current_company.change_year[current_year] = ((last_close_ticker_year - first_close_ticker_year) / first_close_ticker_year) * 100
                result.append(current_company)
                current_company = Company(name)
                current_year = year
                current_ticker = ticker
                first_close_ticker_year = close
                last_close_ticker_year = close


# print last computed key
if current_ticker:
    current_company.change_year[current_year] = ((last_close_ticker_year - first_close_ticker_year) / first_close_ticker_year) * 100
    result.append(current_company)

for company in result:
    print(str(company.change_year[2016]) + "," + str(company.change_year[2017]) + "," + str(company.change_year[2018]) + "," + str(company.name))


