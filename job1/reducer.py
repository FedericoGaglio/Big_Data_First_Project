#!/usr/bin/env python
import sys

# constant for fields position
TICKER = 0
DATE = 1
CLOSE = 2
LOW = 3
HIGH = 4
VOLUME = 5

START_YEAR = 2008
END_YEAR = 2018


class AvgVolume:

    def __init__(self):
        self.volume_sum = 0
        self.volume_count = 0

    def add(self, volume):
        self.volume_sum += volume
        self.volume_count += 1

    def get_avg(self):
        return self.volume_sum/self.volume_count

    def reset(self):
        self.__init__()


# global variables
result = []
current_ticker = None
close_price_starting_value = None
first_year = None
close_price_final_value = None
last_year = None
min_low_price = sys.maxsize
max_high_price = - sys.maxsize
avg_volume = AvgVolume()


def append_item_to_list():
    close_difference = close_price_final_value - close_price_starting_value
    percentage_change = (close_difference / close_price_starting_value) * 100

    record = {'ticker': current_ticker,
              'percentageChange': percentage_change,
              'minLowPrice': min_low_price,
              'maxHighPrice': max_high_price,
              'avgVolume': avg_volume.get_avg()
              }

    result.append(record)


def parse_values(value_list):
    ticker = value_list[TICKER].strip()
    year = int(value_list[DATE].strip()[0:4])
    close = float(value_list[CLOSE].strip())
    low = float(value_list[LOW].strip())
    high = float(value_list[HIGH].strip())
    volume = float(value_list[VOLUME].strip())
    return [ticker, year, close, low, high, volume]


for line in sys.stdin:
    value_list = line.strip().split(',')

    if len(value_list) == 6:
        ticker, year, close, low, high, volume = parse_values(value_list)

        if current_ticker and current_ticker != ticker:
            if first_year == START_YEAR and last_year == END_YEAR:
                append_item_to_list()

            close_price_starting_value = close
            first_year = year
            close_price_final_value = close
            last_year = year
            min_low_price = low
            max_high_price = high
            avg_volume.reset()

        else:
            if not current_ticker:
                close_price_starting_value = close
                first_year = year

            close_price_final_value = close
            last_year = year
            min_low_price = min(min_low_price, low)
            max_high_price = max(max_high_price, high)
            avg_volume.add(volume)

        current_ticker = ticker

if current_ticker:
    append_item_to_list()

sorted_result = sorted(result, key=lambda k: k['percentageChange'], reverse=True)

for item in sorted_result:
    print('{}\t{}%\t{}\t{}\t{}'.format(item['ticker'],
                                       item['percentageChange'],
                                       item['minLowPrice'],
                                       item['maxHighPrice'],
                                       item['avgVolume']))
