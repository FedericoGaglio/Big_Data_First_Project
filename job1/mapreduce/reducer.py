#!/usr/bin/env python
import sys

TICKER = 0
DATE = 1
CLOSE = 2
VOLUME = 3

START_YEAR = 2008
END_YEAR = 2018


class AverageObject:

    def __init__(self):
        self.volume_sum = 0
        self.volume_count = 0

    def add(self, volume):
        self.volume_sum += volume
        self.volume_count += 1

    def get_avg(self):
        return self.volume_sum / self.volume_count

    def reset(self):
        self.__init__()


result = []
current_ticker = None
first_year = None
last_year = None
close_price_starting_value = None
close_price_final_value = None
min_price = sys.maxsize
max_price = - sys.maxsize
avg_volume = AverageObject()


def append_item_to_list():
    close_difference = close_price_final_value - close_price_starting_value
    percentage_change = (close_difference / close_price_starting_value) * 100

    record = {'ticker': current_ticker,
              'percentageChange': percentage_change,
              'minPrice': min_price,
              'maxPrice': max_price,
              'avgVolume': avg_volume.get_avg()
              }

    result.append(record)


def parse_values(value_list):
    ticker = value_list[TICKER].strip()
    year = int(value_list[DATE].strip()[0:4])
    close = float(value_list[CLOSE].strip())
    volume = float(value_list[VOLUME].strip())
    return [ticker, year, close, volume]


for line in sys.stdin:
    value_list = line.strip().split('\t')

    if len(value_list) == 4:
        ticker, year, close, volume = parse_values(value_list)

        if current_ticker and current_ticker != ticker:
            if first_year == START_YEAR and last_year == END_YEAR:
                append_item_to_list()

            close_price_starting_value = close
            first_year = year
            close_price_final_value = close
            last_year = year
            min_price = close
            max_price = close
            avg_volume.reset()

        else:
            if not current_ticker:
                close_price_starting_value = close
                first_year = year

            close_price_final_value = close
            last_year = year
            min_price = min(min_price, close)
            max_price = max(max_price, close)
            avg_volume.add(volume)

        current_ticker = ticker

if current_ticker:
    append_item_to_list()

sorted_result = sorted(result, key=lambda k: k['percentageChange'], reverse=True)

for item in sorted_result:
    print('{}\t{}%\t{}\t{}\t{}'.format(item['ticker'],
                                       item['percentageChange'],
                                       item['minPrice'],
                                       item['maxPrice'],
                                       item['avgVolume']))
