#!/usr/bin/env python
import sys

SECTOR = 0
TICKER = 1
DATE = 2
CLOSE = 3
VOLUME = 4

current_Sector = None
current_Ticker = None
current_Year = None
current_Close = 0
current_Volume = 0
result = {}


class Sector:
    def __init__(self, name):
        self.name = name
        self.year_2_record = {}
    def update(self,year,volume):
        self.year_2_record[year].add(volume)

class Record:
    def __init__(self):
        self.avg_year_volume = AverageObject()
        self.avg_year_change_company = AverageObject()
        self.avg_daily_price = AverageObject()

class AverageObject:
    def __init__(self):
        self.value = 0
        self.count = 0

    def add(self, value):
        self.value += value
        self.count += 1

    def avg(self):
        return self.value/self.count


# parse each value in value list
def parseValues(valueList):
    sector = valueList[SECTOR].strip()
    ticker = valueList[TICKER].strip()
    date = valueList[DATE].strip()
    close = float(valueList[CLOSE].strip())
    volume = int(valueList[VOLUME].strip())
    return (sector, ticker, date, close, volume)


# main script
for line in sys.stdin:
    valueList = line.strip().split(',')

    if len(valueList) == 5:
        sector, ticker, date, close, volume = parseValues(valueList)
        year = int(date[0:4])
        if current_Sector and current_Sector != sector:
            pass
        else:
            result[sector] = Sector(sector)
            if current_Ticker and current_Ticker != ticker:
                pass
            else:
                if current_Year and current_Year != year:
                    result[sector].update(current_Year, current_Volume)
                else:
                    current_Volume += volume


    current_Sector = sector
    current_Ticker = ticker
    current_Year = year
    current_Close = close

# print last computed key
if current_Sector:
    # this means that previous close value was the last value
    updateDataStructure(yearToSectorTrend,
                        current_Year,
                        'closePriceFinalValue',
                        current_Close)
    writeRecord()