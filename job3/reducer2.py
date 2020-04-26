#!/usr/bin/env python
import sys
import json

# field position of each row taken from stdin
FIRST_YEAR = 0
SECOND_YEAR = 1
THIRD_YEAR = 2
NAME = 3

result = []

current_first_year = None
current_second_year = None
current_third_year = None
current_name = None


def parse_values(value_list):
    first_year = value_list[FIRST_YEAR].strip()
    second_year = value_list[SECOND_YEAR].strip()
    third_year = value_list[THIRD_YEAR].strip()
    name = value_list[NAME].strip()
    return (first_year, second_year, third_year, name)

# main script
for line in sys.stdin:
    value_list = line.strip().split(',')

    if len(value_list) == 4:
        first_year, second_year, third_year, name = parse_values(value_list)

        if not current_first_year:
            current_first_year = first_year
            current_second_year = second_year
            current_third_year = third_year
            current_name = name
            result.append(current_name)
        else:
            if current_first_year == first_year and current_second_year == second_year and current_third_year == third_year:
                current_name = name
                result.append(current_name)
            else:
                print(str(current_first_year) + " " + str(current_second_year) + " " + str(current_third_year))
                for company in result:
                    print(company)
                result = []
                current_name = name
                result.append(current_name)
                current_first_year = first_year
                current_second_year = second_year
                current_third_year = third_year

# print last computed key
if current_name:
    print(str(current_first_year) + " " + str(current_second_year) + " " + str(current_third_year))
    for company in result:
        print(company)


