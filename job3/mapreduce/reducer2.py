#!/usr/bin/env python
import sys
import json

FIRST_YEAR = 0
SECOND_YEAR = 1
THIRD_YEAR = 2
NAME = 3

result = []

current_first_year = None
current_second_year = None
current_third_year = None

VAR = 10


def parse_values(value_list_param):
    first_year = int(float(value_list_param[FIRST_YEAR].strip()))
    second_year = int(float(value_list_param[SECOND_YEAR].strip()))
    third_year = int(float(value_list_param[THIRD_YEAR].strip()))
    name = value_list_param[NAME].strip()
    return (first_year, second_year, third_year, name)


def equals(first_trend, second_trend):
    return first_trend in range(second_trend-VAR, second_year+VAR+1)

def print_fun():
    global VAR
    global current_first_year
    global current_second_year
    global current_third_year

    first_year_var = "[" + str(current_first_year - VAR) + "," + str(current_first_year + VAR) + "]"
    second_year_var = "[" + str(current_second_year - VAR) + "," + str(current_second_year + VAR) + "]"
    third_year_var = "[" + str(current_third_year - VAR) + "," + str(current_third_year + VAR) + "]"

    print("{ f: " + first_year_var + ", s: " + second_year_var + ", t: " + third_year_var + " }")

def format_company(name_param, first_year_param, second_year_param, third_year_param):
    return str(name_param) + " { f: " + str(first_year_param) + ", s: " + str(second_year_param) + ", t: " + str(third_year_param) + " }"

def initialize_variables(first_year_param, second_year_param, third_year_param):
    global current_first_year
    global current_second_year
    global current_third_year

    current_first_year = first_year_param
    current_second_year = second_year_param
    current_third_year = third_year_param

print("VARIANCE:" + str(VAR) + "\n")

for line in sys.stdin:
    value_list = line.strip().split('\t')

    if len(value_list) == 4:
        first_year, second_year, third_year, name = parse_values(value_list)

        if not current_first_year:
            initialize_variables(first_year, second_year, third_year)
        else:
            if equals(current_first_year, first_year) and equals(current_second_year, second_year) and equals(current_third_year, third_year):
                result.append(format_company(name, first_year, second_year, third_year))
            else:
                print_fun()
                for company in result:
                    print(company)
                print("\n")
                result = [format_company(name, first_year, second_year, third_year)]
                initialize_variables(first_year, second_year, third_year)

if name:
        print_fun()
        for company in result:
            print(company)
        print("\n")


