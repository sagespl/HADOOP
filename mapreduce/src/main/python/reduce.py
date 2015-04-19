#!/usr/bin/env python

import sys

def reduce(lines):
    lastKey = None
    sum = 0
    for line in lines:
        key, value = line.split("<>")
        sum += int(value)
        if key != lastKey:
            print("{0},{1}".format(key, sum))
            sum = 0

        lastKey = key

if __name__ == "__main__":
    reduce(sys.stdin)



