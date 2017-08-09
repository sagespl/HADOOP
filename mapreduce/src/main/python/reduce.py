#!/usr/bin/env python

import sys

def reduce(lines):
    lastKey = None
    sum = 0
    for line in lines:
        key, value = line.split("<>")
        if key != lastKey and lastKey is not None:
            print("{0},{1}".format(lastKey, sum))
            sum = 0

        sum += int(value)
        lastKey = key

if __name__ == "__main__":
    reduce(sys.stdin)



