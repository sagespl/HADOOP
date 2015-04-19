#!/usr/bin/env python

import sys

def map(lines):
    for line in lines:
        for word in line.split(" "):
            print("{0}<>{1}".format(word.strip(), 1))

if __name__ == "__main__":
    map(sys.stdin)
