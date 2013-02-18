#!/usr/bin/env python

import sys
import os
import random
import time
import datetime
from pyes import ES

strings = ['%032x' % random.randrange(256**15)] * 1000

def random_string_generator():
    return strings[random.randint(0,999)]

def main(argv):
    start = 1
    if len(sys.argv) > 1:
        if sys.argv[1]:
            start = sys.argv[1]

    bulksize = 1000

    es = ES(("http", "localhost", 9200), bulk_size=bulksize)

    c0 = 0
    t0 = time.time()
    c1 = 0
    t1 = time.time()
    for n in range(start, start + 1000000):
        result = es.index({ 
                 'a' : random_string_generator(),
                 'b' : random_string_generator(),
                 'c' : random_string_generator(),
                 'd' : random_string_generator(),
                 'e' : random_string_generator(),
                 'f' : random_string_generator(),
                 'g' : random_string_generator(),
                 'h' : random_string_generator(),
                 'i' : random_string_generator(),
                 'j' : random_string_generator(),
                 'k' : random_string_generator(),
                 'l' : random_string_generator(),
                 'm' : random_string_generator(),
                 'n' : random_string_generator(),
                 'o' : random_string_generator(),
                 'p' : random_string_generator(),
                 'q' : random_string_generator(),
                 'r' : random_string_generator(),
                 's' : random_string_generator(),
                 't' : random_string_generator(),
                 'u' : random_string_generator(),
                 'v' : random_string_generator(),
                 'w' : random_string_generator(),
                 'x' : random_string_generator(),
                 'y' : random_string_generator(),
                 'z' : random_string_generator()
             }, 'pyindex', 'pytype', n, bulk=True)
        c0 = c0 + bulksize
        c1 = c1 + bulksize
        if result:
            d0 = (time.time() - t0) 
            d1 = (time.time() - t1) 
            now = datetime.datetime.utcnow()
            print("{0},{1},{2},{3},{4},{5},{6},{7}"
                .format(now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), result.took, c0, d0, c0/(d0*bulksize), c1, d1, c1/(d1*bulksize)))
            c1 = 0
            t1 = time.time()

if __name__ == "__main__":
    main(sys.argv[1:])

