#!/usr/bin/env python

import fileinput
i=1
for line in fileinput.input():
    print "{}\t{}".format(i,line),
    i+=1
