#!/usr/bin/python

import sys
import random

infile = sys.argv[1]

def shuf_print(curlines):
	random.shuffle(curlines)
	for line in curlines:
		print line,	

with open(infile) as rawin:
	curlen = 0
	curlines = []
	for line in rawin:
		sl = line.rstrip().split()
		if len(sl) != curlen:
			shuf_print(curlines)
			curlines = []
		curlen = len(sl)
		curlines.append(line)

	shuf_print(curlines)
