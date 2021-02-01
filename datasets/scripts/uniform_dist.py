#!/usr/bin/python

import sys
import random
import numpy.random

sets = int(sys.argv[1])
avgsetsize=int(sys.argv[2])
universe = int(sys.argv[3])

def getsetsize(setsizeavg):
	return numpy.random.poisson(setsizeavg)


setlist = []
for i in range(sets):
	setsize = getsetsize(avgsetsize)
	newset = []
	for i in range(setsize):
		token = random.randint(1, universe)
		newset.append(token)
	setlist.append(newset)

for l in setlist:
    print " ".join([ str(v) for v in sorted(l)])
