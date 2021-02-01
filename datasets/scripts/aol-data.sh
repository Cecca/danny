#!/bin/bash


for FILE in $*; do
	cat $FILE | gunzip | cut  -f2 
done | sort | uniq
