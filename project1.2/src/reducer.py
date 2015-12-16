#!/usr/bin/env python 

######################################################################
# Reducer script for Project 1.2 .  						  	   ###
# input file 													   ###
######################################################################
import sys

# given total counts and daily count,
# generate a format result and write it 
# to stdout
def generateResult(totalCount, dailyCount, title):
	threshHold = 100000
	# return if totalCount is too small
	if totalCount < threshHold:
		return
	output = "%d\t%s" % (totalCount, title)

	# gernerate daily count information
	startDate, endDate = 20150801, 20150831
	for date in xrange(startDate, endDate+1):
		if dailyCount.get(str(date)) != None:
			count = dailyCount[str(date)]
		else:
			count = 0
		output += "\t%s:%s" % (date, count)
	print output




# this function read in intermediate 
# result from stdin and do the following 
# two tasks
# 1. aggregate daily views based on daily hourly views
# 2. aggregate total views for each title
def reduce():
	currentTitle = None # keep track of current title
	totalCount = 0 # total view counts for current title
	dailyCount = dict() # store daily count for current title

	for line in sys.stdin:
		line = line.strip()
		try:
			title, count = line.split("\t", 1)
			count, date = count.split(":", 1)
		except ValueError:
			# skip the line that is not correcly formatted
			continue
		try:
			count = int(count)
		except ValueError:
			continue

		# if title is the same as currentTitle
		# update total count and dailyCount
		if title == currentTitle:
			totalCount += int(count)
			if dailyCount.get(date)==None:
				dailyCount[date] = count
			else:
				dailyCount[date] += count
		else: # generate Result and change current title
			generateResult(totalCount, dailyCount, currentTitle)
			dailyCount = {date : count}
			totalCount = count
			currentTitle = title
	# don't forget to generate result for last title
	generateResult(totalCount, dailyCount, currentTitle)


def main():
	reduce()


if __name__ == '__main__':
	main()