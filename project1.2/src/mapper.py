#!/usr/bin/env python 

######################################################################
# mapper script for Project 1.2 .  								   ###
# input file 													   ###
######################################################################
import sys
import os

######################################################################
# 							Global Variables 					   ###
######################################################################

SPECIAL_TITLES = set(["Media", "Special", "Talk", "User", "User_talk",\
 "Project", "Project_talk", "File", "File_talk", "MediaWiki", \
 "MediaWiki_talk", "Template", "Template_talk", "Help", "Help_talk",\
 "Category", "Category_talk", "Portal", "Wikipedia", "Wikipedia_talk"])

IMG_EXTENSIONS = set([".jpg", ".gif", ".png", ".JPG", ".GIF", \
						".PNG", ".txt", ".ico"])

BOILERPLATE = set(["404_error/", "Main_Page", \
				"Hypertext_Transfer_Protocol", "Search"])


######################################################################
# 							Script Body 	 					   ###
######################################################################

# given the title, check if it's a special page
def isSpecialPage(title):
	if ":" not in title:
		return False
	title = title.split(":")[0]
	if title in SPECIAL_TITLES:
		return True


# check if the title is a image
def isImg(title):
	if len(title) >= 4 and title[-4:] in IMG_EXTENSIONS:
		return True
	return False


# return True if a line is valid
def isValid(line):
	# filter out lines with less than 4 components 
	words = line.split()
	if len(words) != 4:
		return False

	project, title, accesses, size = tuple(words)

	# filter out logs whoes title is not 'en'
	if project != "en": return False

	if isSpecialPage(title): return False

	# filter out title starting with lower case letter
	if title[0].islower(): return False

	if isImg(title): return False

	if title in BOILERPLATE: return False

	return True

# return the input filename of current mapper task
def getFilename():
	return os.environ["mapreduce_map_input_file"]

# read the log and exlude invalid lines
# then emmit a intermediate result to stdout
def filter():
	filename = getFilename()
	date = filename.split("-")[2]
	for line in sys.stdin:
		line = line.rstrip()
		if isValid(line):
			words = line.split()
			title, accesses = words[1], words[2]
			# key is the title, value is a combination of 
			# total views and date
			sys.stdout.write("%s\t%s:%s\n" % (title, accesses, date))


def main():
	filter()


if __name__ == '__main__':
	main()
