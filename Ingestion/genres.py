import json
import os
import glob
#import requests

#for every artist file, find the genre word frequency and append it to a list if not contained.
list = []

#files = os.listdir('./RealData/')
filter = glob.glob('../../RealData/artist*.txt') 

for afile in filter:
	f = open( afile, 'r' )
	print 'reading file %s' % afile
	lines = f.readlines()

	if 'new' in afile:
		continue
	#print lines
	for j in range(len(lines)):
		#print 'line is: %s' % lines[j] 
		data = json.loads(lines[j])
		if data['response']['status']['message'] != 'Success':
			continue

		terms = data['response']['terms']
		#parse out the genre string.

		for i in range( len( terms ) ):
			thing = terms[i]
			if thing['name'] not in list:
		#if data['genre'] not in list:
				list.append( thing['name'] )
				print 'appending genre %s to list' % thing['name']
	f.close() #close reader

#write the list to file

filename = 'genres.txt'
file_writer = open( filename, 'w' )
i = 0
for item in list:
	file_writer.write( item + ',' + str(i) + '\n' )
	i = i+1
print 'wrote genre list file'
print 'total genres: %s' % str(i)
