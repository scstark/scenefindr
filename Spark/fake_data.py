# fake_data.py

from faker import Factory
import random
#from pyspark import SparkContext
import json
import os

#pubDNS = 'ec2-52-8-170-155.us-west-1.compute.amazonaws.com'

hadoop_remote_path = '/user/sceneFindr/testing/1gb/'

fake = Factory.create()

numPages = 80

#for j in range( numPages ):
def make_data( j ):
	print 'on file %s' % str(j)
	reps = 900000 #900,000

	filename = 'artists%s.txt' % str( j )
	writer = open( filename, 'w' )
	for i in range( reps ):
		if i % 100000 == 0:
			print 'on rep %s' % str( i ) 
		id = fake.random_int( min = 1000, max = 10000000 )
		numGens = fake.random_int( min = 6, max = 25 )
		#filename = 'artist_new_%s.txt' % str( id )
		#writer = open( filename , 'w' )
		for j in range( numGens ):
			freq = random.random()
			wt = random.random()
			gen = fake.word()
			row = { 'frequency': freq, 'name': gen, 'weight': wt, 'id': id }
			writer.write( json.dumps(row) + '\n' )
		
	#writer.close()
	#os.system("hdfs dfs -moveFromLocal {local} {remote}{local}"
        #          .format(local=filename, remote=hadoop_remote_path))
	writer.close()



for i in range( numPages ):
	make_data( i )

for k in range( 160, 240 ):
	make_data( k )
