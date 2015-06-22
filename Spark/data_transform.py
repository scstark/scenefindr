from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkContext
from cassandra.cluster import Cluster
from pyspark import SparkConf
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
from datetime import datetime
from cqlengine import *
#import art_table
import uuid
import glob
import pickle
from scipy import sparse
sc = SparkContext("spark://:7077", "i hate this")
sqlContext = SQLContext(sc)


class ArtTable( Model ):
        id = columns.Text(primary_key = True)
        feature = columns.Blob()

        def __repr__(self):
                return '%s%s' % (self.id, self.feature)

#connect the demo keyspace on our cluster running at
connection.setup(['127.0.0.1'], 'artists')

#do stuff with the DF here

#sync_table(artists)
#artists.create( id = '544909', feature = ''




filter = glob.glob( "hdfs://:9000/artist_new_*.txt")

#first load genre mapping utility file into dictionary
filename = "hdfs://:9000/genres.txt"
thing = sc.textFile( filename )
thing2 = thing.map(lambda x : x.split(",") )
blah = thing2.map( lambda x : (x[0],x[1]) )

for item in blah.collect():
	dict[x[0]] = int( x[1] )
#for each file in the filter
#load path
sync_table(artists)
#path = "hdfs://:9000/artist_new_100901.txt"
for path in filter:
	

	#create the RDD for it
	act = sqlContext.jsonFile(path)
	#act.printSchema()
	act.registerTempTable("activity")
	#list = sqlContext.sql("SELECT * FROM activity")
	list = sqlContext.sql("SELECT frequency,name,weight FROM activity")
	list.collect()

	f = sparse.csc_matrix((1,721))
	for item in list: #create artist-specific feature vectors
		g = sparse.csc_matrix((1,721))
		g[0,dict[item[1]]]=1*item[0]*item[2]
		f = f + g
	#put f on cassandra with artist id and feature vector/
	thing = pickle.dumps(f)#pickle file first
	lala = path.split("_")
	id = lala[2].split(".") #get id from the pathname to put in cassie
	#cqlEngine.saveToCassandra( artists, id, thing )
	ArtTable.create( id= id[0], feature = thing ) 
#artists.create( id = '544909', feature = ''
#filter for text files
#filter2 = glob.glob( "hdfs://:9000/user/sceneFindr/history/events")

#for each path in the filter
#for path in filter2:
#diction = {}
#events = sc.textFile("hdfs://:9000/user/sceneFindr/history/events") 
#	.map( lambda x : fun(x) ) 
	#under resultsPage/results/event/venue/id?? to get easy list
	#of events per venue id.
	
	#for each venue id 
	#then get the associated events
	#then create feature vectors for each venue based on artist features
	#from cassandra
	

	#then save to cassandra:
	#metro region, venue id, venue name, lat, long, address, website, feature.
		
#get each event for each venue. return a tuple with (venue_key, [event])
def fun(x):
	print x
