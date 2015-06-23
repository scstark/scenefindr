from pyspark import SparkContext
from pyspark.sql import SQLContext
#from pyspark import SparkContext
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

sc = SparkContext("spark://ip-172-31-0-173:7077", "i hate this")#PRIVATE DNS HERE!!!!
sqlContext = SQLContext(sc)


class artists( Model ):
        id = columns.Text(primary_key = True)
        feature = columns.Blob()

        def __repr__(self):
                return '%s%s' % (self.id, self.feature)


def fun( ln ):
        #return (id, gen_feature) tuple
        id = ln[1]
        gen_feature = sparse.csc_matrix((1,721))
        gen_feature[0,dict[ln[2]]] = 1*ln[3]*ln[0]
        return (id, gen_feature)

#connect the demo keyspace on our cluster running at
connection.setup(['127.0.0.1'], 'scenefindr')

#do stuff with the DF here

#sync_table(artists)
#artists.create( id = '544909', feature = ''




#objection = sc.textFile( "hdfs://ec2-52-8-170-155.us-west-1.compute.amazonaws.com:9000/user/sceneFindr/history/artist_new")
#print 'FILTER HERE %s' % filter
#first load genre mapping utility file into dictionary
filename = "hdfs://ec2-52-8-170-155.us-west-1.compute.amazonaws.com:9000/user/sceneFindr/history/genres.txt" #public dns goes here
thing = sc.textFile( filename )
thing2 = thing.map(lambda x : x.split(",") )
blah = thing2.map( lambda x : (x[0],x[1]) )

dict = {}

for la in blah.collect():
	#print la
	#print la[0]
	dict[la[0]] = int( la[1] )
#for each file in the filter
objection = sqlContext.jsonFile( "hdfs://ec2-52-8-170-155.us-west-1.compute.amazonaws.com:9000/user/sceneFindr/history/artist_new")#public DNS goes here
#map to (id, name_feature) tuple
ob1 = objection.map( lambda line : fun( line ) ) 
ob2 = ob1.reduceByKey( lambda a,b : a + b ) 
#ob2 = ob1.map( lambda 
ob3 = ob2.collect()
#load path
sync_table(artists)

#write contents of ob2 (reduced vectors and ids) to cassie
for item in ob3:
	artists.create( id = item[0], feature = pickle.dumps( item[1] ) )

#print 'synced table'
#v = sparse.csc_matrix((1,721))
#v[0,1] = 1
#artists.create( id = "80951", feature = pickle.dumps( v ) )

#def fun( ln ):
#	#return (id, gen_feature) tuple
#	id = ln[1]
#	gen_feature = sparse.csc_matrix((1,721))
#	gen_feature[0,dict[ln[2]]] = 1*ln[3]*ln[0]
#	return (id, gen_feature)


#path = "hdfs://:9000/artist_new_100901.txt"
#for path in filter:
	#print 'in for loop over filter'

	#create the RDD for it
	#act = sqlContext.jsonFile(path)
	#act.printSchema()
	#act.registerTempTable("activity")
	#list = sqlContext.sql("SELECT * FROM activity")
	#list = sqlContext.sql("SELECT frequency,name,weight FROM activity")
	#list.collect()

	#f = sparse.csc_matrix((1,721))
	#for item in list: #create artist-specific feature vectors
	#	g = sparse.csc_matrix((1,721))
	#	g[0,dict[item[1]]]=1*item[0]*item[2]
	#	f = f + g
	#put f on cassandra with artist id and feature vector/
	#thing = pickle.dumps(f)#pickle file first
	#lala = path.split("_")
	#id = lala[2].split(".") #get id from the pathname to put in cassie
	#print 'adding to cassie'
	#cqlEngine.saveToCassandra( artists, id, thing )
	#artists.create( id= id[0], feature = thing ) 
#artists.create( id = '544909', feature = ''
#filter for text files
#filter2 = glob.glob( "hdfs://:9000/user/sceneFindr/history/events")

#for each path in the filter
#for path in filter2:
#diction = {}
events = sqlContext.jsonFile("hdfs://ec2-52-8-170-155.us-west-1.compute.amazonaws.com:9000/user/sceneFindr/history/events") 

blah = events.select( "resultsPage.results")

#blah2 = blah.collect[0]
#blah3 = blah2[0]
#la = blah3[0]
#	la.map( lambda x : fun(x) ) 
	#under resultsPage/results/event/venue/id?? to get easy list
	#of events per venue id.
	
	#for each venue id 
	#then get the associated events
	#then create feature vectors for each venue based on artist features
	#from cassandra
	

	#then save to cassandra:
	#metro region, venue id, venue name, lat, long, address, website, feature.
		
#get each event for each venue. return a tuple with (venue_key, [event])
#def fun(x):
#	#x is a list

#	thing1 = x[12][3] #get venue id
#	thing2 = x[5] #get performance info
#	return (thing1, thing2)
