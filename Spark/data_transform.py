from pyspark import SparkContext
from pyspark.sql import SQLContext
#from pyspark import SparkContext
from cassandra.cluster import Cluster
from pyspark import SparkConf
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine import query
from cqlengine.management import sync_table
from datetime import datetime
from cqlengine import *
#import art_table
import uuid
import glob
import pickle
from scipy import sparse
import pyspark.mllib
from pyspark.mllib.linalg import Vectors as vectors
import pyspark.mllib.clustering as clustering

privateIP = ''
pubDNS = ''
sc = SparkContext("spark://:7077", "i hate this")#PRIVATE DNS HERE!!!!
sqlContext = SQLContext(sc)


cluster = Cluster([privateIP])#ip goes here
session = cluster.connect()
session.set_keyspace( 'scenefindr' )

billing = { "headline":2, "support":1 }


class artists( Model ):
        id = columns.Text(primary_key = True)
        feature = columns.Blob()

        def __repr__(self):
                return '%s%s' % (self.id, self.feature)

class venues( Model ):
	metkey = columns.Text(primary_key = True)
	venid = columns.Text(primary_key = True)
	ven_name = columns.Text()
	url = columns.Text()
	lat = columns.Float()
	long = columns.Float()
	metname = columns.Text()
	feature = columns.Blob()
	#(areaKey, ven_id, ven_name, lat, long, url, metroName, feature)
	def __repr__(self):
		return '%s%s%s%s%d%d%s' %( self.metro, self.ven_id, self.ven_name, self.lat, self.long, self.url, self.metName, self.feature )


def fun( ln ):
        #return (id, gen_feature) tuple
        id = ln[1]
        gen_feature = sparse.csc_matrix((721,1))
        gen_feature[dict[ln[2]],0] = 1*ln[3]*ln[0]
        return (id, gen_feature)


def fun2( id, input ):
	#input is a list of all performers at a venue
	
	print 'in fun2'
	#id is the venue id
	#will return aggregated feature vector for venue
	#rdd = sc.parallelize(input)
	
	#for item in input:
	#cassie_query = "SELECT feature FROM artists WHERE id=%s" % input[1]

	#	result = session.execute( cassie_query )

	#	pickle.loads(result) #unpickle the feature vector for arithmetic
		#get the headline scalar and read artist's feature vector from cassandra
	#rdd2 = rdd
	lalalala = map( lambda x : fun4(x[0][0][1]) * billing[x[0][1]], input )#.reudceByKey( lambda a,b: a + b )
	#return 'hi'
	print lalalala
	#return the venue's features.
	return lalalala

def fun3( input ):
	ven_name = input[0][0][0]
	ven_id = input[0][0][1]
	lat = input[0][0][2]
        long = input[0][0][3]
        url = input[0][0][5]
        areaKey = input[0][0][4][2]
        metroName = input[0][0][4][1]
        feature = fun2( ven_id, input[1] )

	return [areaKey, ven_id, ven_name, lat, long, url, metroName, feature]

#def fun4( art_id ):
	#helper function to query cassandra for artist features
	#q = "SELECT feature FROM artists WHERE id=%s" % art_id
	#result = session.execute( q ) 
	#print 'TYPE OF RESULT: %s' % type(result).__name__
	#dill = pickle.loads( result[0][0] )
	#response = pickle.loads( result[0][0] )
        #print 'queried cassie for id %s' % id
        #print 'result of query is: %s' % response
	#print 'TYPE OF RESULT: %s' % type(dill).__name__
	#return pickle.loads( result[0][0] )
	#return dill
#connect the demo keyspace on our cluster running at
connection.setup(['127.0.0.1'], 'scenefindr')

#do stuff with the DF here

#sync_table(artists)

#def fun4( art_id ):
#	return keys[art_id]
#print 'FILTER HERE %s' % filter
#first load genre mapping utility file into dictionary

filename = "hdfs://%s:9000/user/sceneFindr/history/genres.txt" % pubDNS #public dns goes here
thing = sc.textFile( filename )
thing2 = thing.map(lambda x : x.split(",") )
blah = thing2.map( lambda x : (x[0],x[1]) )

dict = {}

for la in blah.collect():
	#print la
	#print la[0]
	dict[la[0]] = int( la[1] )
#for each file in the filter
artistPath = "hdfs://%s:9000/user/sceneFindr/history/artist_new" % pubDNS

objection = sqlContext.jsonFile( artistPath )#public DNS goes here
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

keys = ob2.collectAsMap()

def fun4( art_id ):
	#if art_id == '8084088
	#if art_id in keys.keys():
	#	return keys[art_id]
        #else:
	#	return sparse.csc_matrix((1,721))
	try:
		return keys[art_id]
	except KeyError:
		return sparse.csc_matrix((721,1))
#def fun( ln )
#	#return (id, gen_feature) tuple
#	id = ln[1]
#	gen_feature = sparse.csc_matrix((1,721))
#	gen_feature[0,dict[ln[2]]] = 1*ln[3]*ln[0]
#	return (id, gen_feature)


def getBilling( b ):
	if b in billing.keys():
		return billing[b]
	else:
		return 1
	


def fun5( input ):
	#input is list of artists for a venue
	try:
		list = map( lambda x : fun4( x[0][0][1] ) * getBilling( x[0][1] ), input)
		return reduce( lambda a,b: a+b, list)
	except TypeError:
		return sparse.csc_matrix( (721,1) )


#for each path in the filter
#for path in filter2:
#diction = {}
eventFiles = "hdfs://%s:9000/user/sceneFindr/history/events" % pubDNS

events = sqlContext.jsonFile(eventFiles) 
sync_table(venues)
ev2 = events.select( "resultsPage.results.event")
rdd = sc.parallelize( [] )
#print 'LENGTH OF ev2 IS: %s' % str( len( ev2 ) )
for item in ev2.collect():
	list = item[0]
	rdd2 = sc.parallelize( list )
	rdd = rdd.union( rdd2 )
#	ev3 = item[0]
#	ev4 = ev3[0]
#	ev5 = sc.parallelize( ev4 )
rdd3 = rdd.map( lambda x : ( ( x[12], x[4] ), x[5] ) ).reduceByKey( lambda a,b : a + b ).coalesce( 10 )
#aggregate the performance lists by venue information
#ev7 = ev6.map( lambda x : fun3( x )  )
#rdd4 = rdd3.collect()
#ev7 = rdd3.map( lambda x : ( x[0], map( lambda y: fun4( y[0][0][1] )*getBilling(y[0][1]), x[1] ) ) )#.reduceByKey( lambda a,b: a+b )
#ev7 = ev6.map( lambda x : ( x[0], reduce( lambda a,b: a + b, map( lambda y: fun4( y[0][0][1] )*getBilling( y[0][1] ), x[1] ) ) ) )
ev7 = rdd3.map( lambda x : ( x[0], fun5( x[1] ) ) )
#ev8 = ev7.map( lambda x : ( x[0], reduce( lambda a,b: a+b, x[1] ) )
#newEvents = ev8.collect()	
ev8 = ev7.collect()
for item in ev8:
	venues.create( metkey = str(item[0][0][4][2]), venid = str(item[0][0][1]), feature = pickle.dumps( item[1] ), lat = item[0][0][2], long = item[0][0][3], metname = item[0][0][4][1], url = item[0][0][5], ven_name = item[0][0][0] )
	
	#print ev7.first()
	#( 0 : areaKey, 1 : ven_id, 2 : ven_name, 3 : lat, 4 : long, 5 : url, 6 : metroName, 7 : feature)
	#for item in ev8:
	#	venues.create( areaKey = item[0], ven_id = item[1], ven_name = item[2], lat = item[3], long = item[4], url = item[5], metroName = item[6], feature = item[7] )
	#for thing in ev6.collect():
	#	ven_name = thing[0][0][0]
	#	ven_id = thing[0][0][1]
	#	lat = thing[0][0][2]
	#	long = thing[0][0][3]
	#	url = thing[0][0][5]
	#	areaKey = thing[0][0][4][2]
	#	metroName = thing[0][0][4][1]
	#	feature = fun2( thing[1] )

#start batch ML algorithms
model = clustering.KMeans()

ev9 = ev7.map( lambda x : x[1] ) #only use feature vectors for this

model.train( ev9, 28 ) #rdd, k


		
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
