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
import random
import pickle
from scipy import sparse
import pyspark.mllib
from pyspark.mllib.linalg import Vectors as vectors
import pyspark.mllib.clustering as clustering

privateIP = ''
pubDNS = ''
sc = SparkContext("spark://:7077", "batch")#PRIVATE DNS HERE!!!!
sqlContext = SQLContext(sc)


#cluster = Cluster([privateIP])#ip goes here
#session = cluster.connect()
#session.set_keyspace( 'scenefindr' )

billing = { "headline":2, "support":1 }


class artists( Model ):
        id = columns.Text(primary_key = True)
        feature = columns.Blob()
	#cluster = columns.Blob()

        def __repr__(self):
                return '%s%s' % (self.id, self.feature)

class artists2( Model ):
	id = columns.Text( primary_key = True )
	cluster = columns.Blob()

	def __repr__(self):
		return '%s%s' % ( self.id, self.cluster )

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
		return '%s%s%s%s%d%d%s' %( self.metkey, self.venid, self.ven_name, self.lat, self.long, self.url, self.metname, self.feature )
class venues( Model ):
        metkey = columns.Text(primary_key = True)
        id = columns.Text(primary_key = True)
	cluster = columns.Blob()

	def __repr__(self):
		return '%s%s%s' %( self.metkey, self.id, self.cluster )
#class centers( Model ):
	#metkey = columns.Text( primary_key = True )
	#centers = columns.Blob()
	
	#def __repr__(self):
	#	return '%s%s' % ( self.metkey, self.centers )


filename = "hdfs://%s:9000/user/sceneFindr/history/genres.txt" % pubDNS #public dns goes here
thing = sc.textFile( filename ).map( lambda x : x.split( "," ) ).map( lambda x : ( x[0],x[1] ) )
#thing2 = thing.map(lambda x : x.split(",") )
#blah = thing2.map( lambda x : (x[0],x[1]) )

gens = {}

for la in thing.collect():
	#print la
	#print la[0]
	gens[la[0]] = int( la[1] )





def fun( ln ):
        #return (id, gen_feature) tuple
        id = str( ln[1] )
        gen_feature = sparse.csc_matrix((len(gens.keys()),1))
        try:
		gen_feature[gens[ln[2]],0] = 1*ln[3]*ln[0]
        except KeyError:
		print 'hi'
	return (id, gen_feature)

#connect the demo keyspace on our cluster running at
connection.setup(['127.0.0.1'], 'scenefindr')

#for each file in the filter
artistPath = "hdfs://%s:9000/user/sceneFindr/history/artist_new" % pubDNS

arts = sqlContext.jsonFile( artistPath ).map( lambda line : fun( line ) ).reduceByKey( lambda a,b : a + b ).coalesce(10).cache()
#arts3 = arts.reduceByKey( lambda a,b : a + b ).coalesce(10)#public DNS goes here
#map to (id, name_feature) tuple
#arts2 = arts.collect()
#load path
sync_table(artists)

#artFeatures = {}
#write contents of ob2 (reduced vectors and ids) to cassie
#for item in arts2:
#	artFeatures[item[0]] = item[1]
	#artists.create( id = item[0], feature = pickle.dumps( item[1] ) )

#arts = ob2.collectAsMap()
#arts = sc.parallelize( ob3 ).collectAsMap()
#ob3.cache()

def fun4( art_id ):
	#if art_id == '8084088
	#if str( art_id ) in keys.keys():
	#	return keys[str( art_id )]
        #else:
	#	return sparse.csc_matrix((1,721))
	vec = sparse.csc_matrix( (len(gens.keys()),1))
	num = random.randint(1,20)
	
	population = range( 0, len( gens.keys() ) - 1  )
	
	indices = random.sample( population, num )
	
	for item in indices:
		vec[item,0] = random.random()

	return vec
	#try:
	#	return artFeatures[str( art_id )]
		#return [y[0] for y in ob3].index(art_id)
	#except KeyError:
	#	vec = sparse.csc_matrix((len(gens.keys()),1))
	#	vec[4,0] = 1 #debugging purposes
		#return sparse.csc_matrix((721,1))
	#	return vec

def getBilling( b ):
	#if b in billing.keys():
	#	return billing[b]
	#else:
	#	return 1
	try:
		return billing[b]
	except KeyError:
		return 1


def fun5( input ):
	#input is list of artists for a venue
	try:
		vecList = map( lambda x : fun4( x[0][1] ) * getBilling( x[1] ), input)
		return reduce( lambda a,b: a+b, vecList)
	except TypeError:
		vec = sparse.csc_matrix( (len(gens.keys()),1) )
		#vec[5,0] = 1 #debugging purposes
		#return sparse.csc_matrix( (721,1) )
		return vec


#for each path in the filter
#for path in filter2:
#diction = {}

eventFiles = "hdfs://%s:9000/user/sceneFindr/history/events" % pubDNS

events = sqlContext.jsonFile(eventFiles) 
#sync_table(venues)
ev2 = events.select( "resultsPage.results.event" )
rdd = sc.parallelize( [] )
#print 'LENGTH OF ev2 IS: %s' % str( len( ev2 ) )
for item in ev2.collect():
	list = item[0]
	rdd2 = sc.parallelize( list )
	rdd = rdd.union( rdd2 )
	ev3 = item[0]
	ev4 = ev3[0]
	ev5 = sc.parallelize( ev4 )
rdd3 = rdd.map( lambda x : ( ( x[12], x[4] ), x[5] ) ).reduceByKey( lambda a,b : a + b ).coalesce( 10 )
#aggregate the performance lists by venue information
ev7 = rdd3.map( lambda x : ( x[0], fun5( x[1] ) ) )
#ev8 = ev7.map( lambda x : ( x[0], reduce( lambda a,b: a+b, x[1] ) )
#newEvents = ev8.collect()	
ev8 = ev7.collect()
for item in ev8:
	venues.create( metkey = str(item[0][0][4][2]), venid = str(item[0][0][1]), feature = pickle.dumps( item[1] ), lat = item[0][0][2], long = item[0][0][3], metname = item[0][0][4][1], url = item[0][0][5], ven_name = item[0][0][0] )
	
	#print ev7.first()
	#( 0 : areaKey, 1 : ven_id, 2 : ven_name, 3 : lat, 4 : long, 5 : url, 6 : metroName, 7 : feature, )



#start batch ML algorithms
ev9 = ev7.map( lambda x : x[1] ) #only use feature vectors for this
#PCA first ---- not actually implemented in pyspark 1.4 yet!!!


#then KMeans
model = clustering.KMeans()


#ev9 = ev7.map( lambda x : x[1] ) # only use feature vectors for this

model2 = model.train( ev9, 28 ) # rdd,k

#match artists to that cluster
lalala = arts.map( lambda x : ( x[0], x[1], model.predict( x[1] ) ) )
#match venues to cluster
ev10 = ev7.map( lambda x : ( x[0], x[1], model.predict( x[1] ) ) )

#artFeatures = arts2.collectAsMap()
#arts2 = arts.collect()
#for item in arts2:
#	best = model2.predict( item[1] )
	#artsits2.create( id = item[0], cluster = pickle.dumps( best ) )

for item in lalala.collect():
	artists2.create( id = item[0], cluster = pickle.dumps( item[2] )

for item in ev10.collect():
	vens2.create( metkey = str(item[0][0][4][2]), id = str( item[0][0][1] ), cluster = pickle.dumps( item[2] ) )
