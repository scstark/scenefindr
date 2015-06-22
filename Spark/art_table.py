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
import uuid


sc = SparkContext("spark://", "objection")
sqlContext = SQLContext( sc )

class ArtTable( Model ):
	id = columns.Text(primary_key = True)
	feature = columns.Blob()

	def __repr__(self):
		return '%s%s' % (self.id, self.feature)

#connect the demo keyspace on our cluster running at 
connection.setup(['127.0.0.1'], 'artists')	

#do stuff with the DF here

sync_table(artists)
#artists.create( id = '544909', feature = ''	

#ArtTable.create( id = "r" , feature = "f" )
