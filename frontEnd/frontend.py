
#from cassandra.cluster import Cluster
from flask import Flask
from cassandra.cluster import Cluster
import json
import pickle
from cqlengine import *
from scipy import sparse
ipAdr = '172.31.0.173'
cluster = Cluster([ipAdr])#ip goes here
session = cluster.connect()
session.set_keyspace( 'scenefindr' )

app = Flask(__name__)

@app.route("/api/artist/<id>")
def cass_api(id):
	print 'querying cassie for artist id %s' % id
	query = "SELECT feature FROM artists WHERE id = %s"
	print 'query is: %s' % query
	result = session.execute(query,parameters=[id] )
	print 'have result of query'
	print 'TYPE OF RESULT: %s' % type(result).__name__
	print 'LENGTH OF RESULT IS: %s' % str( len( result ) )
	#for item in result:
	#	print item
	response = pickle.loads( result[0][0] )
	print 'queried cassie for artist id %s' % id 
	print 'result of query is: %s \ni hate my life' % response 
# 	return json.dumps( response )
#	return response
	return str( response )

@app.route("/api/venue/<metkey>/<venid>")
def query_venues( metkey, venid ):
	print 'querying cassie for venue id %s' % venid
	query = "SELECT feature FROM venues WHERE metkey = %s AND venid = %s"
        print 'query is: %s' % query
        result = session.execute( query,parameters=[metkey, venid] )
	print 'have result of query'
        print 'RESULT IS: %s' % result[0][0]
	print 'TYPE OF RESULT: %s' % type(result).__name__
        print 'LENGTH OF RESULT IS: %s' % str( len( result ) )
	response = pickle.loads( result[0][0] )
	print 'queried cassie for venue id %s' % venid
        print 'result of query is: %s \ni hate my life' % response
	return str( response )
	#return 'hi'

@app.route("/api/clusters/<metid>")
def query_clusters( id ):
	print 'hi :)'

@app.route("/api/metros/<metkey>/")
def query_metros( metkey ):
	return 'hi :D'



@app.route("/")
@app.route("/index")
def hello():
	return "Hello World!"

@app.route("/holla")
def holla():
	return "Holla World!"

@app.route("/api/<month>/")
def api(month):
	return "This is data for " + month

@app.route("/api/<month>/<days>/")
def api_stats(month, days):
	data = int(days) + 10
	if data == 31:
		s = "There are " + str(data) + " days in " + month
	else:
		s = "Check a calendar"
	return s

if __name__ == "__main__": 
    app.run(host='0.0.0.0', debug=True)
