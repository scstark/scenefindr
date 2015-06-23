
#from cassandra.cluster import Cluster
from flask import Flask
from cassandra.cluster import Cluster
import json
import pickle
from cqlengine import *
from scipy import sparse
cluster = Cluster([''])#ip goes here
session = cluster.connect()
session.set_keyspace( 'scenefindr' )

app = Flask(__name__)

@app.route("/api/artist/<id>")
def cass_api(id):
	print 'querying cassie for id %s' % id
	query = "SELECT feature FROM artists WHERE id = %s"
	print 'query is: %s' % query
	result = session.execute(query,parameters=[id] )
	print 'have result of query'
	print 'TYPE OF RESULT: %s' % type(result).__name__
	print 'LENGTH OF RESULT IS: %s' % str( len( result ) )
	#for item in result:
	#	print item
	response = pickle.loads( result[0][0] )
	print 'queried cassie for id %s' % id 
	print 'result of query is: %s' % response 
# 	return json.dumps( response )
#	return response
	return 'hi'

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
