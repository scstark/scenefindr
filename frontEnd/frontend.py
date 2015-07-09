
#from cassandra.cluster import Cluster
from flask import Flask, jsonify, render_template, request
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
def query_clusters( metid ):
	print 'hi :)'

@app.route("/api/metros/<metkey>/")
def query_metros( metkey ):
	return 'hi :D'

@app.route("/recs", methods = ['GET','POST'])
def recs(  ):
	if request.method == 'GET':
		print 'method is get'
		return render_template( 'index2.html' )
	elif request.method == 'POST':
		
		#user first gives artist(s) and metro area
		artist = request.form['artist']
		print 'artist is %s' % str( artist )
		metro = request.form['metro']
		print 'metro is %s' % str( metro )
		#get cluster center for artist
		cql = "SELECT * FROM artists WHERE id = %s"
		result = session.execute( cql, parameters=[artist] )
		if result != []:
			#get at most 5 venues in that metro area from the given cluster
		
			cql2 = "SELECT * FROM venues WHERE metkey = %s AND cluster =%s LIMIT 5 ALLOW FILTERING"
			session.execute( cql2, parameters=[metro, cluster] )
			return render_template( "index2.html" )
		#	return 'recommending things :D'
		else:

			jsonresponse = {"artist": artist + " is not in the database"} # creating a json response if the username doesn't exist
			return render_template("no_userid.html", user_id = jsonresponse) # rendering template with the response
		#	print "not in database :C"
		#	return "not in database"
		#print 'method is Post'
		return render_template( "index2.html" )

@app.route("/about")
def about():
	return render_template( "about.html" )

@app.route("/contact")
def contact():
	return render_template( "contact.html" )
@app.route("/api")
def api_home():
	return render_template( "api.html" )


@app.route("/slides")
def slides():
	return render_template("slides.html")

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

