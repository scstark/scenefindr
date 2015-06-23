SELECT * FROM watch

from cassandra.cluster import Cluster
from flask import Flask
import json

cluster = Cluster(['172.31.0.173'])
session = cluster.connect('sceneFindr')

app = Flask(__name__)

@app.route("/artist/<id>")
def cass_api(id):
  cql = "SELECT * FROM artists WHERE id = %s"
  stmt = session.execute(cql,parameters=[id] )
  response = json.dumps(stmt)
  return response

@app.route("/allart")
def cass_all():
  print "objection"
  cql = "SELECT * FROM artists"
  #bound_stmt = prepared_stmt.bind(str(repo))
  stmt = session.execute(cql)
  responselist = json.dumps(stmt)
  jsonresponse = [{"reponame":x[0], "watchcount":x[1]} for x in responselist]
  return render_template("index.html", repos = jsonresponse)


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
