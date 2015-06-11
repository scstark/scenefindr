
# collectVenues.py
# @author: scstark
# Collects venue data off of Eventful API and pushes it to Kafka nodes.

# from kafka import *
# from urllib import urlretreive
# import json
import eventful

thing = eventful.load_source('eventful','/usr/local/eventfulpy/eventful.py')
# mykfka = KafkaClient("localhost:9092")
# mykfka = KafkaClient("lalala:9092")

topic = "venues"
#api key here
myKey = ''

api = thing.API(myKey)

# If you need to log in:
# api.login('username','password')

events = api.call('/events/search', q='music', l='San Diego')

for event in events['events']['event']:
	print "%s at %s" % (event['title'], event['venue_name'])

#class Streamer():
#	def 


