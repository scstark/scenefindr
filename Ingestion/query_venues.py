# query_venues.py
# @author: scstark
# query_venues.py queries venues in a given city, state and saves it to a .csv file.


# import songkick client module
import songkick as sk
import httplib2 as htp2
import dateutil as dtutl
# kafka-specific imports
from kafka import *
from urllib import urlretrieve
import datetime as dt
import os
import time
import sys

#thing = Songkick

# set up kafka
mykafka = KafkaClient("localhost:9092")
producer = SimpleProducer(mykafka)
topicName = "venueData"

KEY = ""

songkick = sk.Songkick(api_key=KEY)
#test API calls
"""events = songkick.events.query( artist_name='coltrane motion',
                               per_page=10,
                               min_date=date(2009, 1, 1) )

for event in events:
	print event.display_name        # Coltrane Motion at Arlene's Grocery (June 2, 2010)
	print event.location.city       # New York, NY, US
	print event.venue.display_name  # Arlene's Grocery
"""
#makes sure date is in correct format
def date( y, m, d):
	month = ''
	day = ''
	if m < 10:
		month = '0' + str(m)
	else:
		month = str(m)
	if d < 10:
		day = '0' + str(d)
	else:
		day = str(d)
	
	print ( str(y), month, day )
	ass = '%s-%s-%s' % (str(y), month, day)
	print( ass )
	return '%s-%s-%s' % (str(y), month, day)

# query for 10 coltrane motion events, no earlier than 1/1/2009
events = songkick.events.query(artist_name='coltrane motion',
                               per_page=10,
                               min_date=date(2009, 1, 1))

# iterate over the list of events
for event in events:
    print event.display_name        # Coltrane Motion at Arlene's Grocery (June 2, 2010)
    print event.location.city       # New York, NY, US
    print event.venue.display_name  # Arlene's Grocery
