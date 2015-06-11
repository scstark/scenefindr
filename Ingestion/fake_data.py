# fake_data.py
# Will make fake data for MVP
# in form of JSON format similar to Songkick
# 
import json
import random
import math
from faker import Factory
from datetime import datetime

#fake = Factory.create()

#print fake.name()

#print fake.bs()

genre_list = ['alternative rock','classical','baroque','romantic','jazz','indie rock','modern','metal','garage rock','grunge','post-punk','electronic','dance','hip hop','rap','pop','80s','90s','hair metal','death metal']


def create_fake_freq_dist( freq, genre, wt ):
	"""Creates fake frequency distributions for
		a fake artist. Has structure 
		{ frequency, genre, weight }.
	
	
	Args:
	artist: string

	Returns: Fake list of distributions and weights for each genre present.

	Example returned frequency vector: 
		{ 'freq': 0.22768960312989286,
		 'name': 'alternative rock', 
		'weight': 0.8341473773624973}

	"""
	#for i in range( 1 in fake.int() ):
	#wt = random.random()
	
	#genre = fake.word()

	#freq = random.random()	
	print '{ freq: %s, name: %s, wt: %s }' % (freq,genre,wt)
	dist = {'freq':freq, 'name': genre, 'wt': wt }
	#ToDo: generate multiple for a given artist, with decreasing probability
	#of adding another genre	
	return json.dumps( dist )

def create_fake_venues(venue_name, city, addr, zip, lat, long, url):
	"""Creates list of venues in a city and writes it to file
			

	"""
	print 'in create_fake_venue'
	print '{venue: %s, address: %s, city: %s, zip: %s, lat: %s, long: %s, url: %s}' % (venue_name,addr,city,zip,lat,long,url)
	info = {'name': venue_name, 'address': addr, 'city': city,
		'zip': zip, 'lat': str(lat), 'long': str(long), 'url': url}

	return json.dumps(info) 

def create_fake_event(venue, desc, opener, headliner):

	print '{venue: %s, description: %s, opener: %s, headliner: %s}' % (venue, desc, opener, headliner)

	event_info = {'venue': venue, 'description': desc, 'opener': opener, 'headliner': headliner }
	
	return json.dumps( event_info )

def fake_venues_events( num_events=10000, num_venues=150, num_artists=500, hrp="/user/sceneFindr/history/"):
	fake = Factory.create()
	#assuming one city for now.
	for v in range(num_venues):
		#each city has venues

		zip = fake.postcode()

                lat = fake.latitude()

                long = fake.longitude()

                addr = fake.street_address()

                venue_name = fake.company()

                url = fake.url()
	
		city = "San Francisco"	

		venue = create_fake_venues(venue_name, city, addr, zip, lat, long, url)
		for event in range(num_events):
			#each venue has events
			local_filename = "FakeData/venue_%s_event_%s.txt" % (v, event)
			#open filewriter
        		file_writer = open(local_filename, 'w')
	
			#each event has a description and opening act.
			desc = fake.sentence(nb_words = 6, variable_nb_words = True)
			
			opener = fake.name()
			fake_artist_dist( opener )
			headliner = fake.name()
			fake_artist_dist( headliner )
			format = create_fake_event( venue, desc, opener, headliner )
	
		
			#write to file
			file_writer.write( format  + '\n' )
			#close the writer
	       		file_writer.close()

	#put it on HDFS

def fake_artist_dist( artist ):
	local_filename = "FakeData/artist_%s_dist.txt" % artist
	
	for i in range( len(genre_list) ):
		print genre_list[i]
	
	file_writer = open( local_filename, 'w' )
	#generate new weight vectors according to a geometric distn
	#with param lambda = 1/20
	#sample without replacement from the list of genres
	#k = math.floor( len( genre_list )/3 )
	#print k
	k = 6
	genres = random.sample( genre_list, k )
	#while( count < genre_list/2 ):
	for genre in genres:
		wt = random.random()
	
		#genre = 

		freq = random.random()	
		format = create_fake_freq_dist( freq, genre, wt )
		file_writer.write( format + '\n' )
	
	file_writer.close()
	

#create_fake_freq_dist()
#create_fake_venues('San Francisco')
#create_fake_event('San Francisco')
fake_venues_events( num_events=10, num_venues=3, num_artists=20 )
