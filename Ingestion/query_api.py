import requests
import json
import time

token = 'WGLTwENb36bAgHNc'
#query all events in LA Metro Area
def parse_artists( data ):
	thing0 = data['resultsPage']
	thing = thing0['results']
	thing1 = thing['event']
	for i in range( len(thing1) ):
		thing2=thing1[i]
		#print thing2.keys()
		thing3 = thing2['performance']
		#print thing3
		#for j in thing3:
		print 'length of thing3: %s' % int(len(thing3))
		#for i in range( len(thing3) ):
		if len(thing3) >0:
			thing4 = thing3[0]
			#print thing4
			#thing4 = thing3['artist']
			thing5 = thing4['artist']
			print thing5
			#	thing4 = thing3['artist']
			artName= thing5['displayName']
			artID= str( thing5['id'] ) 
			filename = '../../RealData/artist_%s.txt' % artID
			#print 'writing to file %s' % filename
			#writer = open( filename, 'w' )
			#write artist distribution file
			key = '8HMTSGFYLCXWETBE0'
			#query EchoNest for artist TF
			pt = 'http://developer.echonest.com/api/v4/artist/terms?'
			qp = {'api_key': key, 'name': artName, 'format': 'json' }#, 'bucket': 'id:songkick' }
			result = requests.get( pt, params= qp )
			print 'result is %s' % result
			if '429' in str( result ):
				print 'rate limited. retrying in 60s'
				time.sleep(60) #wait out the timeout - a minute
				result = requests.get( pt, params = qp ) #retry requests
			writer = open( filename, 'w' )
			LALALA = result.json()
			writer.write( json.dumps( LALALA ) )
			print 'writing to file %s' % filename
			#writer.write( result )
			writer.close()

#point = 'http://developer.echonest.com/api/v4/artist/terms?'
#apkey = ''
#querparams = {'api_key': apkey, 'name': 'weezer', 'format': 'json' }

#my_result = requests.get(point, params = querparams )
#flufflyDuckling = my_result.json()
#fw = open( 'RealData/weezer_res.txt','w')
#fw.write( json.dumps(  flufflyDuckling ) )
#fw.close()

for i in range(1,20):
	query_params = { 'apikey': token,
        	         'location': 'sk:9426',
                	 'per_page': 100,
                 	'page': i,
		         }

	endpoint = 'http://api.songkick.com/api/3.0/events.json?'

	response = requests.get( endpoint, params=query_params )
	full_data = response.json()
	print 'on page %s of events' % str(i) #inform of request progress
	
	#get artist information
	#go to 'performance:artist:id' and get their distributions from echonest
	parse_artists( full_data )
	locName = query_params['location'].split(":")
	metroName = locName[0]+locName[1]
	file_name = "../../RealData/%s_events_%s.txt" % (metroName,query_params['page'])
	#print full_data['resultsPage']
	file_writer = open(file_name, 'w' )
	#data = full_data['resultsPage']['results']['event']
	file_writer.write( json.dumps( full_data ) + '\n' )
	file_writer.close()


#pt = 'http://developer.echonest.com/api/v4/artist/profile?'
#qp = {'api_key': token, 'name': 'weezer' }
#result = requests.get( pt, params= qp )
#print 'results for weezer: \n %s' %  result
#writer.write( json.dumps( result ) )
#writer.close()
