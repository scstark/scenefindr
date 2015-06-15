import requests
import json


token = ''
#query all events in SF Metro Area
def parse_artists( data ):
	thing0 = data['resultsPage']
	thing = thing0['results']
	thing1 = thing['event']
	thing2=thing1[0]
	#print thing2.keys()
	thing3 = thing2['performance']
	#print thing3
	#for j in thing3:
	print len(thing3)
	for i in range( len(thing3) ):
		thing4 = thing3[i]
		#print thing4
	#thing4 = thing3['artist']
		thing5 = thing4['artist']
		print thing5
	#	thing4 = thing3['artist']
		artName= thing5['displayName']
		artID= str( thing5['id'] ) 
		filename = 'RealData/artist_%s_%s.txt' % (artID, artName.replace(" ","") )
		print 'writing to file %s' % filename
		writer = open( filename, 'w' )
		#write artist distribution file
		key = ''
		pt = 'http://developer.echonest.com/api/v4/artist/terms?'
		qp = {'api_key': key, 'name': artName, 'format': 'json' }#, 'bucket': 'id:songkick' }
		result = requests.get( pt, params= qp )
		print result
		LALALA = result.json()
		writer.write( json.dumps( LALALA ) )
		#writer.write( result )
		writer.close()

point = 'http://developer.echonest.com/api/v4/artist/terms?'
apkey = ''
querparams = {'api_key': apkey, 'name': 'weezer', 'format': 'json' }

my_result = requests.get(point, params = querparams )
flufflyDuckling = my_result.json()
fw = open( 'RealData/weezer_res.txt','w')
fw.write( json.dumps(  flufflyDuckling ) )
fw.close()

for i in range(1,24):
	query_params = { 'apikey': token,
        	         'location': 'sk:26330',
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
	
	file_name = "RealData/%s_events_%s.txt" % (query_params['location'],query_params['page'])
	#print full_data['resultsPage']
	file_writer = open(file_name, 'w' )

	file_writer.write( json.dumps( full_data ) + '\n' )
	file_writer.close()


#pt = 'http://developer.echonest.com/api/v4/artist/profile?'
#qp = {'api_key': token, 'name': 'weezer' }
#result = requests.get( pt, params= qp )
#print 'results for weezer: \n %s' %  result
#writer.write( json.dumps( result ) )
#writer.close()
