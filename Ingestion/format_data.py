#format the artist JSON data into lines containing only the 3-tuple JSON formats of {frequency, name, wt}

import json
import glob

filter = glob.glob("../../RealData/artist_*.txt")
for afile in filter:
	if 'new' in afile:
		print 'new is in filename. continuing'
		continue
	print 'reading file %s' % afile
	with open(afile, 'r') as myfile:
		try:
			result = json.loads( myfile.readline() )
			if result['response']['status']['code'] > 0:
				print 'result unsuccessful. continuing.'
				continue
			data = result['response']['terms']
			print data
			#now remaining data is the list of terms.
			new_name = afile.split('_')#split into 2 parts
			nfn = new_name[0] + '_new_' + new_name[1]
			id = new_name[1].split(".")
			if '.txt' not in nfn:
				nfn = nfn + '.txt'
			#print 'nfn is: %s' % nfn
			fw = open( nfn, 'w' ) 
			for a in data:
				a['id'] = id[0]
				fw.write( json.dumps( a ) + '\n' )

			fw.close() 
			print 'wrote file %s' % nfn
		except ValueError:
			print 'value error. continuing.'
			continue
