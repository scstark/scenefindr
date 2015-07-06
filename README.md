# SceneFindr

###Stephanie Stark

###Insight Data Engineering

Table of Contents
1. Introduction
2. Data Sources
3. Pipeline
	-Ingestion
	-File Distribution
	-Batch Processing
	-Database
	-Front-End Application
4. Dependencies
5. Future Work
6. Acknowledgements

1. Introduction
	Live music recommendation engine for events in your area based on bands you like. Developed in collaboration with Eric White, an Insight Data Science fellow.

2. Data Sources
	Data was collected from Songkick's Events API and Echonest's Artists API. We collected data on upcoming events in various metro areas and characterized their 'sound' through term frequencies for genres of music via Echonest's term frequency data, which gives a frequency and weight for each genre provided.

3. Pipeline

[picture]
	-Ingestion: Data from both APIs were in JSON format. I used a combination of Python and shell scripts to query the APIs from the cluster name node and put them onto HDFS.
	-File Distribution: HDFS provides distributed storage throughout the cluster. To play to HDFS's strength of , files were stored in ~100 MB blocks out of 128 MB.
	-Batch Processing: I used Spark 1.3.1 for its fast computation and access to Machine Learning libraries like Sparkling Water and MLlib, which made the collaboration possible. I soon plan to upgrade to Spark 1.4.
	-Database: I used four tables. My aim was to have the feature vectors for artists, events, and venues immediately able to be queried from the API apart from the clustering and so there were three tables dedicated to that. Finally, a fourth table stored clustering results for each artist. Cassandra presented itself as the ideal database for the purposes of availability and persistence, since live music does not update that often, retraining the model weekly would suffice, and availability would be key in the envisioning this project as a service for users as well as for the ease of a data scientist in querying data.
	-Front-End Application: *Under construction* I used Flask for website handling, with Bootstrap for the template. 


4. Dependencies
	I used the following open-source packages for connectors between the technologies outlined above:
	-CqlEngine for Pyspark and Cassandra.

5. Future Work
	Future work would include refining the construction of feature vectors, using Principal Component Analysis for feature dimension reduction (present in Spark MLlib 1.4), adding TF/IDF support for further compatibility with Eric White's model, and implementing cosine similarity for a more accurate measure of similarity between vectors. I would also like to add a Google Maps support to the front end website to show the locations of the recommended venues.


6. Acknowledgements
	Many thanks to Eric White for collaborating. Additionally, many thanks to the Insight team for the wonderful opportunity and my fellow Fellows for their support.
