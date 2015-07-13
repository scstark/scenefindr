#!/usr/bin/env bash

sudo python query_api.py

sudo python format_data.py

# move files to appropriate HDFS directories
hdfs dfs -copyFromLocal ../../RealData/artist_new*.txt /user/sceneFindr/history/artists

hdfs dfs -copyFromLocal ../../RealData/sk*.txt /user/sceneFindr/history/events

# flush files from local directory
#sudo rm artist*.txt
#sudo rm sk*.txt
