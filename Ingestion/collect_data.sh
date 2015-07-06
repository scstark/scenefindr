#!/usr/bin/env bash

sudo python query_api.py

# move files to appropriate HDFS directories
hdfs dfs -copyFromLocal artist*.txt /user/sceneFindr/history/artists

hdfs dfs -copyFromLocal sk*.txt /user/sceneFindr/history/artists

# flush files from local directory
sudo rm artist*.txt
sudo rm sk*.txt
