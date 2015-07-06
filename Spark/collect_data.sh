#!/usr/bin/env bash

sudo python fake_data.py

# move files to appropriate HDFS directories
hdfs dfs -copyFromLocal artist*.txt /user/sceneFindr/testing/400gb/artists

#hdfs dfs -copyFromLocal sk*.txt /user/sceneFindr/history/artists

# flush files from local directory
sudo rm artist*.txt
#sudo rm sk*.txt
