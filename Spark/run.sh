#!/bin/bash

spark-submit --master spark://ip-172-31-0-173:7077 --executor-memory 5000m --driver-memory 5000m data_transform.py
