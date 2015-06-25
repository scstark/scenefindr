#!/bin/bash

# install and configure spark
sudo apt-get update

sudo apt-get --yes --force-yes install openjdk-7-jdk

# first install sbt
wget https://dl.bintray.com/sbt/debian/sbt-0.13.7.deb -P ~/Downloads
sudo dpkg -i ~/Downloads/sbt-0.13.7.deb
sudo apt-get install sbt

# now install spark
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.3.1-bin-hadoop2.6.tgz -P ~/Downloads
sudo tar zxvf ~/Downloads/spark-1.3.1-bin-hadoop2.6.tgz -C /usr/local
sudo mv /usr/local/spark-1.3.1-bin-hadoop2.6 /usr/local/spark

# Set the SPARK_HOME environment variable and add to PATH in .profile
# add to .profile
echo -e "export SPARK_HOME=/usr/local/spark\nexport PATH=$PATH:$SPARK_HOME/bin\n" | cat >> ~/.profile

. ~/.profile

# Set the Java path for spark-env
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh

echo -e "\nexport JAVA_HOME=/usr\nexport SPARK_PUBLIC_DNS=<TO BE CHANGED!!!!>\n" | cat >> $SPARK_HOME/conf/spark-env.sh
