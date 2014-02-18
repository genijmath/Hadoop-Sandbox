#!/bin/bash

#http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH4/latest/CDH4-Quick-Start/cdh4qs_topic_3_3.html


sudo service hadoop-yarn-resourcemanager stop 
sudo service hadoop-yarn-nodemanager stop 
sudo service hadoop-mapreduce-historyserver stop

for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do sudo service $x stop ; done




