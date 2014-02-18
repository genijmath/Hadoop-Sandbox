#!/bin/bash

#http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH4/latest/CDH4-Quick-Start/cdh4qs_topic_3_3.html

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export HADOOP_CONF_DIR=$DIR/../hadoop_cfg

for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do sudo service $x start; done

sudo service hadoop-yarn-resourcemanager start 
sudo service hadoop-yarn-nodemanager start 
sudo service hadoop-mapreduce-historyserver start


