Hadoop-Sandbox
==============

This project implements a word-count map-reduce job using Wiki data in an efficient way.  
This involves cleaning wiki markup, stemming, and "in-mapper combining" design pattern.  
Surprisingly it covers many techniques described by Tom White.  
Couple of critical Hadoop-2.0.0 bugs where discovered along the way.  

Jobs are executed through unit tests; some jobs are submitted locally and some jobs run on pseudo-cluster.


Data sources
============

Data source is coming from [wiki-offline] (http://en.wikipedia.org/wiki/Wikipedia:Database_download)  
I used [Unofficial Torrent Main Link] (http://kickass.to/wikipedia-english-official-offline-edition-version-20130805-xprt-t7731695.html)
(Aug 5, 2013; 30AC2EF27829B1B5A7D0644097F55F335CA5241B is the info hash)

File wiki.xml.bz2 (10,082,006,833 bytes) references this data source.  
File wiki.0.bz2 is first 2000000 records from wiki.xml.bz2  
(see test.properties)



Environment setup
=================

I installed Claudera cdh4.5.0 on 6-core desktop (using YARN), Ubuntu 12.05 x64.  
Deployment followed [CDH4 Quick Start] (http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH4/latest/CDH4-Quick-Start/cdh4qs_topic_3_3.html)
but then some changes were applied to config files (`hadoop_cfg` folder contains configuration files)

Default pseudo-cluster configuration is included into `hadoop_cfg` folder (`/etc/hadoop/conf` is a soft-link to `hadoop_cfg`)

Maven is configured to use Cloudera repository:

    `<repository>
        <!-- Cloudera Repository -->
        <id>cloudera</id>
        <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
        <releases>
            <enabled>true</enabled>
        </releases>
        <snapshots>
            <enabled>true</enabled>
       </snapshots>
    </repository>`


I used IntelliJ (community edition) as an IDE.

maven-surefire-plugin maven plugin was used to configure classpath.  
It looks like this classpath is applicable to unit tests only, which is why all code is executed from unit tests  
(in production you would use hadoop script to start your jobs, so this applies to IDE/dev setup only)


Configuration
=============
Tests use test.properties file for configuration.  
Some test have hardcoded values of 6 or 12 which reflect number of cores on my desktop


Notes
=====

Wiki data comes as XML file (bzip2).  
I made an attempt to use Mahout's `XmlInputFormat` but it does not accept compressed input.

The approach used to parse XML is to use `TextInputFormat` format with `</page>` as a delimiter.

Severe bug was discovered in Hadoop `2.0.0`'s `LineReader` class (`<page>` becomes `page>` if 4K buffer ends with `<`),
which seems to be fixed in `2.2.0`. However this approach is still impacted by [MAPREDUCE-5656]
(https://issues.apache.org/jira/browse/MAPREDUCE-5656) or [HADOOP-9867]
(https://issues.apache.org/jira/browse/HADOOP-9867) which is targeted to be fixed in `2.3.0`.

`org.apache.hadoop.util` included in the source code contains `LineReader` class from `2.2.0` release.

There is no harm if I loose a couple of pages during processing (because of the [HADOOP-9867]
(https://issues.apache.org/jira/browse/HADOOP-9867) bug),
but I can avoid loosing them at all if I use only 6 mappers (no block boundary breaks delimiter in this case)

Going with only 6 mappers reduces runtime as well because of the smaller amount of book-keeping.


Implementation remarks
======================
Wiki mark-up removal is implemented in `Analyzer.cleanupPage` and `Analyzer.cleanNonWords`.

For word stem analysis, [Lucene] (http://lucene.apache.org/core/4_4_0/core/org/apache/lucene/analysis/package-summary.html)
libraries are used.

`wiki.WordCount` originally used standard HashMap, but it failed with "GC overhead limit exceeded" on the full dataset;
so special hash-map implementation was used.




Wiki data is processing
=======================

`wiki.ETL.Import`: takes original wiki data, and converts into [Title->cleaned page content] sequence file, `gzip`  
`wiki.ETL.WordStem`: takes `ETL.Import` output and generates [Title->word-stems only] sequence file, `gzip`  
`wiki.WordCount`: takes `ETL.WordStem` output and generates word-counts.  



*git push hadoop master*












