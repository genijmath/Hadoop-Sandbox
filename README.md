Hadoop-Sandbox
==============

Hadoop experiments (exercises)

Purpose of this project is to play with wikipedia data to do some computations using Hadoop.

Data source is coming from
http://en.wikipedia.org/wiki/Wikipedia:Database_download
I used Unofficial Torrent Main Link
http://kickass.to/wikipedia-english-official-offline-edition-version-20130805-xprt-t7731695.html
(Aug 5, 2013; 30AC2EF27829B1B5A7D0644097F55F335CA5241B is the info hash)

File wiki.xml.bz2 (10,082,006,833 bytes) references this data source
File wiki.0.bz2 is first 2000000 records from wiki.xml.bz2

Configuration:
Except for PageCount test (which I plan to remove), rest of the tests will use test.properties file for configuration

Environment setup:
I installed Claudera cdh4.5.0 on 6-core desktop (using YARN), Ubuntu 12.05 x64.
Deployment followed
http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH4/latest/CDH4-Quick-Start/cdh4qs_topic_3_3.html
but then some changes were applied to config files (hadoop_cfg folder contains configuraiton files)

The goal is to run some unit tests using local job tracker, and some jobs on pseudo-cluster.
Default pseudo-cluster configuration is included into hadoop_cfg folder (/etc/hadoop/conf is a soft-link to hadoop_cfg)

Maven is configured to use Claudera repository:
                <repository>
                    <!-- Cloudera Repository -->
                    <id>cloudera</id>
                    <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
                    <releases>
                        <enabled>true</enabled>
                    </releases>
                    <snapshots>
                        <enabled>true</enabled>
                   </snapshots>
                </repository>

I used IntelliJ (community edition) as an IDE

maven-surefire-plugin maven plugin was used to configure classpath.
It looks like this classpath is applicable to unit tests only, which is why all code is executed from unit tests
(in production you would use hadoop script to start your jobs, so this applies to IDE/dev setup only)




History:

PageCount is a pilot, extracts the number of pages in wiki database.
This code has bugs somewhere, I do not want to fix them as PageTitles is an improved version
I made an attempt to use Mahout's XmlInputFormat but it does not accept compressed input

PageTitles counts <title>..</title> within wiki, and generates the list of titles.
The number of <title> blocks is the same as the number of pages
This code was used to test that approach taken for parsing XMLs really works.
The approach is use TextInputFormat using </page> as a delimiter, and lstrip the value until <page> is removed.
Severe bug was discovered in Hadoop 2.0.0's LineReader class (<page> becomes page> if 4K buffer ends with <),
which was improved in 2.2.0 but currently is not entirely fixed.
https://issues.apache.org/jira/browse/MAPREDUCE-5656
https://issues.apache.org/jira/browse/HADOOP-9867  <-- unresolved at present; nice example given by Jason Lowe
Fixed is targeted for 2.3.0
org.apache.hadoop.util contains LineReader from 2.2.0 release

PageTitles includes a good unit-test launching engine.

There is no harm if I loose a couple of pages during processing (because of the HADOOP-9867 bug),
but I can avoid loosing them at all if I use only 6 mappers (no block boundary breaks delimiter in this case)
Going with only 6 mappers reduces runtime as well because of the smaller amount of book-keeping.

For future analysis I want to get pages extracted and cleaned from XML tags/wiki mark-up.
Analyzer.cleanupPage + Analyzer.cleanNonWords does precisely that in an efficient manner.

For word stem analysis, Lucene libraries are used:
http://lucene.apache.org/core/4_4_0/core/org/apache/lucene/analysis/package-summary.html




Wiki data is processing:

ETL.Import: takes original wiki data, and converts into [Title->cleaned page content] sequence file, gzip
ETL.WordStem: takes ETL.Import output and generates [Title->word-stems only] sequence file, gzip
ETL.WordCount: takes ETL.WordStem output and generates word-counts.
  NOTE: currently handles small tests nicely, but processing of the full data fails with
  "GC overhead limit exceeded"

#git push hadoop master










