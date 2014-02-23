#!/bin/bash
/usr/lib/jvm/jdk1.6.0_37/bin/java -agentlib:hprof=cpu=samples,heap=sites,depth=6,force=n,thread=y,verbose=n,file=prof.txt -ea -Djava.library.path=/usr/lib/hadoop/lib/native -Didea.launcher.port=7533 -Didea.launcher.bin.path=/opt/intellij-idea-ce/bin -Dfile.encoding=UTF-8 -classpath /opt/intellij-idea-ce/lib/idea_rt.jar:/opt/intellij-idea-ce/plugins/junit/lib/junit-rt.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/charsets.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/javaws.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/resources.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/plugin.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/deploy.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/jsse.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/jce.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/rt.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/management-agent.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/ext/sunjce_provider.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/ext/sunpkcs11.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/ext/dnsns.jar:/usr/lib/jvm/jdk1.6.0_37/jre/lib/ext/localedata.jar:/mnt/disk0/yevgen/Projects/HDP/target/test-classes:/mnt/disk0/yevgen/Projects/HDP/target/classes:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/hadoop-mapreduce-client-core/2.0.0-cdh4.5.0/hadoop-mapreduce-client-core-2.0.0-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/hadoop-yarn-common/2.0.0-cdh4.5.0/hadoop-yarn-common-2.0.0-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/log4j/log4j/1.2.17/log4j-1.2.17.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/hadoop-yarn-api/2.0.0-cdh4.5.0/hadoop-yarn-api-2.0.0-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/org/slf4j/slf4j-api/1.6.1/slf4j-api-1.6.1.jar:/mnt/disk0/yevgen/.m2/repository/org/slf4j/slf4j-log4j12/1.6.1/slf4j-log4j12-1.6.1.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/hadoop-annotations/2.0.0-cdh4.5.0/hadoop-annotations-2.0.0-cdh4.5.0.jar:/usr/lib/jvm/jdk1.7.0_45/lib/tools.jar:/mnt/disk0/yevgen/.m2/repository/com/google/inject/extensions/guice-servlet/3.0/guice-servlet-3.0.jar:/mnt/disk0/yevgen/.m2/repository/com/google/inject/guice/3.0/guice-3.0.jar:/mnt/disk0/yevgen/.m2/repository/javax/inject/javax.inject/1/javax.inject-1.jar:/mnt/disk0/yevgen/.m2/repository/aopalliance/aopalliance/1.0/aopalliance-1.0.jar:/mnt/disk0/yevgen/.m2/repository/org/jboss/netty/netty/3.2.4.Final/netty-3.2.4.Final.jar:/mnt/disk0/yevgen/.m2/repository/com/google/protobuf/protobuf-java/2.4.0a/protobuf-java-2.4.0a.jar:/mnt/disk0/yevgen/.m2/repository/commons-io/commons-io/2.1/commons-io-2.1.jar:/mnt/disk0/yevgen/.m2/repository/com/sun/jersey/jersey-test-framework/jersey-test-framework-grizzly2/1.8/jersey-test-framework-grizzly2-1.8.jar:/mnt/disk0/yevgen/.m2/repository/com/sun/jersey/jersey-test-framework/jersey-test-framework-core/1.8/jersey-test-framework-core-1.8.jar:/mnt/disk0/yevgen/.m2/repository/org/glassfish/javax.servlet/3.0/javax.servlet-3.0.jar:/mnt/disk0/yevgen/.m2/repository/junit/junit/4.10/junit-4.10.jar:/mnt/disk0/yevgen/.m2/repository/com/sun/jersey/jersey-server/1.8/jersey-server-1.8.jar:/mnt/disk0/yevgen/.m2/repository/asm/asm/3.1/asm-3.1.jar:/mnt/disk0/yevgen/.m2/repository/com/sun/jersey/jersey-core/1.8/jersey-core-1.8.jar:/mnt/disk0/yevgen/.m2/repository/com/sun/jersey/jersey-client/1.8/jersey-client-1.8.jar:/mnt/disk0/yevgen/.m2/repository/com/sun/jersey/jersey-grizzly2/1.8/jersey-grizzly2-1.8.jar:/mnt/disk0/yevgen/.m2/repository/org/glassfish/grizzly/grizzly-http/2.1.1/grizzly-http-2.1.1.jar:/mnt/disk0/yevgen/.m2/repository/org/glassfish/grizzly/grizzly-framework/2.1.1/grizzly-framework-2.1.1.jar:/mnt/disk0/yevgen/.m2/repository/org/glassfish/gmbal/gmbal-api-only/3.0.0-b023/gmbal-api-only-3.0.0-b023.jar:/mnt/disk0/yevgen/.m2/repository/org/glassfish/external/management-api/3.0.0-b012/management-api-3.0.0-b012.jar:/mnt/disk0/yevgen/.m2/repository/org/glassfish/grizzly/grizzly-http-server/2.1.1/grizzly-http-server-2.1.1.jar:/mnt/disk0/yevgen/.m2/repository/org/glassfish/grizzly/grizzly-rcm/2.1.1/grizzly-rcm-2.1.1.jar:/mnt/disk0/yevgen/.m2/repository/org/glassfish/grizzly/grizzly-http-servlet/2.1.1/grizzly-http-servlet-2.1.1.jar:/mnt/disk0/yevgen/.m2/repository/org/glassfish/grizzly/grizzly-framework/2.1.1/grizzly-framework-2.1.1-tests.jar:/mnt/disk0/yevgen/.m2/repository/com/sun/jersey/jersey-json/1.8/jersey-json-1.8.jar:/mnt/disk0/yevgen/.m2/repository/org/codehaus/jettison/jettison/1.1/jettison-1.1.jar:/mnt/disk0/yevgen/.m2/repository/stax/stax-api/1.0.1/stax-api-1.0.1.jar:/mnt/disk0/yevgen/.m2/repository/com/sun/xml/bind/jaxb-impl/2.2.3-1/jaxb-impl-2.2.3-1.jar:/mnt/disk0/yevgen/.m2/repository/javax/xml/bind/jaxb-api/2.2.2/jaxb-api-2.2.2.jar:/mnt/disk0/yevgen/.m2/repository/javax/activation/activation/1.1/activation-1.1.jar:/mnt/disk0/yevgen/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.8.8/jackson-core-asl-1.8.8.jar:/mnt/disk0/yevgen/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.8.8/jackson-mapper-asl-1.8.8.jar:/mnt/disk0/yevgen/.m2/repository/org/codehaus/jackson/jackson-jaxrs/1.7.1/jackson-jaxrs-1.7.1.jar:/mnt/disk0/yevgen/.m2/repository/org/codehaus/jackson/jackson-xc/1.7.1/jackson-xc-1.7.1.jar:/mnt/disk0/yevgen/.m2/repository/com/sun/jersey/contribs/jersey-guice/1.8/jersey-guice-1.8.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/avro/avro/1.7.4/avro-1.7.4.jar:/mnt/disk0/yevgen/.m2/repository/com/thoughtworks/paranamer/paranamer/2.3/paranamer-2.3.jar:/mnt/disk0/yevgen/.m2/repository/org/xerial/snappy/snappy-java/1.0.4.1/snappy-java-1.0.4.1.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/commons/commons-compress/1.4.1/commons-compress-1.4.1.jar:/mnt/disk0/yevgen/.m2/repository/org/tukaani/xz/1.0/xz-1.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/hadoop-mapreduce-client-common/2.0.0-cdh4.5.0/hadoop-mapreduce-client-common-2.0.0-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/hadoop-yarn-client/2.0.0-cdh4.5.0/hadoop-yarn-client-2.0.0-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/hadoop-yarn-server-common/2.0.0-cdh4.5.0/hadoop-yarn-server-common-2.0.0-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/zookeeper/zookeeper/3.4.5-cdh4.5.0/zookeeper-3.4.5-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/jline/jline/0.9.94/jline-0.9.94.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/hadoop-hdfs/2.0.0-cdh4.5.0/hadoop-hdfs-2.0.0-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/com/google/guava/guava/11.0.2/guava-11.0.2.jar:/mnt/disk0/yevgen/.m2/repository/com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar:/mnt/disk0/yevgen/.m2/repository/org/mortbay/jetty/jetty/6.1.26.cloudera.2/jetty-6.1.26.cloudera.2.jar:/mnt/disk0/yevgen/.m2/repository/org/mortbay/jetty/jetty-util/6.1.26.cloudera.2/jetty-util-6.1.26.cloudera.2.jar:/mnt/disk0/yevgen/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar:/mnt/disk0/yevgen/.m2/repository/commons-codec/commons-codec/1.4/commons-codec-1.4.jar:/mnt/disk0/yevgen/.m2/repository/commons-lang/commons-lang/2.5/commons-lang-2.5.jar:/mnt/disk0/yevgen/.m2/repository/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar:/mnt/disk0/yevgen/.m2/repository/commons-daemon/commons-daemon/1.0.3/commons-daemon-1.0.3.jar:/mnt/disk0/yevgen/.m2/repository/javax/servlet/jsp/jsp-api/2.1/jsp-api-2.1.jar:/mnt/disk0/yevgen/.m2/repository/javax/servlet/servlet-api/2.5/servlet-api-2.5.jar:/mnt/disk0/yevgen/.m2/repository/tomcat/jasper-runtime/5.5.23/jasper-runtime-5.5.23.jar:/mnt/disk0/yevgen/.m2/repository/commons-el/commons-el/1.0/commons-el-1.0.jar:/mnt/disk0/yevgen/.m2/repository/xmlenc/xmlenc/0.52/xmlenc-0.52.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/hadoop-common/2.0.0-cdh4.5.0/hadoop-common-2.0.0-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/commons/commons-math/2.1/commons-math-2.1.jar:/mnt/disk0/yevgen/.m2/repository/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar:/mnt/disk0/yevgen/.m2/repository/commons-net/commons-net/3.1/commons-net-3.1.jar:/mnt/disk0/yevgen/.m2/repository/tomcat/jasper-compiler/5.5.23/jasper-compiler-5.5.23.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/cloudera-jets3t/2.0.0-cdh4.5.0/cloudera-jets3t-2.0.0-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/net/java/dev/jets3t/jets3t/0.6.1/jets3t-0.6.1.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/httpcomponents/httpclient/4.0.1/httpclient-4.0.1.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/httpcomponents/httpcore/4.0.1/httpcore-4.0.1.jar:/mnt/disk0/yevgen/.m2/repository/commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar:/mnt/disk0/yevgen/.m2/repository/commons-collections/commons-collections/3.2.1/commons-collections-3.2.1.jar:/mnt/disk0/yevgen/.m2/repository/commons-digester/commons-digester/1.8/commons-digester-1.8.jar:/mnt/disk0/yevgen/.m2/repository/commons-beanutils/commons-beanutils/1.7.0/commons-beanutils-1.7.0.jar:/mnt/disk0/yevgen/.m2/repository/commons-beanutils/commons-beanutils-core/1.8.0/commons-beanutils-core-1.8.0.jar:/mnt/disk0/yevgen/.m2/repository/org/mockito/mockito-all/1.8.5/mockito-all-1.8.5.jar:/mnt/disk0/yevgen/.m2/repository/net/sf/kosmosfs/kfs/0.3/kfs-0.3.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/hadoop-auth/2.0.0-cdh4.5.0/hadoop-auth-2.0.0-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/com/jcraft/jsch/0.1.42/jsch-0.1.42.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/hadoop-core/2.0.0-mr1-cdh4.5.0/hadoop-core-2.0.0-mr1-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/hsqldb/hsqldb/1.8.0.10/hsqldb-1.8.0.10.jar:/mnt/disk0/yevgen/.m2/repository/org/eclipse/jdt/core/3.1.1/core-3.1.1.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/hadoop/hadoop-test/1.0.0/hadoop-test-1.0.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/ftpserver/ftplet-api/1.0.0/ftplet-api-1.0.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/mina/mina-core/2.0.0-M5/mina-core-2.0.0-M5.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/ftpserver/ftpserver-core/1.0.0/ftpserver-core-1.0.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/ftpserver/ftpserver-deprecated/1.0.0-M2/ftpserver-deprecated-1.0.0-M2.jar:/mnt/disk0/yevgen/.m2/repository/org/hamcrest/hamcrest-core/1.1/hamcrest-core-1.1.jar:/mnt/disk0/yevgen/.m2/repository/org/hamcrest/hamcrest-all/1.1/hamcrest-all-1.1.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/mrunit/mrunit/1.0.0/mrunit-1.0.0-hadoop2.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/mahout/mahout-integration/0.7-cdh4.5.0/mahout-integration-0.7-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/mahout/mahout-core/0.7-cdh4.5.0/mahout-core-0.7-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/mahout/mahout-math/0.7-cdh4.5.0/mahout-math-0.7-cdh4.5.0.jar:/mnt/disk0/yevgen/.m2/repository/org/uncommons/maths/uncommons-maths/1.2.2a/uncommons-maths-1.2.2a.jar:/mnt/disk0/yevgen/.m2/repository/jfree/jfreechart/1.0.8a/jfreechart-1.0.8a.jar:/mnt/disk0/yevgen/.m2/repository/com/thoughtworks/xstream/xstream/1.3.1/xstream-1.3.1.jar:/mnt/disk0/yevgen/.m2/repository/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/lucene/lucene-core/4.4.0/lucene-core-4.4.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/lucene/lucene-analyzers/3.6.0/lucene-analyzers-3.6.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/mahout/commons/commons-cli/2.0-mahout/commons-cli-2.0-mahout.jar:/mnt/disk0/yevgen/.m2/repository/commons-dbcp/commons-dbcp/1.4/commons-dbcp-1.4.jar:/mnt/disk0/yevgen/.m2/repository/commons-pool/commons-pool/1.5.6/commons-pool-1.5.6.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/solr/solr-commons-csv/3.5.0/solr-commons-csv-3.5.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/lucene/lucene-benchmark/3.6.0/lucene-benchmark-3.6.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/lucene/lucene-highlighter/3.6.0/lucene-highlighter-3.6.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/lucene/lucene-memory/3.6.0/lucene-memory-3.6.0.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/lucene/lucene-queries/3.6.0/lucene-queries-3.6.0.jar:/mnt/disk0/yevgen/.m2/repository/jakarta-regexp/jakarta-regexp/1.4/jakarta-regexp-1.4.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/lucene/lucene-facet/3.6.0/lucene-facet-3.6.0.jar:/mnt/disk0/yevgen/.m2/repository/com/ibm/icu/icu4j/4.8.1.1/icu4j-4.8.1.1.jar:/mnt/disk0/yevgen/.m2/repository/xerces/xercesImpl/2.9.1/xercesImpl-2.9.1.jar:/mnt/disk0/yevgen/.m2/repository/xml-apis/xml-apis/1.3.04/xml-apis-1.3.04.jar:/mnt/disk0/yevgen/.m2/repository/org/mongodb/mongo-java-driver/2.5/mongo-java-driver-2.5.jar:/mnt/disk0/yevgen/.m2/repository/org/mongodb/bson/2.5/bson-2.5.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/cassandra/cassandra-all/0.8.1/cassandra-all-0.8.1.jar:/mnt/disk0/yevgen/.m2/repository/com/googlecode/concurrentlinkedhashmap/concurrentlinkedhashmap-lru/1.1/concurrentlinkedhashmap-lru-1.1.jar:/mnt/disk0/yevgen/.m2/repository/org/antlr/antlr/3.2/antlr-3.2.jar:/mnt/disk0/yevgen/.m2/repository/org/antlr/antlr-runtime/3.2/antlr-runtime-3.2.jar:/mnt/disk0/yevgen/.m2/repository/org/antlr/stringtemplate/3.2/stringtemplate-3.2.jar:/mnt/disk0/yevgen/.m2/repository/antlr/antlr/2.7.7/antlr-2.7.7.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/cassandra/deps/avro/1.4.0-cassandra-1/avro-1.4.0-cassandra-1.jar:/mnt/disk0/yevgen/.m2/repository/com/googlecode/json-simple/json-simple/1.1/json-simple-1.1.jar:/mnt/disk0/yevgen/.m2/repository/com/github/stephenc/high-scale-lib/high-scale-lib/1.1.2/high-scale-lib-1.1.2.jar:/mnt/disk0/yevgen/.m2/repository/org/yaml/snakeyaml/1.6/snakeyaml-1.6.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/thrift/libthrift/0.6.1/libthrift-0.6.1.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/cassandra/cassandra-thrift/0.8.1/cassandra-thrift-0.8.1.jar:/mnt/disk0/yevgen/.m2/repository/com/github/stephenc/jamm/0.2.2/jamm-0.2.2.jar:/mnt/disk0/yevgen/.m2/repository/me/prettyprint/hector-core/0.8.0-2/hector-core-0.8.0-2.jar:/mnt/disk0/yevgen/.m2/repository/org/slf4j/jul-to-slf4j/1.6.1/jul-to-slf4j-1.6.1.jar:/mnt/disk0/yevgen/.m2/repository/com/github/stephenc/eaio-uuid/uuid/3.2.0/uuid-3.2.0.jar:/mnt/disk0/yevgen/.m2/repository/com/ecyrd/speed4j/speed4j/0.9/speed4j-0.9.jar:/mnt/disk0/yevgen/.m2/repository/jfree/jcommon/1.0.12/jcommon-1.0.12.jar:/mnt/disk0/yevgen/.m2/repository/org/apache/lucene/lucene-analyzers-common/4.4.0/lucene-analyzers-common-4.4.0.jar:/etc/hadoop/conf:/usr/lib/hadoop/lib/*:/usr/lib/hadoop/.//*:/usr/lib/hadoop-hdfs/./:/usr/lib/hadoop-hdfs/lib/*:/usr/lib/hadoop-hdfs/.//*:/usr/lib/hadoop-yarn/lib/*:/usr/lib/hadoop-yarn/.//*:/usr/lib/hadoop-mapreduce/lib/*:/usr/lib/hadoop-mapreduce/.//* com.intellij.rt.execution.application.AppMain com.intellij.rt.execution.junit.JUnitStarter -ideVersion5 wiki.WordCountTest,WCSmallLocal
