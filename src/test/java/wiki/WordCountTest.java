/**
 * Copyright 2014 Yevgen Yampolskiy
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wiki;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import java.io.File;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.security.Credentials;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class WordCountTest extends TestGeneric{
    MapDriver<Text, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        WordCount.WCMapper mapper = new WordCount.WCMapper();
        WordCount.WCReducer reducer = new WordCount.WCReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new Text("article_nm"), new Text(
                "little little pig the little"));
        mapDriver.withOutput(new Text("the"), new IntWritable(1));
        mapDriver.withOutput(new Text("pig"), new IntWritable(1));
        mapDriver.withOutput(new Text("little"), new IntWritable(3));
        mapDriver.runTest();
    }

    @Test
    public void WCSmallLocal() throws Exception {
        String inpath = "Data/output/wiki/Stems/loc_0";
        String outpath = "Data/output/wiki/wordCount/loc_0";
        Configuration conf = new Configuration();
        makeJobLocal(conf);
        WordCount.WCDriver driver = new WordCount.WCDriver();
        driver.setConf(conf);

//        conf.setBoolean("mapred.task.profile", true);
//
//        conf.setBoolean("mapred.task.profile", true);
//        conf.set("mapred.task.profile.params", "-agentlib:hprof=cpu=samples,heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s");
//        conf.set("mapred.task.profile.maps", "0-2");
//        conf.set("mapred.task.profile.reduces", ""); // no reduces

        Path outDir = new Path(outpath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outDir)){
            fs.delete(outDir, true);
        }

        assertEquals(0, driver.run(new String[]{inpath, outpath}));
    }


    @Test
    public void WCSmallCluster() throws Exception {
        String inpath = "Data/output/wiki/Stems/loc_0";
        String outpath = "Data/output/wiki/wordCount/loc_0";
        Configuration conf = new Configuration();
        makeJobCluster(conf);
        WordCount.WCDriver driver = new WordCount.WCDriver();
        driver.setConf(conf);

        Path outDir = new Path(outpath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outDir)){
            fs.delete(outDir, true);
        }

        assertEquals(0, driver.run(new String[]{inpath, outpath}));
    }

    @Test
    public void WCFullCluster() throws Exception {
        String inpath = "Data/output/wiki/Stems/full";
        String outpath = "Data/output/wiki/wordCount/full";
        Configuration conf = new Configuration();
        makeJobCluster(conf);
        WordCount.WCDriver driver = new WordCount.WCDriver();
        driver.setConf(conf);

        Path outDir = new Path(outpath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outDir)){
            fs.delete(outDir, true);
        }


        long sz = (new File(TestConfig.loc_wiki_full).length() / 6) + 1;
        conf.set("mapred.min.split.size", Long.toString(sz));
        conf.set("mapred.reduce.tasks", "12");
        conf.set("mapred.map.max.attempts", "1");
        conf.set("mapreduce.map.speculative", "false");
        conf.set("mapreduce.reduce.speculative", "false");

        assertEquals(0, driver.run(new String[]{inpath, outpath}));
    }

    static class WordCacheTestIO implements MapContext<Text, Text, Text, IntWritable> {
        HashMap<String, Integer> result = new HashMap<String, Integer>();

        @Override
        public InputSplit getInputSplit() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void write(Text key, IntWritable value) throws IOException, InterruptedException {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public OutputCommitter getOutputCommitter() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public TaskAttemptID getTaskAttemptID() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void setStatus(String msg) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getStatus() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public float getProgress() {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Counter getCounter(Enum<?> counterName) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Counter getCounter(String groupName, String counterName) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Configuration getConfiguration() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Credentials getCredentials() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public JobID getJobID() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public int getNumReduceTasks() {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Path getWorkingDirectory() throws IOException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Class<?> getOutputKeyClass() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Class<?> getOutputValueClass() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Class<?> getMapOutputKeyClass() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Class<?> getMapOutputValueClass() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getJobName() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public boolean userClassesTakesPrecedence() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public RawComparator<?> getSortComparator() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getJar() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public RawComparator<?> getGroupingComparator() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public boolean getJobSetupCleanupNeeded() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public boolean getTaskCleanupNeeded() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public boolean getProfileEnabled() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getProfileParams() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getUser() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public boolean getSymlink() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Path[] getArchiveClassPaths() {
            return new Path[0];  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public URI[] getCacheArchives() throws IOException {
            return new URI[0];  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public URI[] getCacheFiles() throws IOException {
            return new URI[0];  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Path[] getLocalCacheArchives() throws IOException {
            return new Path[0];  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Path[] getLocalCacheFiles() throws IOException {
            return new Path[0];  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Path[] getFileClassPaths() {
            return new Path[0];  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String[] getArchiveTimestamps() {
            return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String[] getFileTimestamps() {
            return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public int getMaxMapAttempts() {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public int getMaxReduceAttempts() {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void progress() {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }
    @Test
    public void testWordCache() throws IOException, InterruptedException {
        WordCount.WordCache wc;
        wc = new WordCount.WordCache(null, 1<<3, 15);

        wc.incWordCount("stu".getBytes(), 0, 3);
        assertEquals(1, wc.getWordCount("stu".getBytes(), 0, 3));
        wc.incWordCount("stu".getBytes(), 0, 3);
        assertEquals(2, wc.getWordCount("stu".getBytes(), 0, 3));
        wc.incWordCount("cab".getBytes(), 0, 3);//stu and cab has hash-code (wc.hashCode1) = 4

        byte[] text = ( "abcdefghijklmnopqrstuvxyz" +
                        "abcdefghijklmnopqrstuvxyz" +
                        "abcdefghijklmnopqrstuvxyz" +
                        "abcdefghijklmnopqrstuvxyz" +
                        "abc").getBytes();

        WordCacheTestIO context = new WordCacheTestIO(){
            @Override
            public void write(Text key, IntWritable value) throws IOException, InterruptedException {
                Integer val = result.get(key.toString());
                if (val == null){
                    val = 0;
                }
                result.put(key.toString(), val+1);
            }
        };


        wc = new WordCount.WordCache(context, 1<<3, 15);

        for(int i = 0; i < text.length; i++){
            wc.incWordCount(text, i, i+1);
        }

        wc.flush();

        assertEquals(context.result.size(), "abcdefghijklmnopqrstuvxyz".length());
        for(char c: "defghijklmnopqrstuvxyz".toCharArray()){
            assertEquals(context.result.get("" + c), (Integer) 4);
        }
        for(char c: "abc".toCharArray()){
            assertEquals(context.result.get("" + c), (Integer) 5);
        }



    }


}
