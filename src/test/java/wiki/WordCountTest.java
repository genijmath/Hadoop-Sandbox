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
        mapDriver.withOutput(new Text("pig"), new IntWritable(1));
        mapDriver.withOutput(new Text("the"), new IntWritable(1));
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
            return null;  
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return false;  
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return null;  
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return null;  
        }

        @Override
        public void write(Text key, IntWritable value) throws IOException, InterruptedException {
            
        }

        @Override
        public OutputCommitter getOutputCommitter() {
            return null;  
        }

        @Override
        public TaskAttemptID getTaskAttemptID() {
            return null;  
        }

        @Override
        public void setStatus(String msg) {
            
        }

        @Override
        public String getStatus() {
            return null;  
        }

        @Override
        public float getProgress() {
            return 0;  
        }

        @Override
        public Counter getCounter(Enum<?> counterName) {
            return null;  
        }

        @Override
        public Counter getCounter(String groupName, String counterName) {
            return null;  
        }

        @Override
        public Configuration getConfiguration() {
            return null;  
        }

        @Override
        public Credentials getCredentials() {
            return null;  
        }

        @Override
        public JobID getJobID() {
            return null;  
        }

        @Override
        public int getNumReduceTasks() {
            return 0;  
        }

        @Override
        public Path getWorkingDirectory() throws IOException {
            return null;  
        }

        @Override
        public Class<?> getOutputKeyClass() {
            return null;  
        }

        @Override
        public Class<?> getOutputValueClass() {
            return null;  
        }

        @Override
        public Class<?> getMapOutputKeyClass() {
            return null;  
        }

        @Override
        public Class<?> getMapOutputValueClass() {
            return null;  
        }

        @Override
        public String getJobName() {
            return null;  
        }

        @Override
        public boolean userClassesTakesPrecedence() {
            return false;  
        }

        @Override
        public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
            return null;  
        }

        @Override
        public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
            return null;  
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
            return null;  
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
            return null;  
        }

        @Override
        public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
            return null;  
        }

        @Override
        public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
            return null;  
        }

        @Override
        public RawComparator<?> getSortComparator() {
            return null;  
        }

        @Override
        public String getJar() {
            return null;  
        }

        @Override
        public RawComparator<?> getGroupingComparator() {
            return null;  
        }

        @Override
        public boolean getJobSetupCleanupNeeded() {
            return false;  
        }

        @Override
        public boolean getTaskCleanupNeeded() {
            return false;  
        }

        @Override
        public boolean getProfileEnabled() {
            return false;  
        }

        @Override
        public String getProfileParams() {
            return null;  
        }

        @Override
        public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
            return null;  
        }

        @Override
        public String getUser() {
            return null;  
        }

        @Override
        public boolean getSymlink() {
            return false;  
        }

        @Override
        public Path[] getArchiveClassPaths() {
            return new Path[0];  
        }

        @Override
        public URI[] getCacheArchives() throws IOException {
            return new URI[0];  
        }

        @Override
        public URI[] getCacheFiles() throws IOException {
            return new URI[0];  
        }

        @Override
        public Path[] getLocalCacheArchives() throws IOException {
            return new Path[0];  
        }

        @Override
        public Path[] getLocalCacheFiles() throws IOException {
            return new Path[0];  
        }

        @Override
        public Path[] getFileClassPaths() {
            return new Path[0];  
        }

        @Override
        public String[] getArchiveTimestamps() {
            return new String[0];  
        }

        @Override
        public String[] getFileTimestamps() {
            return new String[0];  
        }

        @Override
        public int getMaxMapAttempts() {
            return 0;  
        }

        @Override
        public int getMaxReduceAttempts() {
            return 0;  
        }

        @Override
        public void progress() {
            
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
