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
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import java.io.File;
import java.util.*;
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
                "little pig was running across the street little by little"));
        mapDriver.withOutput(new Text("littl"), new IntWritable(3));
        mapDriver.withOutput(new Text("street"), new IntWritable(1));
        mapDriver.withOutput(new Text("run"), new IntWritable(1));
        mapDriver.withOutput(new Text("across"), new IntWritable(1));
        mapDriver.withOutput(new Text("pig"), new IntWritable(1));
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


}
