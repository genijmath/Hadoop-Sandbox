/**
 * Copyright 2014 Yevgen Yampolskiy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wiki;

/*!!!


THIS IS MY FIRST - VERY DIRTY - APPROACH
CLEANER TEST SETUP CAN BE FOUND IN OTHER TESTS

I DO NOT WANT TO DUMP THIS FILE AT THIS MOMENT, BUT PROBABLY WILL REMOVE IT LATER


!!!!
 */

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
//import org.apache.mahout.text.wikipedia.XmlInputFormat;
//import org.apache.hadoop.streaming.StreamXmlRecordReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class PageCountTest {
    @Test
    public void testCountPagesDirect() throws Exception {
        Path path = new Path("Data/input/wiki/wiki.0.bz2");
        int pc = PageCount.countPagesDirect(path);
        System.out.println(pc);
        assertThat(pc, is(10351));
    }


    @Test
    public void testCountPagesMapRed_TextInputFormat() throws Exception {
        String input = "Data/input/wiki/wiki.0.bz2";
        String output = "Data/output/wiki/0/";
        if (new File(output).exists()){
            org.apache.commons.io.FileUtils.deleteDirectory(new File(output));
        }


        PageCount.PageDriver driver = new PageCount.PageDriver();
        String[] args = String.format(  "-D mapreduce.framework.name=local " +
                                        "-fs file:/// -D hadoop.tmp.dir=/mnt/disk2/tmp3/localRun " +
                                        "-D mapred.job.tracker=local " +
                                        "-D mapreduce.job.inputformat.class=org.apache.hadoop.mapreduce.lib.input.TextInputFormat " +
                                        //"-D mapreduce.job.inputformat.class=org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat " +
                                        "%s  %s", input, output).split(" +");

        ToolRunner.run(driver, args);

        FileSystem fs = FileSystem.get(driver.getConf());

        FSDataInputStream is = fs.open(new Path(output + "/part-r-00000"));

        byte[] o = new byte[1024];
        int o_l = is.read(o);
        String o_r = new String(o, 0, o_l);
        assertThat(o_r, is("10349"));//Lost 2 pages on input splits
    }

//    @Test
//    public void testCountPagesMapRed_XMLFormat() throws Exception {
//        String input = "Data/input/wiki/wiki.0";
//        String output = "Data/output/wiki/0/";
//        if (new File(output).exists()){
//            org.apache.commons.io.FileUtils.deleteDirectory(new File(output));
//        }
//
//        //mahout XmlInputFormat does not support compression
//        //https://github.com/cberzan/hackreduce/blob/master/example/XMLRecordReader.java could be better implementation, need to try
//        PageCount.PageDriver driver = new PageCount.PageDriver();
//        String[] args = ("-D mapreduce.framework.name=local " +
//                "-fs file:/// -D hadoop.tmp.dir=/mnt/disk2/tmp3/localRun " +
//                "-D mapred.job.tracker=local " +
//                //"-D mapreduce.job.inputformat.class=org.apache.hadoop.mapreduce.lib.input.TextInputFormat " +
//                "-D mapreduce.job.inputformat.class=org.apache.mahout.text.wikipedia.XmlInputFormat " +
//                String.format("-D %s=<page> ", XmlInputFormat.START_TAG_KEY) +
//                String.format("-D %s=</page> ", XmlInputFormat.END_TAG_KEY) +
//                String.format("%s  %s", input, output)).split(" +");
//
//        ToolRunner.run(driver, args);
//
//        FileSystem fs = FileSystem.get(driver.getConf());
//
//        FSDataInputStream is = fs.open(new Path(output + "/part-r-00000"));
//
//        byte[] o = new byte[1024];
//        int o_l = is.read(o);
//        String o_r = new String(o, 0, o_l);
//        assertThat(o_r, is("10351"));
//    }

    @Test
    public void testCountPagesMapRed_XMLFormat2() throws Exception {
        String input = "Data/input/wiki/wiki.0.bz2";
        String output = "Data/output/wiki/0/";
        if (new File(output).exists()){
            org.apache.commons.io.FileUtils.deleteDirectory(new File(output));
        }

        PageCount.PageDriver driver = new PageCount.PageDriver();
        String[] args = ("-D mapreduce.framework.name=local " +
                "-fs file:/// -D hadoop.tmp.dir=/mnt/disk2/tmp3/localRun " +
                "-D mapred.job.tracker=local " +
                //"-D mapreduce.job.inputformat.class=org.apache.hadoop.mapreduce.lib.input.TextInputFormat " +
                "-D mapreduce.job.inputformat.class=wiki.XmlInputFormat " +
                String.format("-D %s=<page> ", wiki.XmlInputFormat.START_TAG_KEY) +
                String.format("-D %s=</page> ", wiki.XmlInputFormat.END_TAG_KEY) +
                String.format("%s  %s", input, output)).split(" +");

        ToolRunner.run(driver, args);

        FileSystem fs = FileSystem.get(driver.getConf());

        FSDataInputStream is = fs.open(new Path(output + "/part-r-00000"));

        byte[] o = new byte[1024];
        int o_l = is.read(o);
        String o_r = new String(o, 0, o_l).trim();
        assertThat(o_r, is("10352"));
    }

    @Test
    public void testCountPagesMapRed_XMLFormat2Cluster() throws Exception {
        String input = "Data/input/wiki/wiki.0.bz2";
        String output = "Data/output/wiki/0_cl/";

        Process p;
        p = Runtime.getRuntime().exec("hadoop fs -rmr " + output);
        p.waitFor();


        PageCount.PageDriver driver = new PageCount.PageDriver();
        String[] args = (
                //"-D mapreduce.job.inputformat.class=org.apache.hadoop.mapreduce.lib.input.TextInputFormat " +
                "-libjars /mnt/disk0/yevgen/Projects/HDP/target/Hadoop-1.0-SNAPSHOT.jar " +
                "-D mapreduce.job.inputformat.class=wiki.XmlInputFormat " +
                String.format("-D %s=<page> ", wiki.XmlInputFormat.START_TAG_KEY) +
                String.format("-D %s=</page> ", wiki.XmlInputFormat.END_TAG_KEY) +
                String.format("%s  %s", input, output)).split(" +");

        ToolRunner.run(driver, args);

        FileSystem fs = FileSystem.get(driver.getConf());

        FSDataInputStream is = fs.open(new Path(output + "/part-r-00000"));

        byte[] o = new byte[1024];
        int o_l = is.read(o);
        String o_r = new String(o, 0, o_l).trim();
        assertThat(o_r, is("10352")); //file ends with <page>...EOF so extra 1 record is expected
    }



    @Test
    public void testCountPagesMapRed_XMLFormat2ClusterFull() throws Exception {
        String input = "/home/yevgen/wiki.xml.bz2";
        String output = "Data/output/wiki/full_cl/";
        String inp2 = "file://" + input;

        Process p;
        p = Runtime.getRuntime().exec("hadoop fs -rmr " + output);

        p.waitFor();
        p = Runtime.getRuntime().exec("mvn package  -Dmaven.test.skip=true");
        p.waitFor();

        long sz = (new File(input).length() / 6) + 1;


        PageCount.PageDriver driver = new PageCount.PageDriver();
        String[] args = (
                //"-D mapreduce.job.inputformat.class=org.apache.hadoop.mapreduce.lib.input.TextInputFormat " +
                "-libjars /mnt/disk0/yevgen/Projects/HDP/target/Hadoop-1.0-SNAPSHOT.jar " +
                        "-D mapreduce.job.inputformat.class=wiki.XmlInputFormat " +
                        "-D mapred.reduce.slowstart.completed.maps=1 " +
//                        "-D mapred.min.split.size="+ sz + " " +
//                        "-D mapred.task.profile=true " +
//                        "-D mapred.task.profile.params=-agentlib:hprof=cpu=samples,heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s " +
//                        "-D mapred.task.profile=0 " +
//                        "-D mapred.task.profile.reducers= " +
                        String.format("-D %s=<page> ", wiki.XmlInputFormat.START_TAG_KEY) +
                        String.format("-D %s=</page> ", wiki.XmlInputFormat.END_TAG_KEY) +
                        String.format("%s  %s", inp2, output)).split(" +");



        long start = System.currentTimeMillis();
        ToolRunner.run(driver, args);
        long end = System.currentTimeMillis();
        System.out.println("Runtime: " + (end - start));

        FileSystem fs = FileSystem.get(driver.getConf());

        FSDataInputStream is = fs.open(new Path(output + "/part-r-00000"));

        byte[] o = new byte[1024];
        int o_l = is.read(o);
        String o_r = new String(o, 0, o_l).trim();
        assertThat(o_r, is("13715113"));//13711702 in   1040 seconds
        //13715111 in 1327
        //correct: 13715113

    }

}


//Running job: job_1390710842182_0041
//
//Total committed heap usage (bytes)=30434197504
//        BAD_DATA
//        attempt_1390710842182_0041_m_000150_0=1
//        MY_GROUP
//        MY_NAME=4530
//        Shuffle Errors
//        BAD_ID=0
//        CONNECTION=0
//        IO_ERROR=0
//        WRONG_LENGTH=0
//        WRONG_MAP=0
//        WRONG_REDUCE=0
//        File Input Format Counters
//        Bytes Read=10130105631
//        File Output Format Counters
//        Bytes Written=9
//        wiki.PageCount$BAD_SPLIT
//        LINE_READER_BUG=1
//        wiki.PageCount$CLEANUP
//        EXECUTED=151
//        Runtime: 1310969
//
//        java.lang.AssertionError:
//        Expected: is "10352"
//        got: "13715111"
//
