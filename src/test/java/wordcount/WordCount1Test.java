package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WordCount1Test {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordCount1.WCMapper mapper = new WordCount1.WCMapper();
        WordCount1.WCReducer reducer = new WordCount1.WCReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void basicValid() throws IOException {
        Text value = new Text("a bbb c bbb a");
        mapDriver.withInput(new LongWritable(), value);
        mapDriver.withOutput(new Text("a"), new IntWritable(1));
        mapDriver.withOutput(new Text("bbb"), new IntWritable(1));
        mapDriver.withOutput(new Text("c"), new IntWritable(1));
        mapDriver.withOutput(new Text("bbb"), new IntWritable(1));
        mapDriver.withOutput(new Text("a"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void basicReduce() throws IOException{
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("a"), values);
        reduceDriver.withOutput(new Text("a"), new IntWritable(3));
        reduceDriver.runTest();
    }

    @Test
    public void integrationTest() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("a bb a bb c"));
        mapReduceDriver.withInput(new LongWritable(), new Text("a c bb c c"));
        mapReduceDriver.withOutput(new Text("a"), new IntWritable(3));
        mapReduceDriver.withOutput(new Text("bb"), new IntWritable(3));
        mapReduceDriver.withOutput(new Text("c"), new IntWritable(4))
                .runTest();
        //mapReduceDriver.run();
    }

    @Test
    public void localIntegrationTest() throws Exception{


        String input = "Data/input/wc";
        String output = "Data/output/wc";

        File out = new File(output);
        if (out.exists()){
            org.apache.commons.io.FileUtils.deleteDirectory(out);
        }

        WordCount1.WCDriver driver = new WordCount1.WCDriver();
        //-D mapreduce.jobtracker.address=localhost:8021
        String[] args = String.format("-D mapreduce.framework.name=local -fs file:/// -D hadoop.tmp.dir=/mnt/disk2/tmp3/localRun -D mapred.job.tracker=local %s  %s", input.toString(), output.toString()).split(" +");
        System.out.println(Arrays.toString(args));
        int exitCode = ToolRunner.run(driver, args);
        assertThat(exitCode, is(0));
    }

    @Test
    public void localIntegrationTest2() throws Exception{
        Configuration conf = new Configuration(true);
        conf.setQuietMode(false);

        FileSystem fs = FileSystem.get(conf);



        Path input = new Path("Data/input/wc");
        Path output = new Path("Data/output/wc");

        if (!fs.exists(input)){
            fs.mkdirs(input);

            FSDataOutputStream os = fs.create(new Path(input.toString() + "/sample.txt"));
            os.write("a b c\nd e f\ng h\n".getBytes());
        }

        if (fs.exists(output)){
            fs.delete(output, true);
        }

//        FileSystem fs = FileSystem.getLocal(conf);
//        fs.delete(output, true);

        WordCount1.WCDriver driver = new WordCount1.WCDriver();
        //driver.setConf(conf);

        //-D mapreduce.jobtracker.address=localhost:8021
        //-D hadoop.tmp.dir=/mnt/disk2/tmp3/localRun
        String[] args = String.format("-libjars target/Hadoop-1.0-SNAPSHOT.jar %s  %s", input.toString(), output.toString()).split(" +");//-conf /etc/hadoop/conf/mapred-site.xml
        System.out.println(Arrays.toString(args));
        int exitCode = ToolRunner.run(driver, args);
        assertThat(exitCode, is(0));
    }
}
