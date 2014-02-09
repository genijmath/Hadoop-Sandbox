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


package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.PrintWriter;
import java.util.*;

import org.apache.hadoop.mapreduce.MRJobConfig;

import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;




public class WordCount1 {
    static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer st = new StringTokenizer(line);
            while (st.hasMoreTokens()) {
                String wd = st.nextToken();
                context.write(new Text(wd), new IntWritable(1));
            }
        }
    }

    static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i: values){
                sum += i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    static class WCDriver extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {

            System.out.println("Arguments: " + Arrays.toString(args));

            if (args.length != 2){
                System.err.printf("Usage: %s [generic optinos] <input> <output>\n", getClass().getSimpleName());
                ToolRunner.printGenericCommandUsage(System.err);
                return -1;
            }

            //conf.setQuietMode(false);
            //Configuration.dumpConfiguration(conf, new PrintWriter(System.out));


            Job job = Job.getInstance(getConf(), "Word Count");

            //job.setJarByClass(getClass());

            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setMapperClass(WCMapper.class);
            job.setReducerClass(WCReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

//            ArrayList<String> settings = new ArrayList<String>();
//            for(Map.Entry<String, String> e: getConf()){
//                settings.add(String.format("%s=%s\n", e.getKey(), e.getValue()));
//            }
//            Collections.sort(settings);
//            for(String s: settings){
//                System.out.print(s);
//            }


            //job.submit();
            return  job.waitForCompletion(true) ? 0 : 1;

            //return 0;
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration(true);
        conf.setQuietMode(false);


        WCDriver driver = new WCDriver();
        driver.setConf(conf);

        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);


//        Configuration conf = new Configuration();
//        Configuration.dumpConfiguration(conf, new PrintWriter(System.out));

//        for (Map.Entry<String, String> entry: conf){
//            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
//        }

    }
}