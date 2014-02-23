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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;


public class WordCount {

    static class WCMapper extends Mapper<Text, Text, Text, IntWritable> {
        Map<String, Integer> wc = new HashMap<String, Integer>();
        Text key = new Text();
        IntWritable value = new IntWritable();
        int lim=3;
        final int CAP = 1000000;

        protected void setup(Context context
        ) throws IOException, InterruptedException {

        }

        protected void cleanup(Context context
        ) throws IOException, InterruptedException {
            for(Map.Entry<String, Integer> e: wc.entrySet()){
                key.set(e.getKey());
                value.set(e.getValue());
                context.write(key, value);
            }
        }


        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), " ", false);
            while (tokenizer.hasMoreTokens()){
                String word = tokenizer.nextToken();
                Integer cnt = wc.get(word);
                if (cnt == null){
                    cnt = 0;
                }
                wc.put(word, cnt + 1);

                manageWC(context);
            }
        }

        void manageWC(Context context) throws IOException, InterruptedException {

            int count = 0;
            while (wc.size() > CAP){
                Map<String, Integer> keep = new HashMap<String, Integer>();
                for(Map.Entry<String, Integer> e: wc.entrySet()){
                    if (e.getValue() < lim){
                        key.set(e.getKey());
                        value.set(e.getValue());
                        context.write(key, value);
                        count++;
                    }else{
                        keep.put(e.getKey(), e.getValue());
                    }
                }
                wc = keep;
                if (wc.size() > CAP ||
                        (10 * count < CAP) //too little output
                        )
                    lim++;
            }
        }
    }

    static class WCCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
        IntWritable value = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            value.set(sum);
            context.write(key, value);
        }
    }

    static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        IntWritable value = new IntWritable();
        Text key = new Text();
        final int N_FREQ_WRDS = 10000;

        static class WordFrequency implements Comparable<WordFrequency>{
            String word;
            int freq;

            WordFrequency(String word, int freq){
                this.word = word;
                this.freq = freq;
            }

            @Override
            public int compareTo(WordFrequency o) {
                return this.freq - o.freq;
            }
        }

        PriorityQueue<WordFrequency> freq_words = new PriorityQueue<WordFrequency>(100);

        private MultipleOutputs<Text, IntWritable> multipleOutputs;

        @Override
        protected void setup(Context context){
            multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            value.set(sum);
            multipleOutputs.write(key, value, "all-freq");

            if (freq_words.size() < N_FREQ_WRDS){
                freq_words.add(new WordFrequency(key.toString(), sum));
            }else{
                WordFrequency wf = freq_words.peek();
                if (wf.freq < sum){
                    freq_words.poll();
                    freq_words.add(new WordFrequency(key.toString(), sum));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            WordFrequency[] words = new WordFrequency[freq_words.size()];
            int count = 0;
            while(freq_words.size() > 0){
                WordFrequency wf = freq_words.poll();
                words[count++] = wf;
            }
            for(int i = words.length-1; i>=0; i--){
                key.set(words[i].word);
                value.set(words[i].freq);
                multipleOutputs.write(key, value, "max-freq");
            }

            multipleOutputs.close();

        }
    }

    static class WCDriver extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {
            if (args.length != 2){
                System.out.print("Word-count usage: <input> <output>");
                ToolRunner.printGenericCommandUsage(System.out);
                System.exit(1);
            }

            Job job = Job.getInstance(getConf());
            job.setJobName("word count");
            job.setJarByClass(WordCount.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(WCMapper.class);
            job.setCombinerClass(WCCombiner.class);
            job.setReducerClass(WCReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);


            //SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
            FileSystem fs = FileSystem.get(getConf());
            for(FileStatus fstat: fs.globStatus(new Path(args[0], "*-r-*"))){
                SequenceFileInputFormat.addInputPath(job, fstat.getPath());
            }

            TextOutputFormat.setOutputPath(job, new Path(args[1]));
            TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            //TextOutputFormat.setCompressOutput(job, true);


            boolean rc = job.waitForCompletion(true);
            return (rc) ? 0 : 1;
        }
    }

}
