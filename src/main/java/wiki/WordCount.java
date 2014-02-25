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
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
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

    static class WordCache{
        //HashMap boxing creates overhead
        //What is implemented here is an open addressing hash table using double hashing
        //see "Introduction to Algorithms", (Chapter: Hash Tables)
        //Some adjustment was made so that hash never was getting into an overflow situation:
        //If hash table is 75% full then 10% of elements that were not recently used
        //are written to 'context' and removed

        final int CAP;//hold CAP words in-memory, must be power of 2 (open addressing hash used)
        final int MAX_WORD_LEN_PLUS_1;//words longer than MAX_WORD_LEN_PLUS_1-1 won't be cached; 0 at the end of the word!
        final int CAP_M;//=CAP*MAX_WORD_LEN_PLUS_1

        byte[] words;  //new byte[CAP * MAX_WORD_LEN_PLUS_1]
        int[] counts; //how many time word was observed
        long[] lastUsed;//when this word count was updated the last time
        long[] buffer;//temp buffer for sorting
        long lastUsedSeq = 0;//time-line for lastUsed
        int hashLoad = 0; //how many elements are in the hash table

        Text key = new Text();
        IntWritable value = new IntWritable();

        MapContext<Text, Text, Text, IntWritable> context = null;

        WordCache(MapContext<Text, Text, Text, IntWritable> context){
            this(context, 1<<20, 15);
        }

        WordCache(MapContext<Text, Text, Text, IntWritable> context, int CAP, int MAX_WORD_LEN_PLUS_1){
            this.CAP = CAP;
            this.MAX_WORD_LEN_PLUS_1 = MAX_WORD_LEN_PLUS_1;
            this.CAP_M = CAP * MAX_WORD_LEN_PLUS_1;

            words = new byte[CAP * MAX_WORD_LEN_PLUS_1];
            counts = new int[CAP];
            lastUsed = new long[CAP];
            buffer = new long[CAP];//temp buffer for sorting

            this.context = context;
            Arrays.fill(words, (byte) 'x');//zero is used to detect end-of-the word, see 'same word' implementation
            for(int i = 0; i < words.length; i += MAX_WORD_LEN_PLUS_1)
                words[i] = 0;
        }


        void incWordCount(byte[] text, int start, int end) throws IOException, InterruptedException {
            //increases word count by 1, and writes part of the hash to context if hash is getting too loaded

            assert end - start < MAX_WORD_LEN_PLUS_1 : "Word too long!";

            int h1 = hashCode1(text, start, end) % CAP;
            int h2 = hashCode2(text, start, end) % CAP;


            int count = 0;

            int pos = h1 * MAX_WORD_LEN_PLUS_1;
            int h2M = h2 * MAX_WORD_LEN_PLUS_1;
            final int CAP_M = CAP * MAX_WORD_LEN_PLUS_1;
            int iPos = h1;

            do{
                if (words[pos] == 0){//empty slot
                    for(int i = start; i < end; i++){
                        words[pos + i -start] = text[i];
                    }
                    words[pos + end-start] = 0;
                    counts[iPos] = 1;
                    hashLoad++;
                    lastUsed[iPos] = lastUsedSeq++;
                    return;
                }else{
                    //same words?
                    if (words[pos + end-start] == 0){//this is why settings words[..]='x' is important
                        boolean same = true;
                        for(int i = start; i < end; i++){
                            if (words[pos + i-start] != text[i]){
                                same = false;
                                break;
                            }
                        }
                        if (same){
                            counts[iPos] += 1;
                            lastUsed[iPos] = lastUsedSeq++;
                            return;
                        }
                    }
                }

                pos = (pos + h2M) % CAP_M;//Try next slot
                iPos = (iPos + h2) % CAP;
                count++;


                if (4*hashLoad >= 3*CAP){//75% full
                    //remove 10% of elements which were not used for a long time
                    int p = 0;
                    for(int i = 0, k = 0; i < words.length; i+=MAX_WORD_LEN_PLUS_1, k++){
                        if (words[i] != 0){
                            buffer[p++] = lastUsed[k];
                        }
                    }
                    Arrays.sort(buffer, 0, p);
                    long cutoff = buffer[p/10];

                    flush(cutoff);

                    //reset
                    count = 0;
                    pos = h1 * MAX_WORD_LEN_PLUS_1;
                    iPos = h1;
                }
            } while(count < CAP);
            throw new RuntimeException("Should not be here");
        }

        void flush() throws  IOException, InterruptedException{
            flush(Long.MAX_VALUE);
        }

        void flush(long cutoff) throws IOException, InterruptedException {
            for(int i = 0, k = 0; i < words.length; i+=MAX_WORD_LEN_PLUS_1, k++){
                if (words[i] != 0  && lastUsed[k] <= cutoff){
                    int wordEnd = i;
                    while(words[wordEnd] != 0) wordEnd++;
                    words[wordEnd] = 'x';
                    key.set(words, i, wordEnd - i);
                    value.set(counts[k]);
                    context.write(key, value);
                    words[i] = 0;//remove word
                    hashLoad--;
                }
            }

        }

        int getWordCount(byte[] text, int start, int end){
            assert end - start < MAX_WORD_LEN_PLUS_1 : "Word too long!";

            int h1 = hashCode1(text, start, end) % CAP;
            int h2 = hashCode2(text, start, end) % CAP;


            int count = 0;
            int pos = h1 * MAX_WORD_LEN_PLUS_1;
            int h2M = h2 * MAX_WORD_LEN_PLUS_1;

            int iPos = h1;

            do{
                //same words?
                if (words[pos+end-start] == 0){
                    boolean same = true;
                    for(int i = start; i < end; i++){
                        if (words[pos + i-start] != text[i]){
                            same = false;
                            break;
                        }
                    }
                    if (same){
                        return counts[iPos];
                    }
                }

                pos = (pos + h2M) % CAP_M;//Try next slot
                iPos = (iPos + h2) % CAP;
                count++;

            } while(count < CAP);
            return -1;
        }

        int hashCode1(byte[] text, int start, int end){
            int hashCode = 0;
            for(int i = start; i < end; i++){
                hashCode = hashCode * 31 + text[i];
            }
            return hashCode & Integer.MAX_VALUE; //non-negative
        }

        int hashCode2(byte[] text, int start, int end){
            int hashCode = 0;
            for(int i = start; i < end; i++){
                hashCode = hashCode * 29 + text[i];
            }
            return (hashCode | 1) & Integer.MAX_VALUE; //odd positive
        }

    }

    static class WCMapper extends Mapper<Text, Text, Text, IntWritable> {
        WordCache wc;
        Text key = new Text();
        IntWritable value = new IntWritable();

        protected void setup(Context context
        ) throws IOException, InterruptedException {
            wc = new WordCache(context);
        }

        protected void cleanup(Context context
        ) throws IOException, InterruptedException {
            wc.flush();
        }


        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            byte[] text = value.getBytes();
            int len = value.getLength();
            int start = 0;
            while(true){
                while(start < len && text[start] == ' ')
                    start++;

                if (start == len){
                    return;
                }

                int end = start;
                while(end < len && text[end] != ' ')
                    end++;

                if (end - start >= wc.MAX_WORD_LEN_PLUS_1){//word is too long -- just write it out
                    this.key.set(text, start, end - start);
                    this.value.set(1);
                    context.write(this.key, this.value);
                }else{
                    wc.incWordCount(text, start, end); //add to hash
                }
                start = end;
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
