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

/*
Counts the number of <page> .. </page> nodes.
This verifies that the way we read data does not skip any pages

Two ways: scan throw the file, or use map-reduce.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat

import java.io.IOException;
import java.io.InputStream;

public class PageCount {

    /**
     * This class does not take into account possiblility of splits between pages
     */
    static class PageMapperForTextFormatWithBug extends Mapper<LongWritable, Text, NullWritable, IntWritable>{

        int pageCount = 0;
        boolean pageOpened = false;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException{
            pageCount = 0;
        }

        //This approach won't work if split does not contain <page>...</page> pair.
        //So if we use TextInputFormat then results should be corrupted

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //String s = value.toString();
            if (value.getLength() > 10)
                return;



            if (!pageOpened){
                if (value.find("<page>") >= 0){
                    pageOpened = !pageOpened;
                }
            }else{
                if (value.find("</page>") >= 0){
                    pageCount++;
                    pageOpened = !pageOpened;
                }
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException{
            context.write(NullWritable.get(), new IntWritable(pageCount));
        }
    }


    enum BAD_SPLIT{
        LINE_READER_BUG
    }

    enum CLEANUP{
        EXECUTED
    }

    static class PageMapperForTextFormat extends Mapper<LongWritable, Text, NullWritable, IntWritable>{

        int pageCount = 0;
        boolean pageOpened = false;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException{
            pageCount = 0;
        }

        //This approach won't work if split does not contain <page>...</page> pair.
        //So if we use TextInputFormat then results should be corrupted

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.getLength() == 0){
                context.getCounter(BAD_SPLIT.LINE_READER_BUG).increment(1);
                context.getCounter("BAD_DATA_OR_END", context.getTaskAttemptID().toString()).increment(1);
                System.out.println("Empty line?");
                return;
            }
            if (value.find("<page>") < 0){
                System.out.println("PAGE NOT FOUND");
                System.out.println(value);
                context.getCounter("BAD_DATA1", context.getTaskAttemptID().toString()).increment(1);
            }
            if (value.getLength() > 0 && value.find("<page>", 10) >= 0){
                System.out.println("DOUBLE PAGE FOUND");
                System.out.println(value);
                context.getCounter("BAD_DATA2", context.getTaskAttemptID().toString()).increment(1);
            }

            if (value.find("</page>") >= 0){
                System.out.println("CLOSE PAGE FOUND");
                System.out.println(value);
                context.getCounter("BAD_DATA3", context.getTaskAttemptID().toString()).increment(1);
            }

            if (value.getLength() > 0 && value.find("<title>", 30) >= 0){
                System.out.println("EXTRA TITLE FOUND");
                System.out.println(value);
                context.getCounter("BAD_DATA4", context.getTaskAttemptID().toString()).increment(1);
            }

            pageCount++;
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException{
            context.getCounter(CLEANUP.EXECUTED).increment(1);
            context.write(NullWritable.get(), new IntWritable(pageCount));
        }
    }

    static class PageReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable>{
        @Override
        protected void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int count = 0;
            for(IntWritable v: values){
                count += v.get();
            }
            context.write(NullWritable.get(), new IntWritable(count));
        }
    }

    static class PageDriver extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {
            if (args.length != 2){
                System.err.printf("Usage: %s [generic optinos] <input> <output>\n", getClass().getSimpleName());
                ToolRunner.printGenericCommandUsage(System.err);
                return -1;
            }

            Path input = new Path(args[0]);
            Path output = new Path(args[1]);

            Configuration conf = getConf();//new Configuration()
//            FileSystem fs = FileSystem.get(conf);
//            if (fs.exists(output)){
//                fs.delete(output);
//            }

            Job job = Job.getInstance(conf, "Page Count");
            job.addFileToClassPath(new Path("file:///mnt/disk0/yevgen/Projects/Hadoop2/out/artifacts/Hadoop2_jar/Hadoop2.jar"));

            job.setMapperClass(PageMapperForTextFormat.class);
            job.setReducerClass(PageReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(IntWritable.class);

            String className = conf.get(MRJobConfig.INPUT_FORMAT_CLASS_ATTR);

            if (className instanceof String){
                Class<InputFormat> cls = (Class<InputFormat>) Class.forName(className);
                job.setInputFormatClass(cls);
//                conf.setClass(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, cls, InputFormat.class);
            }

            //job.setInputFormatClass(TextInputFormat.class);

            job.setOutputFormatClass(TextOutputFormat.class);

            TextInputFormat.addInputPath(job, input);
            TextOutputFormat.setOutputPath(job, output);


            System.out.println("keep.task.files.pattern="+conf.get("keep.task.files.pattern"));

//            for (Map.Entry<String, String> e: conf){
//                System.out.printf("%s=%s\n", e.getKey(), e.getValue());
//            }

            //System.out.println("prop="+conf.get("prop"));
//            return 0;
            return job.waitForCompletion(true) ? 0 : 1;
        }
    }


    static int countPagesDirect(Path filePath) throws IOException{
        Configuration conf = new Configuration();
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec= factory.getCodec(filePath);
        FileSystem fs = FileSystem.getLocal(conf);

        InputStream in = codec.createInputStream(fs.open(filePath));

//        Scanner scanner = new Scanner(in);
//        scanner.useDelimiter("<page>");
//        int count = -1;
//        while(scanner.hasNext()){
//            count++;
//            scanner.next();
//        }

        //This approach is 50% faster than scanner
        int count = 0;
        byte[] tagBytesOpen = "<page>".getBytes();
        byte[] tagBytesClose = "</page>".getBytes();

        byte[] buffer = new byte[0xFFFFF];
        int buf_start = 0;
        long bytes_read = 0;

        byte[] tagBytes = tagBytesOpen;
        boolean search4openTag = true;

        while(true){

            int br = in.read(buffer, buf_start, buffer.length-buf_start);
            for(int s = 0; s < br - tagBytes.length + 1; s++){
                if (buffer[s] == tagBytes[0]){
                    boolean found = true;
                    for(int i = 0; i < tagBytes.length; i++){
                        if (buffer[s+i] != tagBytes[i]){
                            found = false;
                            break;
                        }
                    }
                    if (found){
                        if (search4openTag){
                            search4openTag = false;
                            tagBytes = tagBytesClose;
                        }else{
                            count++;
                            search4openTag = true;
                            tagBytes = tagBytesOpen;
                        }
                    }
                }

            }

            if (br < 0)
                break;
            bytes_read += br;

            //if buffer end with "<pag" then we copy "<pag" to the beginning of the buffer
            //tagBytesClose = tagBytesOpen+1, so we restore biggest one
            buf_start = 0;
            for(int i = br - tagBytesClose.length + 1; i < buffer.length; i++){
                buffer[buf_start++] = buffer[i];
            }
        }

        in.close();

        System.out.println("Bytes read: " + bytes_read);

        return count;
    }




    public static void main(String[] args)
            throws Exception{
        //String srcFile = "/mnt/disk0/yevgen/Projects/Hadoop2/WikiFull/wiki.xml.bz2";
        String srcFile = "/mnt/disk0/yevgen/Projects/Hadoop2/WikiFull/wiki.0.bz2"; //10 million records file
        //String srcFile = "/mnt/disk0/yevgen/Projects/Hadoop2/Test/sample2/wiki_sample.bz2";
        //wiki_sample.bz2


//        long start = System.currentTimeMillis();
//        int count = countPagesDirect(new Path(srcFile));
//        long end = System.currentTimeMillis();
//        System.out.println("#of pages: " + count);//10352       16423ms
//        System.out.println("Runtime: " + (end-start));   //10 mil: 80 Sec,, 64719 pages


        long start = System.currentTimeMillis();
        //ToolRunner.run(new PageDriver(), new String[]{"-Dkeep.task.files.pattern=.*", "file:///"+srcFile, "pageCountDriver2"});
        ToolRunner.run(new PageDriver(), new String[]{"file:///"+srcFile, "pageCountDriver2"});
        long end = System.currentTimeMillis();
        System.out.println("Runtime: " + (end-start));


    }

//  All wiki data single scan
//    #of pages: 13715055 with a bug
//    Runtime: 2378817


//    Runtime: 1093426; #of pages: 13715113

    //RT: 1110081 (incorrect):
}


