package wiki;

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


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Collection of classes for pre-processing
 */
public class ETL {
    /**
     * Input wiki .bzip2 file is converted into a sequence file of cleaned pages
     * Title->page content
     * gzip compression
     */


    static class Import{


        static class CleanupMapper extends Mapper<LongWritable, Text, Text, Text>{
            Text title = new Text();
            Text writeValue = new Text();



            @Override
            protected void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {

                if (value == null){
                    System.out.println("NULL VALUE? unexpected");
                    throw new RuntimeException("NULL VALUE? unexpected");
                }

                if (!extractTitle(value))
                    return;

                int len = value.getLength();
                len = Analyzer.cleanupPage(value.getBytes(), len, true);
                if (len <= 0) return;
                len = Analyzer.cleanNonWords(value.getBytes(), len);
                if (len <= 0) return;
                writeValue.set(value.getBytes(), 0, len);
                context.write(title, writeValue);
            }

            boolean extractTitle(Text txt){
                int s = txt.find("<title>");
                if (s < 0) return false;
                int e = txt.find("</title>", s);
                if (e < 0) return false;
                s += "<title>".length();
                title.set(txt.getBytes(), s, e-s);
                return true;
            }
        }

        static class Driver extends Configured implements Tool {

            @Override
            public int run(String[] args) throws Exception {

                if (args.length != 2){
                    System.out.println("Usage: <input> <output>");
                    ToolRunner.printGenericCommandUsage(System.out);
                    System.exit(2);
                }

                Job job = Job.getInstance(getConf());
                job.setJobName("Convert wiki .bz2 to gzip sequence file");
                job.setJarByClass(ETL.class);
                job.setInputFormatClass(XmlInputFormat.class);
                job.setMapperClass(CleanupMapper.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                XmlInputFormat.addInputPath(job, new Path(args[0]));

//                TextOutputFormat.setOutputPath(job, new Path(args[1]));
                job.setOutputFormatClass(SequenceFileOutputFormat.class);
                SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

                SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
                //SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
                SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
                boolean rc = job.waitForCompletion(true);
                return (rc) ? 0 : 1;
            }
        }
    }
}
