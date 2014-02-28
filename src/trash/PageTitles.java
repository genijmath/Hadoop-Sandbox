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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class PageTitles {


    static class PTMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
        int missedTitles = 0;
        int missedTitles2 = 0;
        int missedPages = 0;
        int pagesTotal = 0;
        long total = 0;
        Text out = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
            if (value.find("<page>") < 0){
                missedPages++;
            }else{
                pagesTotal++;
            }
            int start = value.find("<title>");
            if (start < 0){
                missedTitles++;
                return;
            }
            start += "<title>".length();
            int end = value.find("</title>", start);
            if (end < 0){
                missedTitles++;
                missedTitles2++;
                return;
            }

            if (value.find("<title>", end) >= 0){
                //ERROR cannot be two titles!
                context.getCounter("DOUBLE_TITLES", context.getTaskAttemptID().toString()).increment(missedTitles);
                System.out.println(value);
                System.out.print(value);
            }
            total++;
            out.set(value.getBytes(), start, end-start);
            context.write(out, NullWritable.get());
        }

        public void cleanup(Context context){
            context.getCounter("MissedTitles", "Type1").increment(missedTitles);
            context.getCounter("MissedTitles", "Type2").increment(missedTitles2);
            context.getCounter("TotalTitles", "total").increment(total);
            context.getCounter("Pages", "Missed").increment(missedPages);
            context.getCounter("Pages", "Total").increment(pagesTotal);
        }
    }

    static class PTDriver extends Configured implements Tool {
        long getPagesTotal() {
            return pagesTotal;
        }

        long getPagesMissed() {
            return pagesMissed;
        }

        @Override
        public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            //Output directory is removed automatically!

            if (args.length != 2){
                System.out.println("Two arguments are expected, received: " + Arrays.toString(args));
                ToolRunner.printGenericCommandUsage(System.out);
                return 1;
            }
            Job job = Job.getInstance(getConf());
            job.setJobName("PageTitle");
            job.setMapperClass(PTMapper.class);
            job.setJarByClass(PageTitles.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.setInputFormatClass(XmlInputFormat.class);
            XmlInputFormat.addInputPath(job, new Path(args[0]));

            Path outDir = new Path(args[1]);
            TextOutputFormat.setOutputPath(job, outDir);
            FileSystem fs = FileSystem.get(getConf());
            if (fs.exists(outDir)){
                fs.delete(outDir, true);
            }

            boolean success = job.waitForCompletion(true);
            pagesTotal = job.getCounters().findCounter("Pages", "Total").getValue();
            pagesMissed = job.getCounters().findCounter("Pages", "Missed").getValue();
            return (success) ? 0 : 1;
        }

        private long pagesTotal;
        private long pagesMissed;
    }

    public static void main(String[] args)
            throws Exception{
        ToolRunner.run(new PTDriver(), new String[]{"file:///" + args[0], args[1]});
    }


}
