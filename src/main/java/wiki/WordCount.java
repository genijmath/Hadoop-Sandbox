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

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.tartarus.snowball.ext.PorterStemmer;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;


public class WordCount {
    static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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


        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            org.apache.lucene.analysis.Analyzer analyzer = new StandardAnalyzer(Version.valueOf("LUCENE_44"));
            TokenStream ts = analyzer.tokenStream("myfield",
                    new StringReader(value.toString()));

            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            PorterStemmer stem = new PorterStemmer();

            try {
                ts.reset(); // Resets this stream to the beginning. (Required)
                while (ts.incrementToken()) {
                    stem.setCurrent(term.buffer(), term.length());
                    stem.stem();
                    String word = stem.getCurrent();
                    Integer cnt = wc.get(word);
                    if (cnt == null){
                        cnt = 0;
                    }
                    wc.put(word, cnt + 1);

                    manageWC(context);
                }
                ts.end();   // Perform end-of-stream operations, e.g. set the final offset.
            } finally {
                ts.close(); // Release resources associated with this stream.
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
}
