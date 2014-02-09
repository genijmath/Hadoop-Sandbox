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
Originally I planed using org.apache.mahout.text.wikipedia.XmlInputFormat but it does not support compression
Using TextInputFormat for parsing wiki XML appeared to be the best approach
 */

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yevgen
 * Date: 2/2/14
 * Time: 12:56 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class XmlInputFormat extends TextInputFormat {

    private static final Logger log = LoggerFactory.getLogger(XmlInputFormat.class);

    public static final String START_TAG_KEY = "xmlinput.start";
    public static final String END_TAG_KEY = "xmlinput.end";
    private String startTag;
    private String endTag;

    public static void setStartTag(String startTag, Configuration conf){
        conf.set(START_TAG_KEY, startTag);
    }

    public static void setEndTag(String endTag, Configuration conf){
        conf.set(END_TAG_KEY, endTag);
    }

    @Override
    public RecordReader<LongWritable, Text>
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
        Configuration conf = context.getConfiguration();
        String startTag = conf.get(START_TAG_KEY);
        String endTag = conf.get(END_TAG_KEY);
        if (startTag == null)
            throw new RuntimeException(String.format("start tag is missing (%s)", START_TAG_KEY));
        if (endTag == null)
            throw new RuntimeException(String.format("start tag is missing (%s)", END_TAG_KEY));
        return new XMLLineRecordReader(startTag, endTag);
    }

    public static class XMLLineRecordReader extends LineRecordReader{
        String startTag;
        XMLLineRecordReader(String startTag, String endTag){
            super(endTag.getBytes(Charsets.UTF_8));
            this.startTag = startTag;
        }

        public boolean nextKeyValue() throws IOException{
            /*
            Removes part of the value before the startTag
            Sets value to empty string if startTag cannot be found
             */

            boolean res = super.nextKeyValue();
            if (res){
                Text value = getCurrentValue();
                truncateValue(value);
            }
            return res;
        }

        void truncateValue(Text value){
            int start = value.find(startTag);
            if (start < 0){
                value.set("");
            }else{
                start += startTag.length();
                byte[] val = value.getBytes();
                value.set(val, start, value.getLength() - start);
            }
        }
    }






}
