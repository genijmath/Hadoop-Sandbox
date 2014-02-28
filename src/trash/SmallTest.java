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


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.junit.Test;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.tartarus.snowball.ext.PorterStemmer;

import java.io.*;
import java.util.Scanner;


public class SmallTest {
    @Test
    public void testStandardAnalyzer() throws Exception {
        Analyzer analyzer = new StandardAnalyzer(Version.valueOf("LUCENE_44"));
        TokenStream ts = analyzer.tokenStream("myfield", new StringReader("some text goes to here by Yevgen Yampolskiy"));

        //OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
        CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
        PorterStemmer stem = new PorterStemmer();

        try {
            ts.reset(); // Resets this stream to the beginning. (Required)
            while (ts.incrementToken()) {
                stem.setCurrent(term.buffer(), term.length());
                stem.stem();
                System.out.println(stem.getCurrent());
                //System.out.println(ts.reflectAsString(true));
                // Use AttributeSource.reflectAsString(boolean)
                // for token stream debugging.
//                System.out.println("token: " + ts.reflectAsString(true));
//
//                System.out.println("token start offset: " + offsetAtt.startOffset());
//                System.out.println("  token end offset: " + offsetAtt.endOffset());
            }
            ts.end();   // Perform end-of-stream operations, e.g. set the final offset.
        } finally {
            ts.close(); // Release resources associated with this stream.
        }
    }

    @Test
    public void wikiPageTokenizer(){
        InputStream is = SmallTest.class.getResourceAsStream("pg1.xml");
        Scanner scanner = new Scanner(is).useDelimiter("\\A");
        String page = scanner.next();

    }

}
