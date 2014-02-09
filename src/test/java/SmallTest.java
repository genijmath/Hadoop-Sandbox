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

/**
 * Created with IntelliJ IDEA.
 * User: yevgen
 * Date: 2/5/14
 * Time: 11:35 PM
 * To change this template use File | Settings | File Templates.
 */
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
