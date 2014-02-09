package wiki;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: yevgen
 * Date: 2/6/14
 * Time: 12:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class XmlInputFormatTest {
    @Test
    public void testRecordReader() throws IOException {
        XmlInputFormat.XMLLineRecordReader rr = new XmlInputFormat.XMLLineRecordReader("<page>", "</page>");

        Text txt = new Text();
        txt.set("some text <page> bla-bla");
        rr.truncateValue(txt);
        assertEquals(" bla-bla", txt.toString());

        txt.set("<page>  <page>bla-bla");
        rr.truncateValue(txt);
        assertEquals("  <page>bla-bla", txt.toString());

        txt.set("bla-bla");
        rr.truncateValue(txt);
        assertEquals("", txt.toString());

        txt.set("bla-bla<page>");
        rr.truncateValue(txt);
        assertEquals("", txt.toString());

        txt.set("bla-bla<page><page>");
        rr.truncateValue(txt);
        assertEquals("<page>", txt.toString());

    }
}
