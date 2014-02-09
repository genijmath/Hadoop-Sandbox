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

import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

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
