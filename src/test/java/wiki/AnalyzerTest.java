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

import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AnalyzerTest {

    @Test
    public void testCleanup(){
        String s = null;
        int len = 0;
        byte[] bs = null;
        String page = null;

        s = "abc";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc", new String(bs, 0, len));

        s = "    *    [[abc   ]]def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("*    [[abc   ]]def", new String(bs, 0, len));

        s = "x   *    [[abc   ]]def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals(s, new String(bs, 0, len));

        s = "x   *    [[abc   ]]def\n*[[ XYZ]]TX";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("x   *    [[abc   ]]def\n*[[ XYZ]]TX", new String(bs, 0, len));

        s = "    *    x{{abc   }}def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("*    xdef", new String(bs, 0, len));

        s = "xy*z{{abc   }}def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("xy*zdef", new String(bs, 0, len));

        s = "x   *    {{abc   }}def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("x   *    def", new String(bs, 0, len));

        s = "x   *    {{abc   }}def\n*{{ XYZ}}TX";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("x   *    def\n*TX", new String(bs, 0, len));

        s = "[[abc:bla]]d"; //remove!
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("d", new String(bs, 0, len));

        s = "[http://bla]"; //remove!
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("", new String(bs, 0, len));


        s = "=abc=\nA\n==  def ===\nB\n  c==C=\n  == jjjj  ==   \nF";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("A\nB\nc==C=\nF", new String(bs, 0, len));


        s = "  {{xxxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("{{xxxx", new String(bs, 0, len));

        s = "  {{xx{{yy}}zz";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("{{xxzz", new String(bs, 0, len));


        s = "  *  [[ x [[ y ]]";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("*  [[ x [[ y ]]", new String(bs, 0, len));

        s = "  *  [[ xy ]]\n[[ab]] x [[cd] {{ rbc }} uv\n==title==\nvdx=rs{tx}{{ttxx}}*[mn]\nrtx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("*  [[ xy ]]\n[[ab]] x [[cd]  uv\nvdx=rs{tx}*[mn]\nrtx", new String(bs, 0, len));


        s = "asfddsaf <text xml:space=\"preserve\">abc";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), true);
        assertEquals("abc", new String(bs, 0, len));

        s = "abc &lt;ref&gt; bla &lt;/ref&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len));

        s = "abc &lt;ref blabla bla /&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len));

        s = "abc &lt;ref blabla&gt; bla &lt;/ref&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len));

        s = "abc &lt;ref blabla&gt; bla &lt;/ref&gt;";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc ", new String(bs, 0, len));

        //ref in ref
        s = "abc &lt;ref blabla&gt; xxx &lt;ref blabla&gt; bla &lt;/ref&gt; rrr &lt;/ref&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len));

        s = "abc &lt;ref blabla&gt; xxx &lt;ref blabla /&gt; rrr &lt;/ref&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len));

        s = "abc &lt;ref blabla&gt; xxx &lt;ref&gt; rrr &lt;/ref&gt; rrr &lt;/ref&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len));

        s = "xxx</text>yyy";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("xxx", new String(bs, 0, len));

        s = "abc&lt;blockquote blabla&gt;XYZ&lt;/blockquote&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc_____________________XYZ___________________xxx", new String(bs, 0, len));


        s = "abc&lt;!-- xxx --&gt;yyy";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abcyyy", new String(bs, 0, len));

        s = "&lt;!-- xxx --&gt;";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("", new String(bs, 0, len));

        s = "{|BLA|}";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("", new String(bs, 0, len));

        s = "{|B{|L|}A|}";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("", new String(bs, 0, len));

        s = "x{|B{|L|}A|}y";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("xy", new String(bs, 0, len));

        s = "abc{{xxx begin|rrs}} bla {{xxx end}}def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abcdef", new String(bs, 0, len));


        page = new Scanner(AnalyzerTest.class.getResourceAsStream("../pg1.xml")).useDelimiter("\\A").next();
        bs = page.getBytes();
        assertEquals(10285, bs.length);
        len = Analyzer.cleanupPage(bs, page.length(), true);
        assertEquals(6314, len);
        len = Analyzer.cleanNonWords(bs, len);
        assertEquals(5823, len);

        page = new Scanner(AnalyzerTest.class.getResourceAsStream("../pg2.xml")).useDelimiter("\\A").next();
        bs = page.getBytes();
        assertEquals(26589, bs.length);
        len = Analyzer.cleanupPage(bs, page.length(), true);
        assertEquals(15080, len);
        len = Analyzer.cleanNonWords(bs, len);
        assertEquals(13836, len);

        page = new Scanner(AnalyzerTest.class.getResourceAsStream("../pg3.xml")).useDelimiter("\\A").next();
        bs = page.getBytes();
        len = Analyzer.cleanupPage(bs, page.length(), true);
        assertEquals(0, len);

        page = new Scanner(AnalyzerTest.class.getResourceAsStream("../pg4.xml")).useDelimiter("\\A").next();
        bs = page.getBytes();
        len = Analyzer.cleanupPage(bs, page.length(), true);
        assertEquals(30184, len);

        page = new Scanner(AnalyzerTest.class.getResourceAsStream("../pg5.xml")).useDelimiter("\\A").next();
        bs = page.getBytes();
        len = Analyzer.cleanupPage(bs, page.length(), true);
        assertEquals(64894, len);

        page = new Scanner(AnalyzerTest.class.getResourceAsStream("../pg6.xml")).useDelimiter("\\A").next();
        bs = page.getBytes();
        len = Analyzer.cleanupPage(bs, page.length(), true);
        assertEquals(5406, len);

        page = new Scanner(AnalyzerTest.class.getResourceAsStream("../pg7.xml")).useDelimiter("\\A").next();
        bs = page.getBytes();
        len = Analyzer.cleanupPage(bs, page.length(), true);
        assertEquals(13431, len);

        page = new Scanner(AnalyzerTest.class.getResourceAsStream("../pg8.xml")).useDelimiter("\\A").next();
        bs = page.getBytes();
        len = Analyzer.cleanupPage(bs, page.length(), true);
        assertEquals(16555, len);

    }
}
