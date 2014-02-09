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

        s = "abc";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc", new String(bs, 0, len, Charset.defaultCharset()));

        s = "    *    [[abc   ]]def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("*    [[abc   ]]def", new String(bs, 0, len, Charset.defaultCharset()));

        s = "x   *    [[abc   ]]def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals(s, new String(bs, 0, len, Charset.defaultCharset()));

        s = "x   *    [[abc   ]]def\n*[[ XYZ]]TX";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("x   *    [[abc   ]]def\n*[[ XYZ]]TX", new String(bs, 0, len, Charset.defaultCharset()));

        s = "    *    x{{abc   }}def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("*    xdef", new String(bs, 0, len, Charset.defaultCharset()));

        s = "xy*z{{abc   }}def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("xy*zdef", new String(bs, 0, len, Charset.defaultCharset()));

        s = "x   *    {{abc   }}def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("x   *    def", new String(bs, 0, len, Charset.defaultCharset()));

        s = "x   *    {{abc   }}def\n*{{ XYZ}}TX";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("x   *    def\n*TX", new String(bs, 0, len, Charset.defaultCharset()));


        s = "=abc=\nA\n==  def ===\nB\n  c==C=\n  == jjjj  ==   \nF";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("A\nB\nc==C=\nF", new String(bs, 0, len, Charset.defaultCharset()));


        s = "  {{xxxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("{{xxxx", new String(bs, 0, len, Charset.defaultCharset()));

        s = "  {{xx{{yy}}zz";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("{{xxzz", new String(bs, 0, len, Charset.defaultCharset()));


        s = "  *  [[ x [[ y ]]";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("*  [[ x [[ y ]]", new String(bs, 0, len, Charset.defaultCharset()));

        s = "  *  [[ xy ]]\n[[ab]] x [[cd] {{ rbc }} uv\n==title==\nvdx=rs{tx}{{ttxx}}*[mn]\nrtx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("*  [[ xy ]]\n[[ab]] x [[cd]  uv\nvdx=rs{tx}*[mn]\nrtx", new String(bs, 0, len, Charset.defaultCharset()));


        s = "asfddsaf <text xml:space=\"preserve\">abc";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), true);
        assertEquals("abc", new String(bs, 0, len, Charset.defaultCharset()));

        s = "abc &lt;ref&gt; bla &lt;/ref&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len, Charset.defaultCharset()));

        s = "abc &lt;ref blabla bla /&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len, Charset.defaultCharset()));

        s = "abc &lt;ref blabla&gt; bla &lt;/ref&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len, Charset.defaultCharset()));

        s = "abc &lt;ref blabla&gt; bla &lt;/ref&gt;";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc ", new String(bs, 0, len, Charset.defaultCharset()));

        //ref in ref
        s = "abc &lt;ref blabla&gt; xxx &lt;ref blabla&gt; bla &lt;/ref&gt; rrr &lt;/ref&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len, Charset.defaultCharset()));

        s = "abc &lt;ref blabla&gt; xxx &lt;ref blabla /&gt; rrr &lt;/ref&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len, Charset.defaultCharset()));

        s = "abc &lt;ref blabla&gt; xxx &lt;ref&gt; rrr &lt;/ref&gt; rrr &lt;/ref&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len, Charset.defaultCharset()));

        s = "xxx</text>yyy";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("xxx", new String(bs, 0, len, Charset.defaultCharset()));


        String page1 = new Scanner(AnalyzerTest.class.getResourceAsStream("../pg1.xml")).useDelimiter("\\A").next();
        bs = page1.getBytes();
        len = Analyzer.cleanupPage(bs, page1.length(), true);
        assertEquals(6308, len);
        len = Analyzer.cleanNonWords(bs, len);
        assertEquals(5833, len);

    }


}
