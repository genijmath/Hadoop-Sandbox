package wiki;

import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: yevgen
 * Date: 2/7/14
 * Time: 9:55 PM
 * To change this template use File | Settings | File Templates.
 */
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
        assertEquals("def", new String(bs, 0, len, Charset.defaultCharset()));

        s = "x   *    [[abc   ]]def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals(s, new String(bs, 0, len, Charset.defaultCharset()));

        s = "x   *    [[abc   ]]def\n*[[ XYZ]]TX";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("x   *    [[abc   ]]def\nTX", new String(bs, 0, len, Charset.defaultCharset()));

        s = "  [[abc   ]]def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals( "[[abc   ]]def", new String(bs, 0, len, Charset.defaultCharset()));


        s = "    *    x{{abc   }}def";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("*xdef", new String(bs, 0, len, Charset.defaultCharset()));

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
        assertEquals("*[[ x [[ y ]]", new String(bs, 0, len, Charset.defaultCharset()));

        s = "  *  [[ xy ]]\n[[ab]] x [[cd] {{ rbc }} uv\n==title==\nvdx=rs{tx}{{ttxx}}*[mn]\nrtx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("\n[[ab]] x [[cd]  uv\nvdx=rs{tx}*[mn]\nrtx", new String(bs, 0, len, Charset.defaultCharset()));


        s = "asfddsaf <text xml:space=\"preserve\">abc";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), true);
        assertEquals("abc", new String(bs, 0, len, Charset.defaultCharset()));

        s = "abc &lt;ref&gt; bla &lt;/ref&gt;xxx";
        bs = s.getBytes();
        len = Analyzer.cleanupPage(bs, s.length(), false);
        assertEquals("abc xxx", new String(bs, 0, len, Charset.defaultCharset()));

        s = "abc &lt;ref blabla&gt; bla &lt;/ref&gt;xxx";
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
        System.out.println(len);
        System.out.println(new String(bs, 0, len));

    }


}
