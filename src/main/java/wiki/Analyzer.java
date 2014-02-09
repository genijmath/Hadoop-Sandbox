package wiki;

import org.apache.hadoop.io.Text;

/**
 * Created with IntelliJ IDEA.
 * User: yevgen
 * Date: 2/7/14
 * Time: 9:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class Analyzer {

    /**
     * Removes:
     * Text before <text xml:space="preserve">
     * Text after </text>
     * Titles marked by ^=...=$
     * References tagged by <ref ...</ref>     -- removes citations
     * [http:// ... ]  (single bracket!)
     * Control structures tagged as {{....}}  with possible embedded levels {{ ... {{ ... }}  ...  }}
     * [[File: .... [[  ....  ]]  .... ]]
     * [[Image: .... [[  ....  ]]  .... ]]
     * &lt;gallery&gt; ... &lt;/gallery&gt;

     * @param page single wiki page
     * @return length of the clean page; input page will be modified
     */
    static int cleanupPage(byte[] page, int len, boolean removeStart){
        int pointer = 0;
        boolean newLine = true;
        int ref = 0;
        int brc = 0;
        int txt = 0;

        byte[] refBytes = "&lt;ref".getBytes();
        byte[] endRefBytes = "&lt;/ref&gt;".getBytes();
        byte[] endLstBytes = "]]".getBytes();
        byte[] braceBytes = "{{".getBytes();
        byte[] endBraceBytes = "}}".getBytes();
        byte[] endText = "</text>".getBytes();
        byte[] closeTag = "&gt;".getBytes();

        final byte[] txtStart = "<text xml:space=\"preserve\">".getBytes();

        int start = 0;
        int end = -1;
        if (removeStart){
            end += txtStart.length;
            while (end < len){
                if (page[start]=='<' && page[end] == '>'){
                    boolean same = true;
                    for(int i = 0; i < txtStart.length; i++){
                        if (page[start + i] != txtStart[i]){
                            same = false;
                            break;
                        }
                    }
                    if (same) break;
                }
                start++;
                end++;
            }
            if (end == len)
                return 0;
        }



        I:
        for(int i = end+1; i < len; i++){
            byte b = page[i];

            if (newLine && b == ' ') continue;
            if (newLine && b == '='){
                //remove ^=..=$
                boolean eq_sign = false;
                for(int j = i+1; j < len; j++){
                    byte b2 = page[j];
                    if (b2 == ' ') continue;
                    if (b2 == '\n' || b2 == '\r'){
                        if (eq_sign){
                            //remove title
                            i = j;
                        }else{
                            newLine = false;//we are at '=' character; ignore it
                        }
                        continue I;
                    }
                    eq_sign = (page[j] == '=');
                }
            }

            if (ref == refBytes.length){
                if (b==' ' || b == '&' || b=='\n' || b=='\r' ){
                    ref = 0;
                    //search for &gt;
                    O:
                    while(i < len){
                        if (page[i]  == ';'){
                            for(int j = 0; j < closeTag.length; j++){
                                boolean found = true;
                                if (page[i - closeTag.length + 1 + j] != closeTag[j]){
                                    found = false;
                                    break;
                                }
                                if (found){
                                    if (page[i-closeTag.length] == '/'){
                                        pointer -= refBytes.length;
                                        continue I;//<ref name='...'/>
                                    }
                                    i++;
                                    break O;
                                }
                            }
                        }
                        i++;
                    }
                    int endRef = 0;
                    for(int j = i+1; j < len; j++){
                        if (endRef == endRefBytes.length){
                            i = j - 1;
                            pointer -= refBytes.length;
                            continue I; //found </ref>
                        }
                        if (page[j] == endRefBytes[endRef])
                            endRef++;
                        else
                            endRef = 0;
                    }
                }else{
                    ref = 0;
                }
            }
            if (refBytes[ref] == b) ref++;
            else ref = 0;

            if (brc == braceBytes.length){  //remove {{...}}
                brc = 0;
                int endBrc = 0;
                int lvl = 1;
                for(int j = i+1; j < len; j++){
                    if (page[j] == '{' && page[j-1] == '{')
                        lvl++; //{{ ... {{
                    if (endBrc == endBraceBytes.length){
                        endBrc = 0;
                        lvl--;
                        if (lvl == 0){
                            i = j - 1;
                            pointer -= braceBytes.length;
                            continue I;
                        }
                    }

                    if (page[j] == endBraceBytes[endBrc])
                        endBrc++;
                    else{
                        endBrc = 0;
                    }
                }
            }

            if (braceBytes[brc] == b){
                brc++;
            }else{
                brc = 0;
            }

            if (endText.length == txt){
                pointer -= txt;
                break;
            }
            if (endText[txt] == b){
                txt++;
            }else{
                txt = 0;
            }

            newLine = (b == '\n');

            page[pointer++] = b;
        }
        return pointer;
    }
}
