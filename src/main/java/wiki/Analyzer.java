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


public class Analyzer {



    static boolean[] letters = new boolean[256];
    static {
        for(byte i = 'a'; i <= 'z'; i++){
            letters[i] = true;
        }
        for(byte i = 'A'; i <= 'Z'; i++){
            letters[i] = true;
        }
    }

    /**
     * Removes non-words including special html characters (&...; like &quot;)
     * Takes care of &amp;nbsp;
     * @param page single wiki page
     * @param len length of the input page
     * @return length of page using single space as word separator
     */
    static int cleanNonWords(byte[] page, int len){
        int pointer = 0;
        boolean separtor = true;
        for(int i = 0; i < len; i++){

            if (page[i] == '&'){
                if (i + 4 < len && page[i+1] == 'a' && page[i+2]=='m' && page[i+3]=='p' && page[i+4]==';'){
                    i+=5;
                }

                while(i < len && page[i] != ';')
                    i++;
                continue;
            }

            boolean isLetter = letters[(int) page[i] & 0xFF];
            if ((!isLetter) && separtor)
                continue;

            separtor = !isLetter;

            if (isLetter)
                page[pointer++] = page[i];
            else
                page[pointer++] = ' ';
        }
        return pointer;
    }



    /**
     * Removes:
     * Text before <text xml:space="preserve">
     * Text after </text>
     * Titles marked by ^=...=$
     * [http:// ... ]  (single bracket!)
     * Control structures tagged as {{....}}  with possible embedded levels {{ ... {{ ... }}  ...  }}
     *   special case: {{xxx begin}}....{{xxx end}}
     * [[File: .... [[  ....  ]]  .... ]]
     * [[Image: .... [[  ....  ]]  .... ]]
     * {| ... |}  like {|class=&quot;wikitable&quot;...|} with possible embedded levels ( {{...}} are removed before {|..|} is processed
     * More generally: [[\w*: ... ]]
     * References tagged by <ref ...</ref>     -- removes citations  -- ignoring <br>
     * &lt;gallery&gt; ... &lt;/gallery&gt;
     * more generally, all text between &lt; and 'matching' &gt;
     * Removes: #REDIRECT articles
     *
     * @param page single wiki page
     * @param len actual length of the wiki page
     * @param removeStart indicates if we should consider only chars after<text xml:space="preserve">
     * @return length of the clean page; input page will be modified
     */
    static int cleanupPage(byte[] page, int len, boolean removeStart){
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


        int pointer = 0;
        boolean newLine = true;

        end++;
        while(end < len && page[end] == ' ')
            end++;
        if (end + 8 < len && page[end]=='#' && page[end+1]=='R' && page[end+2]=='E' && page[end+3]=='D' && page[end+8]=='T')//#REDIRECT
            return 0;
        if (end + 8 < len && page[end]=='#' && page[end+1]=='r' && page[end+2]=='e' && page[end+3]=='d' && page[end+8]=='t')//#redirect
            return 0;



        //first byte is used as a counter, so it is initialized as \1
        byte[] ltBytes = "\1&lt;".getBytes();
        byte[] braceBytes = "\1{{".getBytes();
        byte[] endText = "\1</text>".getBytes();
        byte[] bracketBytes = "\1[[".getBytes();
        byte[] httpBytes = "\1[http://".getBytes();

        byte[][] tags = new byte[][]{ltBytes, braceBytes, endText, bracketBytes, httpBytes};
        byte[] gtBytes = "\1&gt;".getBytes();//temp buffer
        byte[] closeComment = "\1--&gt;".getBytes();

        byte[] openConstruct = new byte[3];
        byte[] closeConstruct = new byte[3];

        I:
        for(int i = end; i < len; i++){
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

            for (byte[] tag: tags){
                if (tag[tag[0]] == b){
                    tag[0]++;
                }else{
                    tag[0] = 1;
                }
            }

            if (endText.length == endText[0]){//DONE!
                pointer -= (endText.length - 2);
                break;
            }

            if (braceBytes.length == braceBytes[0]){  //remove {{...}}
                braceBytes[0] = 1;
                openConstruct[0] = 1;
                openConstruct[1] = openConstruct[2] = '{';
                closeConstruct[0] = 1;
                closeConstruct[1] = closeConstruct[2] = '}';
                int rc = getClosePosition(page, len, i+1, openConstruct, closeConstruct);//returns position of the character after }}
                if (rc !=-1){
                    int tg = i+1;//handle {{*** begin | }} ... {{*** end}}
                    while(letters[(int)page[tg]&0xFF]) tg++;
                    int nxt=tg;
                    while(!letters[(int)page[nxt]&0xFF]) nxt++;
                    if (nxt < rc){
                        if (nxt+5 < len && page[nxt]=='b'&&page[nxt+1]=='e'&&page[nxt+2]=='g'&&page[nxt+3]=='i'&&page[nxt+4]=='n' && !letters[(int)page[nxt+5]&0xFF]){
                            byte[] oc = ("\1{{"+ new String(page, i+1, tg-i-1) + " begin").getBytes();
                            byte[] cc = ("\1{{"+ new String(page, i+1, tg-i-1) + " end}}").getBytes();
                            int rc2 = getClosePosition(page, len, i+1, oc, cc);
                            if (rc2 != -1){
                                i = rc2-1; //found {{*** end}}
                                pointer -= 1;
                                continue I;
                            }
                        }
                    }

                    i = rc - 1;
                    pointer -= 1;
                    continue I;
                }
            }

            if (bracketBytes.length == bracketBytes[0]){  //remove [[...]] if necessary
                bracketBytes[0] = 1;
                openConstruct[0] = 1;
                openConstruct[1] = openConstruct[2] = '[';
                closeConstruct[0] = 1;
                closeConstruct[1] = closeConstruct[2] = ']';

                boolean remove = false;
                for(int j = i+1; j < len; j++){
                    if (!letters[(int) page[j] & 0xFF]){
                        remove = (page[j] == ':');
                        break;
                    }
                }
                if (remove){
                    bracketBytes[0] = 1;
                    int rc = getClosePosition(page, len, i+1, openConstruct, closeConstruct);//returns position of the character after }}
                    if (rc!=-1){
                        i = rc - 1;
                        pointer -= 1;
                        continue I;
                    }
                }
            }

            if (httpBytes.length == httpBytes[0]){//remove [http://...]
                httpBytes[0] = 1;
                for(int j = i + 1; j < len; j++){
                    if (page[j] == ']'){
                        i = j;
                        pointer -= (httpBytes.length - 2);
                        continue I;
                    }
                }
            }

            if (ltBytes.length == ltBytes[0]){
                ltBytes[0] = 1;
                int e = i+1;
                while(e < len && letters[(int) page[e] & 0xFF])
                    e++;
                if (e < len && e != i+1){
                    int rc = 0;
                    try{
                        if (e == i + 3 && ( //<br> or <br />
                                (page[i+1]=='b' && page[i+2] == 'r')||
                                (page[i+1]=='B' && page[i+2] == 'R'))){
                            pointer -= 3;
                            i = e;
                            continue I;
                        }


                        rc = getCloseTag(page, len, i+1, e, e, gtBytes);//tag is given by page[i+1: e)
                    }catch (StackOverflowError ex){
                        System.out.println("getCloseTagLoop: ");
                        System.out.println(new String(page, 0, len));
                        System.out.println();
                        System.out.println(new String(page, i+1, e-(i+1)));
                        System.out.flush();
                        throw ex;
                    }


                    if (rc != -1){
                        if (keepTheTag(page, i+1, e)){
                            eraseTag(page, i-3, len, e);
                            eraseTag(page, i-3, len, rc-4);
                            pointer -= 3;
                            continue I;
                        }

                        i = rc - 1;
                        pointer -= 3;
                        continue I;
                    }
                }else{
                    if (e+2 < len && page[e]=='!' && page[e+1]=='-' && page[e+2]=='-'){
                        //remove <!-- ... -->
                        closeComment[0] = 1;
                        e -= 3;
                        do{
                            if (closeComment[closeComment[0]] == page[e]){
                                closeComment[0]++;
                            }else{
                                closeComment[0] = 1;
                            }
                            e++;
                        }
                        while(e < len && closeComment[0] != closeComment.length);
                        i = e - 1;
                        pointer -= 3;
                        continue I;
                    }
                }
            }

            newLine = (b == '\n');

            page[pointer++] = b;
        }


        return cleanupPageSecond(page, pointer);
    }

    //removes {|..|} after {{...}} is removed
    private static int cleanupPageSecond(byte[] page, int len){
        final byte[] txtStart = "<text xml:space=\"preserve\">".getBytes();

        int pointer = 0;


        //first byte is used as a counter, so it is initialized as \1
        byte[] classBytes = "\1{|".getBytes();

        byte[][] tags = new byte[][]{classBytes};
        byte[] openConstruct = new byte[3];
        byte[] closeConstruct = new byte[3];

        I:
        for(int i = 0; i < len; i++){
            byte b = page[i];

            for (byte[] tag: tags){
                if (tag[tag[0]] == b){
                    tag[0]++;
                }else{
                    tag[0] = 1;
                }
            }

            if (classBytes.length == classBytes[0]){ //remove {|...|}
                classBytes[0] = 1;
                openConstruct[0] = 1;
                openConstruct[1] = '{';
                openConstruct[2] = '|';
                closeConstruct[0] = 1;
                closeConstruct[1] = '|';
                closeConstruct[2] = '}';
                int rc = getClosePosition(page, len, i+1, openConstruct, closeConstruct);//returns position of the character after }}
                if (rc != -1){
                    i = rc - 1;
                    pointer -= 1;
                    continue I;
                }
            }

            page[pointer++] = b;
        }


        return pointer;
    }

    private static int calcHash(byte[] arr, int start, int end){
        int hash = 0;
        for(int i = start; i < end; i++){
            hash = hash * 27 + arr[i];
        }
        return hash;
    }

    private static int calcHash(byte[] arr){
        return calcHash(arr, 0, arr.length);
    }

    static int blockquote = calcHash("blockquote".getBytes());
    private static boolean keepTheTag(byte[] page, int start, int end){
        //returns true if we do not want to throw away tag content
        int hash = calcHash(page, start, end);
        return hash == blockquote;
    }

    /**
     *  Starting with <i>in</i> it replaces chars with '_' to the left and right until &lt; or &gt; is found
     */
    private static void eraseTag(byte[] page, int low, int high, int in){
        int start = -1;
        int end = -1;
        for(int i = in; i < high-3; i++){
            if (page[i] == '&' && page[i+1] == 'g' && page[i+2]=='t' && page[i+3]==';'){
                end = i+3;
                break;
            }
        }
        for(int i = in-3; i >= low; i--){
            if (page[i] =='&' && page[i+1] == 'l' && page[i+2]=='t' && page[i+3] == ';'){
                start = i;
            }
        }

        if (start != -1 && end != -1){
            for(int i = start; i <= end; i++)
                page[i] = '_';
        }
    }



    private static int getCloseTag(byte[] page, int len, int s, int e, int start, byte[] gtBytes){
        gtBytes[0] = 1;
        int i = start;
        for(i = start; i < len; i++){//search for />
            if (gtBytes[gtBytes[0]] == page[i]){
                gtBytes[0]++;

                if (gtBytes[0] == gtBytes.length){
                    gtBytes[0] = 0;
                    if (page[i-4] == '/'){ // /&gt;
                        return i + 1;
                    }
                    break;
                }

            }else{
                gtBytes[0] = 1;
            }
        }

        //search for /tag&gt;

        while(true){
            int p = find(page, len, s, e, i);//find next tag name
            if (p == -1)
                return -1;
            if (p+3 >=len)//&gt;
                return -1;
            //is it close tag?

            if (page[p-(e-s)-5]=='&' && page[p-(e-s)-4]=='l' && page[p-(e-s)-3]=='t' && page[p-(e-s)-2]==';'
                    && page[p-(e-s)-1]=='/' && page[p] == '&' && page[p+1]=='g' &&  page[p+2]=='t' && page[p+3]==';'){
                return p+4;
            }

            //is it embedded open tag?
            if (page[p-(e-s)-1]==';' && page[p-(e-s)-2] == 't' && page[p-(e-s)-3]=='l' &&  page[p-(e-s)-4]=='&'){
                if (page[p] == ' ' || page[p]=='\n' || (page[p]=='&' && page[p+1]=='g' &&  page[p+2]=='t' && page[p+3]==';')){
                    int rc = getCloseTag(page, len, s, e, p, gtBytes);
                    if (rc == -1)
                        return -1;

                    i = rc;
                    continue;
                }
            }
            i=p;
        }
    }

    private static int find(byte[] page, int len, int s, int e, int start){
        //look for tag where tag is given by page[s:e)
        while(start < len){
            if (page[start]==page[s]){
                int l = e - s;
                boolean found = true;
                for(int i = 1; i < l; i++){
                    if (page[s+i] != page[start+i]){
                        found = false;
                        break;
                    }
                }
                if (found){
                    start += l;
                    return start;
                }
            }
            start++;
        }
        return -1;
    }

    private static int getClosePosition(byte[] page, int len, int i, byte[] openBrace, byte[] closeBrace) {
        int lvl = 1;
        for(int j = i; j < len; j++){
            if (page[j] == closeBrace[closeBrace[0]]){
                closeBrace[0]++;
            }
            else{
                closeBrace[0] = 1;
            }

            if (page[j] == openBrace[openBrace[0]]){
                openBrace[0]++;
            }
            else{
                openBrace[0] = 1;
            }

            if (openBrace[0] == openBrace.length){
                openBrace[0] = 1;
                lvl++;
            }

            if (closeBrace[0] == closeBrace.length){
                closeBrace[0] = 1;
                lvl--;
                if (lvl == 0){
                    return j+1;
                }
            }
        }
        return -1;
    }
}
