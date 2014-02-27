package wiki;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.util.*;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Copyright 2014 Yevgen Yampolskiy
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class AdHoc {
    @Test
    public void findPages() throws Exception {
        String outpath = "file:///" + TestConfig.project_path + "/Data/output/wiki/Import/loc_0";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);

        SequenceFile.Reader reader;
        reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(outpath, "part-r-00000")));

        Text key, value;
        key = new Text();
        value = new Text();

        //Set<String> special = new HashSet<String>(Arrays.asList("align", "style", "center", "bgcolor", "class"));
        Set<String> special = new HashSet<String>(Arrays.asList("bgcolor"));


        while(reader.next(key, value)){
            Scanner scanner = new Scanner(value.toString());
            scanner.useDelimiter("\\W+");
            int count = 0;
            while(scanner.hasNext()){
                String s = scanner.next();
                if (special.contains(s)){
                    count++;
                }

                if (count > 0){
                    System.out.println("\n\n\n");
                    System.out.println(key);
                    System.out.println(value);
                    break;
                }

            }

        }

    }
}


//An affix is a morpheme that is attached to a word stem linguistics stem to form a new word Affixes may be derivation linguistics derivational like English ness and pre or inflection al like English plural s and past tense ed They are bound morpheme s by definition prefixes and suffixes may be separable affix es Affixation is thus the linguistic process speakers use to form different words by adding morphemes affixes at the beginning prefixation the middle infixation or the end suffixation of words Affixes are divided into plenty of categories depending on their position with reference to the stem Prefix and suffix are extremely common terms Infix and circumfix are less so as they are not important in European languages The other terms are uncommon class wikitable Categories of affixes Affix Example Schema Description Prefix do stem Appears before the stem Suffix postfix look stem Appears after the stem Suffixoid Semi suffix cat stem Appears after the stem but is only partially bound to it Infix Minne sota st em Appears within a stem common in Borneo Philippines languages Circumfix light stem One portion appears before the stem the other after Interfix speed meter stem stem Links two stems together in a compound word compound Duplifix teeny stem Incorporates a reduplication reduplicated portion of a stemgt may occur before after or within the stem Transfix Maltese language Maltese k t b he wrotegt compare root ktb write s te m A discontinuous affix that interleaves within a discontinuous stem Simulfix mse mce stem Changes a segment of a stem Suprafix pro duce noun gt pro duce verb stem Changes a suprasegmental feature of a stem Disfix Alabama language Alabama tipli break upgt compare root tipli break st m The elision of a portion of a stem Prefix and suffix may be subsumed under the term adfix in contrast to infix When marking text for interlinear gloss ing as in the third column in the chart above simple affixes such as prefixes and suffixes are separated from the stem with hyphens Affixes which disrupt the stem or which themselves are discontinuous are often marked off with angle brackets Reduplication is often shown with a tilde Affixes which cannot be segmented are marked with a back slash Lexical affixes or semantic affixes are bound elements that appear as affixes but function as incorporated noun s within verbs and as elements of compound noun s In other words they are similar to word roots stems in function but similar to affixes in form Although similar to incorporated nouns lexical affixes differ in that they never occur as freestanding nouns i e they always appear as affixes Lexical affixes are relatively rare The Wakashan languages Wakashan Salishan languages Salishan and Chimakuan languages all have lexical suffixes the presence of these is an areal feature of the Pacific Northwest of the North America The lexical suffixes of these languages often show little to no resemblance to free nouns with similar meanings Compare the lexical suffixes and free nouns of Saanich language Northern Straits Saanich written in the Saanich orthography and in Americanist phonetic notation Americanist notation class IPA wikitable style font size background efefef colspan Lexical Suffix colspan Noun o a person e t l ew tel x person n t net day s i el sk i l day sen s n foot lower leg sxene sx n foot lower leg wtw ew tx building house campsite le e l house Lexical suffixes when compared with free nouns often have a more generic or general meaning For instance one of these languages may have a lexical suffix that means water in a general sense but it may not have any noun equivalent referring to water in general and instead have several nouns with a more specific meaning such saltwater whitewater etc In other cases the lexical suffixes have become grammaticalization grammaticalized to various degrees Some linguists have claimed that these lexical suffixes provide only adverbial or adjectival notions to verbs Other linguists disagree arguing that they may additionally be syntactic Verb argument arguments just as free nouns are and thus equating lexical suffixes with incorporated nouns Gerdts gives examples of lexical suffixes in the Halkomelem language the word order here is verb subject object class IPA wikitable style line height em font size style background bbbbff VERB style background ffebad SUBJ style background ffbbbb OBJ ni ak t s s eni colspan the woman washed style line height em font size bgcolor white colspan style line height em font size style background bbbbff VERB style background ffebad SUBJ ni k s eni colspan the woman washed In sentence the verb wash is where is the root and and are inflectional suffixes The subject the woman is and the object is In this sentence the baby is a free noun The here is an auxiliary verb auxiliary which can be ignored for explanatory purposes In sentence does not appear as a free noun Instead it appears as the lexical suffix which is affixed to the verb root which has changed slightly in pronunciation but this can also be ignored here Note how the lexical suffix is neither the baby definiteness definite nor a baby indefinite such referential changes are routine with incorporated nouns In orthography the terms for affixes may be used for the smaller elements of conjunct characters For example Maya script Maya glyphs are generally compounds of a main sign and smaller affixes joined at its margins These are called prefixes superfixes postfixes and subfixes according to their position to the left on top to the right or at the bottom of the main glyph A small glyph placed inside another is called an infix Similar terminology is found with the conjunct consonants of the Indic alphabets For example the Tibetan alphabet utilizes prefix suffix superfix and subfix consonant letters Agglutination Augmentative Binary prefix Clitic Concatenation Derivation linguistics Derivation Diminutive English prefixes Family name affixes Internet related prefixes Marker linguistics Separable affix SI prefix Stemming affix removal using computer software Unpaired word Word formation Timothy Montler Montler Timothy Occasional Papers in Linguistics No Missoula MT University of Montana Linguistics Laboratory Montler Timothy Saanich North Straits Salish classified word list Canadian Ethnology service paper No Mercury series Hull Quebec Canadian Museum of Civilization ISBN