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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.*;

public class PageTitlesTest extends TestGeneric {
    void commonSettings(Configuration conf) throws IOException, InterruptedException {
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");
        conf.set("mapred.reduce.slowstart.completed.maps", "1");
    }

    @Test
    public void PTSmallLocal() throws Exception {
        String outpath = "Data/output/wiki/TitleCount/loc_0";
        Configuration conf = new Configuration();
        makeJobLocal(conf);
        PageTitles.PTDriver driver = new PageTitles.PTDriver();
        driver.setConf(conf);
        assertEquals(0, driver.run(new String[]{TestConfig.loc_wiki_0, outpath}));
    }

    @Test
    public void PTSmallCluster() throws Exception {
        String outpath = "Data/output/wiki/TitleCount/cl_0/";
        Configuration conf = new Configuration();
        PageTitles.PTDriver driver = new PageTitles.PTDriver();
        makeJobCluster(conf);
        driver.setConf(conf);
        assertEquals(0, driver.run(new String[]{"file:///" + TestConfig.loc_wiki_0, outpath}));
        assertEquals(driver.getPagesTotal(), 10352);
        assertEquals(driver.getPagesMissed(), 0);
    }

    @Test
    public void PTFullCluster() throws Exception {
        String outpath = "Data/output/wiki/TitleCount/cl_full/";
        Configuration conf = new Configuration();
        PageTitles.PTDriver driver = new PageTitles.PTDriver();
        makeJobCluster(conf);
        String inFile = TestConfig.loc_wiki_full;
//        long sz = (new File(inFile).length() / 6) + 1;
//        conf.set("mapred.min.split.size", Long.toString(sz));
        driver.setConf(conf);
        long start = System.currentTimeMillis();
        assertEquals(0, driver.run(new String[]{"file:///" + inFile, outpath}));
        long end = System.currentTimeMillis();
        System.out.println("Time taken: " + (end-start));

        assertEquals(driver.getPagesTotal(), 13715113);
        assertEquals(driver.getPagesMissed(), 0);
    }

}

