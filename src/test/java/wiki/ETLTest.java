package wiki;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

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
public class ETLTest extends TestGeneric{
    void commonSettings(Configuration conf) throws IOException, InterruptedException {
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");
        conf.set("mapred.reduce.slowstart.completed.maps", "1");
    }

    @Test
    public void ImportSmallLocal() throws Exception {
        String outpath = "Data/output/wiki/Import/loc_0";
        Configuration conf = new Configuration();
        makeJobLocal(conf);
        ETL.Import.ImportDriver driver = new ETL.Import.ImportDriver();
        driver.setConf(conf);

        Path outDir = new Path(outpath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outDir)){
            fs.delete(outDir, true);
        }

        assertEquals(0, driver.run(new String[]{TestConfig.loc_wiki_0, outpath}));
    }

    @Test
    public void ImportSmallCluster() throws Exception {
        String outpath = "Data/output/wiki/Import/loc_0";
        Configuration conf = new Configuration();
        makeJobCluster(conf);
        ETL.Import.ImportDriver driver = new ETL.Import.ImportDriver();
        driver.setConf(conf);

        Path outDir = new Path(outpath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outDir)){
            fs.delete(outDir, true);
        }

        assertEquals(0, driver.run(new String[]{"file:///" + TestConfig.loc_wiki_0, outpath}));
    }


    @Test
    public void ImportFullCluster() throws Exception {
        String outpath = "Data/output/wiki/Import/full";
        Configuration conf = new Configuration();
        makeJobCluster(conf);
        ETL.Import.ImportDriver driver = new ETL.Import.ImportDriver();
        driver.setConf(conf);

        Path outDir = new Path(outpath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outDir)){
            fs.delete(outDir, true);
        }


        long sz = (new File(TestConfig.loc_wiki_full).length() / 6) + 1;
        conf.set("mapred.min.split.size", Long.toString(sz));
        conf.set("mapred.reduce.tasks", "12");
        conf.set("mapred.map.max.attempts", "1");
        conf.set("mapreduce.map.speculative", "false");
        conf.set("mapreduce.reduce.speculative", "false");

        assertEquals(0, driver.run(new String[]{"file:///" + TestConfig.loc_wiki_full, outpath}));
    }

    @Test
    public void StemSmallLocal() throws Exception {
        String inpath = "Data/output/wiki/Import/loc_0";
        String outpath = "Data/output/wiki/Stems/loc_0";
        Configuration conf = new Configuration();
        makeJobLocal(conf);
        ETL.WordStemming.WSDriver driver = new ETL.WordStemming.WSDriver();
        driver.setConf(conf);

        Path outDir = new Path(outpath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outDir)){
            fs.delete(outDir, true);
        }

        assertEquals(0, driver.run(new String[]{inpath, outpath}));
    }


    void updateYarnClasspath(Configuration conf) {
        String cp = conf.get("yarn.application.classpath");
        String home=System.getenv("HOME");
        cp+=","+home+"/" + ".m2/repository/org/apache/lucene/lucene-core/4.4.0/*";
        cp+=","+home+"/" + ".m2/repository/org/apache/lucene/lucene-analyzers/4.4.0/*";
        cp+=","+home+"/" + ".m2/repository/org/apache/lucene/lucene-analyzers-common/4.4.0/*";
        conf.set("yarn.application.classpath", cp);
    }

    @Test
    public void StemSmallCluster() throws Exception {
        String inpath = "Data/output/wiki/Import/loc_0";
        String outpath = "Data/output/wiki/Stems/loc_0";
        Configuration conf = new Configuration();
        makeJobCluster(conf);
        ETL.WordStemming.WSDriver driver = new ETL.WordStemming.WSDriver();
        driver.setConf(conf);

        Path outDir = new Path(outpath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outDir)){
            fs.delete(outDir, true);
        }

        assertEquals(0, driver.run(new String[]{"file:///" + TestConfig.project_path +  "/" +  inpath, outpath}));
    }



    @Test
    public void StemFullCluster() throws Exception {
        String inpath = "Data/output/wiki/Import/full";
        String outpath = "Data/output/wiki/Stems/full";
        Configuration conf = new Configuration();
        makeJobCluster(conf);
        ETL.WordStemming.WSDriver driver = new ETL.WordStemming.WSDriver();
        driver.setConf(conf);

        Path outDir = new Path(outpath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outDir)){
            fs.delete(outDir, true);
        }

        long sz = Long.MAX_VALUE;
        conf.set("mapred.min.split.size", Long.toString(sz));
        conf.set("mapred.reduce.tasks", "0");
        conf.set("mapred.map.max.attempts", "1");
        conf.set("mapreduce.map.speculative", "false");
        conf.set("mapreduce.reduce.speculative", "false");

        assertEquals(0, driver.run(new String[]{inpath, outpath}));
    }

}
