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
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;
import java.util.Scanner;

public class PageTitlesTest {
    private static final Log LOG = LogFactory.getLog(PageTitlesTest.class);

    static class Config{

        private final static String testProp = "test.properties";
        private final static String cfg_projectPath = "project.path"; //~/Projects/HDP
        private final static String cfg_jarPath = "jar.path"; //~/Projects/HDP/target/Hadoop-1.0-SNAPSHOT.jar
        private final static String cfg_loc_wiki_full = "loc.wiki.xml.bz2";///home/yevgen/wiki.xml.bz2
        private final static String cfg_loc_wiki_0 = "loc.wiki.0.bz2"; ///home/yevgen/wiki.0.bz2; first 2000000 lines
        private final static String cfg_loc_run_dir = "loc.run.dir";

        final static String project_path;
        final static String jar_path;
        final static String loc_wiki_full;
        final static String loc_wiki_0;
        final static String loc_run_dir;

        static {
            Properties prop = new Properties();
            try {
                InputStream is = PageTitlesTest.class.getResourceAsStream(testProp);
                if (is == null){
                    is = new FileInputStream(testProp);
                }
                prop.load(is);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
            project_path = prop.getProperty(cfg_projectPath);
            jar_path = prop.getProperty(cfg_jarPath);
            loc_wiki_full = prop.getProperty(cfg_loc_wiki_full);
            loc_wiki_0 = prop.getProperty(cfg_loc_wiki_0);
            loc_run_dir = prop.getProperty(cfg_loc_run_dir);
        }
    }


    void addJarToClasspath(Configuration conf) throws IOException, InterruptedException {
        String cmd = String.format("/bin/bash -c 'cd %s; mvn package -Dmaven.test.skip=true'", Config.project_path);
        if (LOG.isInfoEnabled())
            LOG.info(cmd);

        Process p = Runtime.getRuntime().exec(new String[]{"/bin/bash", "-c", cmd});
        p.waitFor();
        assertEquals(p.exitValue(), 0);
        DistributedCache.addArchiveToClassPath(new Path("file:///" + Config.jar_path), conf, FileSystem.getLocal(conf));
    }

    void commonSettings(Configuration conf) throws IOException, InterruptedException {
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");
        conf.set("mapred.reduce.slowstart.completed.maps", "1");
        addJarToClasspath(conf);
    }

    void makeJobLocal(Configuration conf) throws IOException, InterruptedException {
        conf.set("mapreduce.framework.name", "local");//classic
        conf.set("mapred.job.tracker", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.set("hadoop.tmp.dir", Config.loc_run_dir);
        commonSettings(conf);
    }

    void makeJobCluster(Configuration conf) throws IOException, InterruptedException {
        commonSettings(conf);
    }

    @Test
    public void PTSmallLocal() throws Exception {
        String outpath = "Data/output/wiki/TitleCount/loc_0";
        Configuration conf = new Configuration();
        makeJobLocal(conf);
        PageTitles.PTDriver driver = new PageTitles.PTDriver();
        driver.setConf(conf);
        driver.run(new String[]{Config.loc_wiki_0, outpath});
    }

    @Test
    public void PTSmallCluster() throws Exception {
        String outpath = "Data/output/wiki/TitleCount/cl_0/";
        Configuration conf = new Configuration();
        PageTitles.PTDriver driver = new PageTitles.PTDriver();
        makeJobCluster(conf);
        driver.setConf(conf);
        driver.run(new String[]{"file:///" + Config.loc_wiki_0, outpath});
        assertEquals(driver.getPagesTotal(), 10352);
        assertEquals(driver.getPagesMissed(), 0);
    }

    @Test
    public void PTFullCluster() throws Exception {
        String outpath = "Data/output/wiki/TitleCount/cl_full/";
        Configuration conf = new Configuration();
        PageTitles.PTDriver driver = new PageTitles.PTDriver();
        makeJobCluster(conf);
        String inFile = Config.loc_wiki_full;
//        long sz = (new File(inFile).length() / 6) + 1;
//        conf.set("mapred.min.split.size", Long.toString(sz));
        driver.setConf(conf);
        long start = System.currentTimeMillis();
        driver.run(new String[]{"file:///" + inFile, outpath});
        long end = System.currentTimeMillis();
        System.out.println("Time taken: " + (end-start));

        assertEquals(driver.getPagesTotal(), 13715113);
        assertEquals(driver.getPagesMissed(), 0);
    }

}

