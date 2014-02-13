package wiki;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
class TestConfig {

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
