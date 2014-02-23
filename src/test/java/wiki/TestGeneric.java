package wiki;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.IOException;
import java.util.Map;

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
public class TestGeneric {
    private static final Log LOG = LogFactory.getLog(TestGeneric.class);


    void addJarToClasspath(Configuration conf) throws IOException, InterruptedException {
        String cmd = String.format("/bin/bash -c 'cd %s; mvn package -Dmaven.test.skip=true'", TestConfig.project_path);
        if (LOG.isInfoEnabled())
            LOG.info(cmd);

        Process p = Runtime.getRuntime().exec(new String[]{"/bin/bash", "-c", cmd});
        p.waitFor();
        assertEquals(p.exitValue(), 0);
        DistributedCache.addArchiveToClassPath(new Path("file:///" + TestConfig.jar_path), conf, FileSystem.getLocal(conf));
    }

    void commonSettings(Configuration conf) throws IOException, InterruptedException {
    }

    void makeJobLocal(Configuration conf) throws IOException, InterruptedException {
        conf.set("mapreduce.framework.name", "local");//classic
        conf.set("mapred.job.tracker", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.set("hadoop.tmp.dir", TestConfig.loc_run_dir);
        commonSettings(conf);
        addJarToClasspath(conf);
    }

    void makeJobCluster(Configuration conf) throws IOException, InterruptedException {
        commonSettings(conf);
        addJarToClasspath(conf);
        updateYarnClasspath(conf);
    }

    void updateYarnClasspath(Configuration conf) {
    }

    void printConf(Configuration conf){
        for(Map.Entry<String, String> e: conf){
            System.out.printf("%s=%s\n", e.getKey(), e.getValue());
        }
    }

}
