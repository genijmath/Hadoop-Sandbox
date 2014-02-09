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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yevgen
 * Date: 2/2/14
 * Time: 10:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class ShowCounters {

    static void showCounters(String jobID) throws IOException, InterruptedException {
        Cluster cluster = new Cluster(new Configuration());
        Job job = cluster.getJob(JobID.forName(jobID));
        Counters counters = job.getCounters();
        for (CounterGroup g: counters){
            for(Counter c: g){
                System.out.println(c.getDisplayName() + " " + c.getValue());
            }
        }
    }
}
