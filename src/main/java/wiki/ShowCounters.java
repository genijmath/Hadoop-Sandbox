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
