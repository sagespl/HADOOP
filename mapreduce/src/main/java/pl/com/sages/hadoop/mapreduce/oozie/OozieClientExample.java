package pl.com.sages.hadoop.mapreduce.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.util.Properties;

/**
 * OozieClientExample
 */
public class OozieClientExample {
    public void run () throws OozieClientException, InterruptedException {

        OozieClient oozieClient = new OozieClient("http://sandbox:11000/oozie");

        String user = System.getProperty("user.name");
        System.out.printf("Submitting as user: %s\n", user);

        // Oozie properties
        Properties conf = oozieClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, "hdfs://sandbox:8020/user/root/oozie/");
        conf.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "true");

        // Custom properties
        conf.setProperty("jobTracker", "sandbox:8050");
        conf.setProperty("nameNode", "hdfs://sandbox:8020");
        conf.setProperty("inputDir", "/user/hue/jobsub/sample_data/midsummer.txt");
        conf.setProperty("outputDir", String.format("/user/%s/oozie-wf-server-out", user));

        // Job start
        String jobId = oozieClient.run(conf);
        System.out.println("Oozie workflow job submitted");

        while (oozieClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
            System.out.println("Workflow job running ...");
            Thread.sleep(10 * 1000);
        }

        System.out.println("Oozie workflow job completed ...");
        System.out.println(oozieClient.getJobInfo(jobId));
    }

    public static void main(String[] args) throws OozieClientException, InterruptedException {
        OozieClientExample client = new OozieClientExample();
        client.run();
    }

}
