package pl.com.sages.hadoop.mapreduce.wordcount;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertTrue;

public class WordCountRunnerMiniClusterTest {

    private String inputPath = "/user/sages/dane";
    private String outputPath= "/user/sages/wyniki";;

    private MiniMRClientCluster miniMRClientCluster;
    private MiniDFSCluster miniDFSCluster;

    @Before
    public void before() throws IOException {
        Configuration conf = new YarnConfiguration();

        miniDFSCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();

        miniMRClientCluster = MiniMRClientClusterFactory.create(this.getClass(), 2, conf);
        miniMRClientCluster.start();

        // dane
        DistributedFileSystem fs = miniDFSCluster.getFileSystem();
        fs.mkdirs(new Path(inputPath));
        fs.copyFromLocalFile(new Path(getClass().getClassLoader().getResource("20-000-mil-podmorskiej-zeglugi.txt").getFile()),new Path(inputPath));
    }

    @After
    public void after() throws IOException {
        miniMRClientCluster.stop();
    }

    @Test
    public void shouldCountWords() throws Exception {
        //given
        Configuration config = miniMRClientCluster.getConfig();

        FileSystem fs = miniDFSCluster.getFileSystem();
        fs.delete(new Path(outputPath), true);

        Job job = WordCountRunner.createJob(new Path(inputPath), new Path(outputPath), config);

        //when
        job.waitForCompletion(true);

        //then
        assertTrue(fs.exists(new Path(outputPath + "/_SUCCESS")));
        assertTrue(fs.exists(new Path(outputPath + "/part-r-00000")));
    }

}