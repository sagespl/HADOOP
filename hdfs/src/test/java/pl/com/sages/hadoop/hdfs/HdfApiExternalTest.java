package pl.com.sages.hadoop.hdfs;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class HdfApiExternalTest {

    public static final String LOCAL_INPUT_PATH = "/home/sages/Sages/dane/iris.csv";
    public static final String LOCAL_OUTPUT_PATH = "/tmp/acct.txt";

    public static final String HDFS_INPUT_PATH = "/demo/data/Customer/acct.txt";
    public static final String HDFS_OUTPUT_PATH = "/tmp/hdfs-write-test.txt";

    private FileSystem fs;

    @Before
    public void createRemoteFileSystem() throws IOException {
        Configuration conf = new Configuration(false);
        conf.addResource("/etc/hadoop/conf/core-site.xml");
        conf.addResource("/etc/hadoop/conf/hdfs-site.xml");

        conf.set("fs.default.name", "hdfs://sandbox.hortonworks.com:8020");

        conf.setClass("fs.hdfs.impl", DistributedFileSystem.class, FileSystem.class);

        fs = FileSystem.get(conf);
    }

    @Test
    public void shouldListFiles() throws Exception {
        // given

        // when
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        List<String> files = new ArrayList<>();
        for (FileStatus fileStatuse : fileStatuses) {
            String filename = fileStatuse.getPath().getName();
            files.add(filename);
        }

        // then
        assertThat(files).contains("user");
        assertThat(files).contains("mapred");
        assertThat(files).contains("mr-history");
    }

    @Test
    public void shouldListFilesWithFilter() throws Exception {
        // given

        // when
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().equals("user");
            }
        });

        List<String> files = new ArrayList<>();
        for (FileStatus fileStatuse : fileStatuses) {
            String filename = fileStatuse.getPath().getName();
            files.add(filename);
        }

        // then
        assertThat(files).contains("user");
        assertThat(files).doesNotContain("mapred");
        assertThat(files).doesNotContain("mr-history");
    }

    @Test
    public void shouldReadFile() throws Exception {
        // given

        new File(LOCAL_OUTPUT_PATH).delete();
        FSDataInputStream inputStream = fs.open(new Path(HDFS_INPUT_PATH));

        // przewijanie do przodu
//        inputStream.seek(2000);

        // when
        try {
            IOUtils.copyBytes(inputStream, new FileOutputStream(new File(LOCAL_OUTPUT_PATH)), 4096);
        } finally {
            IOUtils.closeStream(inputStream);
        }

        // then
        assertThat(new File(LOCAL_OUTPUT_PATH)).exists();
    }

    @Test
    public void shouldWriteFile() throws Exception {
        // given
        String inputPath = "/home/sages/Sages/dane/iris.csv";
        String outputPath = "/tmp/hdfs-write-test.txt";

        // when
        FSDataOutputStream outputStream = fs.create(new Path(outputPath), true);
        try {
            IOUtils.copyBytes(new FileInputStream(new File(inputPath)), outputStream, 4096);
        } finally {
            IOUtils.closeStream(outputStream);
        }

        // then
        Assert.assertTrue(fs.exists(new Path(outputPath)));
    }

    @Test
    public void shouldWriteFileWithProgress() throws Exception {
        fs.delete(new Path(HDFS_OUTPUT_PATH), false);

        // when
        FSDataOutputStream outputStream = fs.create(new Path(HDFS_OUTPUT_PATH), new Progressable() {
            @Override
            public void progress() {
                System.out.println("Trawa zapis pliku...");
            }
        });
        try {
            IOUtils.copyBytes(new FileInputStream(new File(LOCAL_INPUT_PATH)), outputStream, 4096);
        } finally {
            IOUtils.closeStream(outputStream);
        }

        // then
        Assert.assertTrue(fs.exists(new Path(HDFS_OUTPUT_PATH)));
    }

    @Test
    public void shouldCopyFromLocal() throws Exception {
        // given
        fs.delete(new Path(HDFS_OUTPUT_PATH), false);

        // when
        fs.copyFromLocalFile(new Path(LOCAL_INPUT_PATH), new Path(HDFS_OUTPUT_PATH));

        // then
        Assert.assertTrue(fs.exists(new Path(HDFS_OUTPUT_PATH)));
    }

    @Test
    public void shouldCreateWholeDirectoryPath() throws Exception {
        // given
        String veryLongPath = "/tmp/very/long/direcotry/path";
        fs.delete(new Path("/tmp/very"), true);//musi być overwerite!!!

        // when
        boolean created = fs.mkdirs(new Path(veryLongPath));

        // then
        Assert.assertTrue(created);
    }

    @Test
    public void shouldNotToChangeNameToWrongFile() throws Exception {
        // given

        // when
        boolean renamed = fs.rename(new Path("/this/file/does/not/exists"), new Path("/some/path/to/file"));

        // then
        Assert.assertFalse(renamed);
    }

    @Test
    public void shouldSetAndGetReplication() throws Exception {
        // given

        // when
        fs.setReplication(new Path(HDFS_INPUT_PATH), (short) 3);
        FileStatus fileStatus = fs.getFileStatus(new Path(HDFS_INPUT_PATH));
        short replication = fileStatus.getReplication();

        // then
        assertThat(replication).isEqualTo((short) 3);
    }

    @Test
    public void shouldManageBlockLocation() throws Exception {
        // given

        // when
        FileStatus fileStatus = fs.getFileStatus(new Path(HDFS_INPUT_PATH));
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        // then
        assertThat(fileBlockLocations.length).isGreaterThanOrEqualTo(1);
    }

}
