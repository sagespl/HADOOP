package pl.com.sages.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

public class HdfsApiExternalTest {

    private static final String USER = System.getProperty("user.name");
    private static final String USER_HOME = "/user/" + USER;
    private static final int BUFF_SIZE = 4096;

    private static final String HDFS_INPUT_PATH = USER_HOME + "/iris.csv";
    private static final String HDFS_OUTPUT_PATH = USER_HOME + "/hdfs-write-test.csv";
    private static final String LOCAL_OUTPUT_PATH = "/tmp/" + USER + "_iris.csv";

    private FileSystem fs;

    @Before
    public void before() throws IOException {

        Configuration conf = new Configuration(false);
        conf.addResource(this.getClass().getClassLoader().getResourceAsStream("hdfs-configuration.xml"));

        fs = FileSystem.get(conf);

        // przygotowanie danych
        cleanFiles();

        FSDataOutputStream outputStream = fs.create(new Path(HDFS_INPUT_PATH), true);
        try {
            IOUtils.copyBytes(getTestFileInputStream(), outputStream, BUFF_SIZE);
        } finally {
            IOUtils.closeStream(outputStream);
        }
    }

    @After
    public void cleanFiles() throws IOException {
        new File(LOCAL_OUTPUT_PATH).delete();
        fs.delete(new Path(HDFS_INPUT_PATH), true);
        fs.delete(new Path(HDFS_OUTPUT_PATH), true);
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
        assertThat(files).contains("tmp");
        assertThat(files).contains("user");
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
        assertThat(files).doesNotContain("tmp");
    }

    @Test
    public void shouldReadFileFromHDFS() throws Exception {
        // given
        FSDataInputStream inputStream = fs.open(new Path(HDFS_INPUT_PATH));

        // when
        try {
            IOUtils.copyBytes(inputStream, new FileOutputStream(new File(LOCAL_OUTPUT_PATH)), BUFF_SIZE);
        } finally {
            IOUtils.closeStream(inputStream);
        }

        // then
        assertThat(new File(LOCAL_OUTPUT_PATH)).exists();
    }

    @Test
    public void shouldWriteFileToHDFS() throws Exception {
        // given

        // when
        FSDataOutputStream outputStream = fs.create(new Path(HDFS_OUTPUT_PATH), true);
        try {
            IOUtils.copyBytes(getTestFileInputStream(), outputStream, BUFF_SIZE);
        } finally {
            IOUtils.closeStream(outputStream);
        }

        // then
        assertTrue(fs.exists(new Path(HDFS_OUTPUT_PATH)));
    }

    @Test
    public void shouldWriteFileWithProgress() throws Exception {
        // given

        // when
        FSDataOutputStream outputStream = fs.create(new Path(HDFS_OUTPUT_PATH), new Progressable() {
            @Override
            public void progress() {
                System.out.println("Trawa zapis pliku...");
            }
        });
        try {
            IOUtils.copyBytes(getTestFileInputStream(), outputStream, BUFF_SIZE);
        } finally {
            IOUtils.closeStream(outputStream);
        }

        // then
        assertTrue(fs.exists(new Path(HDFS_OUTPUT_PATH)));
    }

    @Test
    public void shouldCopyFromLocal() throws Exception {
        // given

        // when
        fs.copyFromLocalFile(new Path(getTestFilePath()), new Path(HDFS_OUTPUT_PATH));

        // then
        assertTrue(fs.exists(new Path(HDFS_OUTPUT_PATH)));
    }

    @Test
    public void shouldCreateWholeDirectoryPath() throws Exception {
        // given
        fs.delete(new Path(USER_HOME + "/very"), true);
        String veryLongDirecotryPath = USER_HOME + "/very/long/direcotry/path";

        // when
        boolean created = fs.mkdirs(new Path(veryLongDirecotryPath));

        // then
        assertTrue(created);
        fs.delete(new Path(USER_HOME + "/very"), true);
    }

    @Test
    public void shouldNotToChangeNameToWrongFile() throws Exception {
        // given

        // when
        boolean renamed = fs.rename(new Path("/tmp/this/file/does/not/exists"), new Path("/tmp/some/path/to/file"));

        // then
        assertFalse(renamed);
    }

    @Test
    public void shouldSetAndGetReplication() throws Exception {
        // given
        short targetReplication = (short) 5;

        // when
        fs.setReplication(new Path(HDFS_INPUT_PATH), targetReplication);
        FileStatus fileStatus = fs.getFileStatus(new Path(HDFS_INPUT_PATH));
        short replication = fileStatus.getReplication();

        // then
        assertThat(replication).isEqualTo(targetReplication);
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

    private InputStream getTestFileInputStream() {
        return this.getClass().getClassLoader().getResourceAsStream("iris.csv");
    }

    private String getTestFilePath() {
        return this.getClass().getClassLoader().getResource("iris.csv").getFile();
    }

}
