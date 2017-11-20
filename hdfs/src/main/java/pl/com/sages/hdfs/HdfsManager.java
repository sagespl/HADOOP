package pl.com.sages.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;

import java.io.IOException;

public class HdfsManager {

    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        Configuration conf = new Configuration(false);
        conf.addResource(HdfsManager.class.getClassLoader().getResourceAsStream("hdfs-configuration.xml"));
        FileSystem fs = FileSystem.get(conf);


        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        SnapshottableDirectoryStatus[] stats = dfs.getSnapshottableDirListing();
        if (stats != null) {
            for (SnapshottableDirectoryStatus stat : stats) {

                Path snapshotableDirectory = stat.getFullPath();
                System.out.println(snapshotableDirectory);

                FileStatus[] fileStatuses = dfs.listStatus(new Path(snapshotableDirectory, ".snapshot"));
                for (FileStatus fileStatus : fileStatuses) {
                    String snapshotName = fileStatus.getPath().getName();
                    System.out.println("Usuwam snapshot: " + snapshotName);
                    dfs.deleteSnapshot(snapshotableDirectory, snapshotName);
                }

                dfs.disallowSnapshot(snapshotableDirectory);
            }
        }

    }

}
