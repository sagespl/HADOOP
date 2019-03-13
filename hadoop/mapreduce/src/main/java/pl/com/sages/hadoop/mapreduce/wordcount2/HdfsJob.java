package pl.com.sages.hadoop.mapreduce.wordcount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.security.PrivilegedExceptionAction;
import java.util.Date;

import static pl.com.sages.hadoop.mapreduce.conf.HadoopConfBuilder.HADOOP_USER;

/**
 * HDFS job
 */
public class HdfsJob {
	public static final String TEST_FILE_NAME = "test";

	public int run() throws IOException {
		//Configuration conf = getConfiguration();
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hdpmaster1:8020");
		conf.set("hadoop.job.ugi", HADOOP_USER);

		Path filePath = new Path(String.format("/user/%s/%s", HADOOP_USER, TEST_FILE_NAME));

		FileSystem fs = FileSystem.get(conf);

		// Create the file and write into it
		FSDataOutputStream fsOut = fs.create(filePath, true);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsOut));

		for (int i=0; i<=10; i++) {
			String line = String.format("Line %d\n", i);
			writer.write(line);
		}

		writer.close();

		// Append  to file
		boolean canAppend = Boolean.getBoolean(fs.getConf().get("dfs.support.append"));
		if (canAppend) {
			System.out.println("Can append!");
			// Create the file and write into it
			fsOut = fs.append(filePath);
			writer = new BufferedWriter(new OutputStreamWriter(fsOut));
			writer.write("Line appended\n");
			writer.close();
		}
		else {
			System.out.println("Cannot append!");
		}

		// List the files
		for(FileStatus status : fs.listStatus(filePath.getParent())){
			Date date = new Date(status.getModificationTime());
			String line = String.format("%s\t%s\t%sB",
					status.getPath(), date.toString(), status.getLen());
			System.out.println(line);
		}

		// Read the test file
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
		String line = reader.readLine();
		while (line != null) {
			System.out.println(line);
			line = reader.readLine();
		}

		reader.close();

		fs.delete(filePath, false);

		// List the files
		for(FileStatus status : fs.listStatus(filePath.getParent())){
			Date date = new Date(status.getModificationTime());
			line = String.format("%s\t%s\t%sB",
					status.getPath(), date.toString(), status.getLen());
			System.out.println(line);
		}

		return 0;
	}

	public int runAsUser(String userName){
		try {
			UserGroupInformation ugi
					= UserGroupInformation.createRemoteUser(userName);

			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				public Void run() throws Exception {
					HdfsJob job = new HdfsJob();
					job.run();

					return null;
				}

			});
			return 0;
		} catch (Exception e) {
			return 1;
		}
	}

	public static void main(String args[]) throws IOException {
		HdfsJob job = new HdfsJob();
		//job.run();
		job.runAsUser(HADOOP_USER);
	}
}
