package ee.ut.cs.willmore;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.ClusterStatus;

import ee.ut.cs.willmore.KMeansCluster.ClusterBSP;

public class KMeansClusterTest {

	public static void main(String[] args) throws InterruptedException,
			IOException, ClassNotFoundException {
		
		// BSP job configuration
		HamaConfiguration conf = new HamaConfiguration();

		conf.setInt("bsp.local.tasks.maximum", 10);

		BSPJob bsp = new BSPJob(conf, KMeansCluster.class);
		// Set the job name
		bsp.setJobName("K Means Clustering");
		bsp.setBspClass(ClusterBSP.class);

		// Set the task size as a number of GroomServer
		BSPJobClient jobClient = new BSPJobClient(conf);
		ClusterStatus cluster = jobClient.getClusterStatus(true);

		System.out.println("Grooms are: " + cluster.getActiveGroomNames());

		// Choose one as a master
		for (String peerName : cluster.getActiveGroomNames().values()) {
			System.out.println("Master Peer:" + peerName);
			conf.set(KMeansCluster.CONF_MASTER_TASK, peerName);
			break;
		}

		System.out.println("Setting number of tasks / clusters to:"
				+ cluster.getGroomServers());

		bsp.setNumBspTask(cluster.getGroomServers());

		FileSystem fileSys = FileSystem.get(conf);

		final long jobTime = System.currentTimeMillis();

		final String srcFileName = "/tmp/kmeans_" + jobTime + "/random-data-in";
		final String fileOutputDir = "/tmp/kmeans_" + jobTime + "/output";

		final Path srcFilePath = new Path(srcFileName);
		final int numPoints = 1000;
		final int range = 100;

		new SphereRandomPointGenerator(10, 5).generateSourceFile(fileSys,
				srcFilePath, numPoints, range);

		conf.set(KMeansCluster.CONF_FILE_SOURCE, srcFilePath.toString());
		conf.set(KMeansCluster.CONF_FILE_OUT, fileOutputDir);

		System.out.println("Starting job");

		if (bsp.waitForCompletion(true)) {
			System.out.println("Done!");
		}

		String localOut = "/tmp/" + jobTime + "/local/";

		fileSys.copyToLocalFile(new Path(fileOutputDir), new Path(localOut));
	
		//fileSys.listStatus()

		System.out.println("Output in: " + new Path(localOut));

	}

}
