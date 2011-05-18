package ee.ut.cs.willmore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeerProtocol;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.DoubleMessage;
import org.apache.zookeeper.KeeperException;

public class KMeansCluster {
	

	private static final String CONF_FILE_OUT = "output.";
	private static String CONF_MASTER_TASK = "master.task.";
	private static String CONF_FILE_SOURCE = "source.";


	public static class ClusterBSP extends BSP {
		
		public static final Log LOG = LogFactory.getLog(ClusterBSP.class);
		private Configuration conf;
		private FileSystem fileSys;
		//private int numPeers;
		private String masterTask;

		//Map of peer name => cluster center (mean)
		final Map<String, Point3D> peerMeanMap = new HashMap<String, Point3D>();
		
		//All points currently in my cluster
		final List<Point3D> points = new ArrayList<Point3D>();
		
		@Override
		public void bsp(BSPPeerProtocol bspPeer) throws IOException,
				KeeperException, InterruptedException {

			
			if (isMaster(bspPeer)) {
				masterInitialize(bspPeer);	
			}
			
			do {	
				bspPeer.sync();
				
				receivePeerMeans(bspPeer);
				
				int numChange = assignmentStep(bspPeer);
				bspPeer.sync();

				updateStep(bspPeer);
				
				notifyMasterNumChange(bspPeer, numChange);
				
				bspPeer.sync();
				
				if (isMaster(bspPeer)) {
					broadcastContinue(bspPeer);
				}

				bspPeer.sync();
				
				broadcastMyMean(bspPeer);
				
			} while (shouldContinue(bspPeer));
			
			writeFinalOutput(bspPeer);
			
		}

		private void writeFinalOutput(BSPPeerProtocol bspPeer) throws IOException {
			String fileName = conf.get(CONF_FILE_OUT) + "-" + bspPeer.getPeerName();
			
			PointWriter writer = new PointWriter(fileSys.create(new Path(fileName), true));
			
			writer.write(calculateCenter(points));
			writer.write(points);
			
			writer.close();
		}

		private void notifyMasterNumChange(BSPPeerProtocol bspPeer,
				int numChange) throws IOException {
			//Notify master of #of changes
			bspPeer.send(masterTask, new DoubleMessage("CHANGE_COUNT", numChange));
		}

		private void masterInitialize(BSPPeerProtocol bspPeer) throws IOException {
		 
			Path srcFilePath = new Path(conf.get(CONF_FILE_SOURCE));
			System.out.println(conf.get(CONF_FILE_SOURCE));
			if (!fileSys.exists(srcFilePath)) {
				throw new RuntimeException("Could not find source file:" + srcFilePath.getName());
			}
			
			FSDataInputStream srcFile = fileSys.open(srcFilePath);
			
			final int numPoints = srcFile.readInt();
			
			for (int i = 0; i < numPoints; i++) {
				points.add(new Point3D(srcFile.readDouble(), 
									   srcFile.readDouble(), 
									   srcFile.readDouble()));
			}
			
			//Assign one mean to each node
			//Means are chosen "randomly" from points

			int ctr = 0; 
			for (String peer : bspPeer.getAllPeerNames()) {
				Point3D p = points.get(ctr++);
				peerMeanMap.put(peer, p);
			}
			
			// Broadcast all peer => mean pairs
			for (String peer : bspPeer.getAllPeerNames()) {
				// Only send message to others (not myself)
				if (peer.equals(bspPeer.getPeerName())) {
					continue;
				}

				for (Map.Entry<String, Point3D> peerMean : peerMeanMap
						.entrySet()) {

					PointMessage msg = new PointMessage(peerMean.getKey(),
							peerMean.getValue());
					bspPeer.send(peer, msg);

				}
			}
		}
		
		private boolean shouldContinue(BSPPeerProtocol bspPeer) throws IOException {
			
			assert (1 == bspPeer.getNumCurrentMessages());
			
			return 1 == ((DoubleMessage)bspPeer.getCurrentMessage()).getData();	
		}

		/**
		 * Receive re-assignment counts from all peers. If sum > threshold, broadcast True, else False
		 * @throws IOException 
		 */
		private void broadcastContinue(BSPPeerProtocol bspPeer) throws IOException {
			
			int total = 0;
			
			DoubleMessage msg;
			while((msg = (DoubleMessage)bspPeer.getCurrentMessage()) != null) {
				total += msg.getData();
			}
			
			System.out.println("Total Changed = " + total);
			DoubleMessage bMsg = new DoubleMessage("COUNTINUE", total > 0 ? 1 : 0);
			
			for (String peer : bspPeer.getAllPeerNames()) {
				bspPeer.send(peer, bMsg);
			}
		}

		private void receivePeerMeans(BSPPeerProtocol bspPeer) throws IOException {
			
			PointMessage pMsg;
			while((pMsg = (PointMessage)bspPeer.getCurrentMessage()) != null) {
				assert(peerMeanMap.containsKey((String)pMsg.getTag()));
				peerMeanMap.put(pMsg.getTag(),pMsg.getData().get(0));
			}
						
		}

		//Receive my new points and update my mean, notifying peers of change
		private int assignmentStep(BSPPeerProtocol bspPeer) throws IOException {
						
			//For each of my points, find new best cluster by geometric distance.			
			Map<String, List<Point3D>> peerNewPoints = new HashMap<String, List<Point3D>>();
			
			for (String peer : peerMeanMap.keySet()) {
				peerNewPoints.put(peer, new ArrayList<Point3D>());
			}
			
			int changeCount = 0;
						
			for (Iterator<Point3D> pointItr = points.iterator(); pointItr.hasNext();) {
				
				Point3D obs = pointItr.next();
				
				double min = Double.MAX_VALUE;
				String minPeer = null;
				
				for (Map.Entry<String, Point3D> peer : peerMeanMap.entrySet()) {
					double distance = obs.distance(peer.getValue());
					
					if (distance < min) {
						min = distance;
						minPeer = peer.getKey();
					}
				}
				
				if (minPeer.equals(bspPeer.getPeerName())) {
					//I don't send updates for points I already own
					continue;
				}
				
				//Remove the point from my collection as I no longer own it.
				pointItr.remove();
				changeCount += 1;
				peerNewPoints.get(minPeer).add(obs);	
			}
			
			
			//Notify other clusters of new points	
			for (Map.Entry<String, List<Point3D>> peerPoints : peerNewPoints.entrySet()) {
				
				//System.out.println("Sending " + peerPoints.getValue().size() + " to " + peerPoints.getKey());
				bspPeer.send(peerPoints.getKey(), new PointMessage("POINTS", peerPoints.getValue()));
			}
				
			return changeCount;
		}

	
		/**
		 * Perform the KMeans Update Step. 
		 * {@link http://en.wikipedia.org/wiki/K-means_clustering#Standard_algorithm}
		 * 
		 * Receive PointMessages from peers that notify me of new points
		 * assigned to my cluster. Calculate the new geometrical center of my
		 * cluster.
		 * 
		 * @param bspPeer
		 * @throws IOException
		 */
		private void updateStep(BSPPeerProtocol bspPeer) throws IOException {	
			
			PointMessage pMsg;
			boolean changed = false;
			
			while((pMsg = (PointMessage)bspPeer.getCurrentMessage()) != null) {
				points.addAll(pMsg.getData());
				changed = true;
			}
						
			if (changed) {
				peerMeanMap.put(bspPeer.getPeerName(), calculateCenter(points));
			}
			
		}
		
		/**
		 * Send my mean as a PointMessage to all peers, except myself.
		 * @param bspPeer
		 * @throws IOException
		 */
		private void broadcastMyMean(BSPPeerProtocol bspPeer) throws IOException {
			
			for (String peer : bspPeer.getAllPeerNames()) {
				
				Point3D myMean = peerMeanMap.get(bspPeer.getPeerName());
				PointMessage msg = new PointMessage(bspPeer.getPeerName(), myMean);
				
				if (!peer.equals(bspPeer.getPeerName())) {
					bspPeer.send(peer, msg);
				}
			}
		}

		private Point3D calculateCenter(List<Point3D> points) {
			double x = 0;
			double y = 0;
			double z = 0;
			
			for (Point3D p : points) {
				x += p.x / points.size();
				y += p.y / points.size();
				z += p.z / points.size();
			}
			
			return new Point3D(x, y, z);
		}

		private boolean isMaster(BSPPeerProtocol bspPeer) {
			return bspPeer.getPeerName().equals(masterTask);
		}

		public Configuration getConf() {
			return conf;
		}

		public void setConf(Configuration conf) {
			this.conf = conf;
			this.masterTask = conf.get(CONF_MASTER_TASK);
			//numPeers = Integer.parseInt(conf.get("bsp.peers.num"));
			try {
				fileSys = FileSystem.get(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	
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
		ClusterStatus cluster = jobClient.getClusterStatus(false);

		// Choose one as a master
		for (String peerName : cluster.getActiveGroomNames().values()) {
			System.out.println("Master Peer:" + peerName);
			conf.set(CONF_MASTER_TASK, peerName);
			break;
		}
		
		bsp.setNumBspTask(cluster.getGroomServers());

		FileSystem fileSys = FileSystem.get(conf);
		
		final long jobTime = System.currentTimeMillis();
		
		final String srcFileName = "/tmp/random-data-in";
		final String fileOutputDir = "/tmp/random-data-out/" + jobTime + "/";
		
		final Path srcFilePath = new Path(srcFileName);
		final int numPoints = 10000;
		final int range = 100;
		//new CubeRandomPointGenerator(5, 10).generateSourceFile(fileSys, srcFilePath, numPoints, range);
		new SphereRandomPointGenerator(10, 5).generateSourceFile(fileSys, srcFilePath, numPoints, range);
		
		conf.set(CONF_FILE_SOURCE, srcFilePath.toString());
		conf.set(CONF_FILE_OUT, fileOutputDir);
		
		if (bsp.waitForCompletion(true)) {
			System.out.println("Done!");
		}
		
		String localOut = "/tmp/" + jobTime + "/local/";
		
		fileSys.copyToLocalFile(new Path(fileOutputDir), new Path(localOut));
		
		System.out.println("Output in: " + new Path(localOut));

	}

}

