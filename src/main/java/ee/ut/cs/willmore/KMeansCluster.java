package ee.ut.cs.willmore;

import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.apache.hama.bsp.BSPMessage;
import org.apache.hama.bsp.BSPPeerProtocol;
import org.apache.hama.bsp.ClusterStatus;
//import org.apache.hama.bsp.DoubleMessage;
import org.apache.zookeeper.KeeperException;

public class KMeansCluster {
	
	static final String CONF_FILE_OUT = "output.";
	static String CONF_MASTER_TASK = "master.task.";
	static String CONF_FILE_SOURCE = "source.";

	public static class ClusterBSP extends BSP {
		
		public static final Log LOG = LogFactory.getLog(ClusterBSP.class);
		private Configuration conf;
		private FileSystem fileSys;
		private String masterTask;

		//Map of peer name => cluster center (mean)
		final Map<String, Point3D> peerMeanMap = new HashMap<String, Point3D>();
		
		//All points currently in my cluster
		final List<Point3D> points = new ArrayList<Point3D>();
				
		@Override
		public void bsp(final BSPPeerProtocol bspPeer) throws IOException,
				KeeperException, InterruptedException {
		
			if (isMaster(bspPeer)) {
				masterInitialize(bspPeer);	
			}
			
			do {
				assert (0 == bspPeer.getNumCurrentMessages());
				bspPeer.sync();
				
				receivePeerMeans(bspPeer);
				
				int numChange = assignmentStep(bspPeer);
				
				assert (0 == bspPeer.getNumCurrentMessages());
				bspPeer.sync();
				
				updateStep(bspPeer);
				
				notifyMasterNumChange(bspPeer, numChange);
				
				assert (0 == bspPeer.getNumCurrentMessages());
				bspPeer.sync();
				
				if (isMaster(bspPeer)) {
					broadcastContinue(bspPeer);
				}
				
				assert (0 == bspPeer.getNumCurrentMessages());
				bspPeer.sync();
				
				broadcastMyMean(bspPeer);
				
			} while (shouldContinue(bspPeer));
			
			//Empty inbox as any messages are now unnecessary
			flushReceivedMessages(bspPeer);
			
			writeFinalOutput(bspPeer);
			
		}

		private void flushReceivedMessages(BSPPeerProtocol bspPeer) throws IOException {
			
			LOG.info("Flushing inbox");
			while(bspPeer.getNumCurrentMessages() > 0) {
				bspPeer.getCurrentMessage();
			}
			
		}

		private void writeFinalOutput(final BSPPeerProtocol bspPeer) throws IOException {
			
			
			final String fileName = conf.get(CONF_FILE_OUT) + "/" + bspPeer.getPeerName().replace(":", "_");
			
			LOG.info("Writing final output to: " + fileName);
			
			PointWriter writer = new PointWriter(fileSys.create(new Path(fileName), true));
			
			writer.write(calculateCenter(points));
			writer.write(points);
			
			writer.close();
		}
		
		private static BSPMessage pointToByteMessage(PointMessage pm) throws IOException {
			
			
			ByteBuffer buffer = ByteBuffer.allocate(pm.getData().size() * 3 * 8);
		    
			for (Point3D p : pm.getData()) {
				buffer.putDouble(p.x);
				buffer.putDouble(p.y);
				buffer.putDouble(p.z);
			}
		   
			return new BSPMessage(pm.getTag().getBytes(), buffer.array());
			
		}
		

		private static PointMessage byteToPointMessage(BSPMessage bMsg) throws IOException {
						
			
			ByteBuffer buffer = ByteBuffer.wrap(bMsg.getData());
			
			List<Point3D> points = new ArrayList<Point3D>();
			
			while (buffer.hasRemaining()) {	
				points.add(new Point3D(buffer.getDouble(), buffer.getDouble(), buffer.getDouble()));
			}
		
			return new PointMessage(new String(bMsg.getTag()), points);
			
		}
		

		/**
		 * Notify master of number of point ownership changes
		 */
		private void notifyMasterNumChange(final BSPPeerProtocol bspPeer,
										   final int numChange) throws IOException {
			
			LOG.info("Notifying master of point change count: " + numChange);
			
			bspPeer.send(masterTask, new BSPMessage("CHANGE_COUNT".getBytes(), Integer.toString(numChange).getBytes()));
		}

		private void masterInitialize(final BSPPeerProtocol bspPeer) throws IOException {

			LOG.info("Starting Master");
			
			final Path srcFilePath = new Path(conf.get(CONF_FILE_SOURCE));

			if (!fileSys.exists(srcFilePath)) {
				throw new RuntimeException("Could not find source file:" + srcFilePath.getName());
			}
			
			final FSDataInputStream srcFile = fileSys.open(srcFilePath);
			
			final int numPoints = srcFile.readInt();
			
			LOG.info("Number of points is: " + numPoints);
			
			for (int i = 0; i < numPoints; i++) {
				points.add(new Point3D(srcFile.readDouble(), 
									   srcFile.readDouble(), 
									   srcFile.readDouble()));
			}
			
			//Assign one mean to each node
			//Means are chosen "randomly" from points

			int ctr = 0; 
			for (final String peer : bspPeer.getAllPeerNames()) {
				Point3D p = points.get(ctr++);
				peerMeanMap.put(peer, p);
			}
			
			// Broadcast all peer => mean pairs
			for (final String peer : bspPeer.getAllPeerNames()) {
				// Only send message to others (not myself)
				if (peer.equals(bspPeer.getPeerName())) {
					continue;
				}

				for (final Map.Entry<String, Point3D> peerMean : peerMeanMap
						.entrySet()) {

					
					PointMessage msg = new PointMessage(peerMean.getKey(),
							peerMean.getValue());
					bspPeer.send(peer, pointToByteMessage(msg));

				}
			}
			
			LOG.info("Initial point messages sent to peers");
		}
		
		
		/**
		 * Master reads the total number of changed messages and returns true if
		 * the process should continue, else false.
		 */
		private boolean shouldContinue(final BSPPeerProtocol bspPeer) throws IOException {
			
			//assert (1 == bspPeer.getNumCurrentMessages());
			
			LOG.info("Testing if should continue, number of messages should be 1 and are: " + 
					bspPeer.getNumCurrentMessages());
			
			return 1 == bspPeer.getCurrentMessage().getData()[0];	
		}

		/**
		 * Receive re-assignment counts from all peers. If sum > threshold, broadcast True, else False
		 * @throws IOException 
		 */
		private void broadcastContinue(final BSPPeerProtocol bspPeer) throws IOException {
			
			int total = 0;
			
			BSPMessage msg;
			while((msg = bspPeer.getCurrentMessage()) != null) {
				
				total += Integer.valueOf(new String(msg.getData()));
			}
			
			final boolean continueProcess = total > 0;
			
			
			final BSPMessage bMsg = new BSPMessage("CONTINUE".getBytes(), continueProcess ? new byte[]{1} : new byte[]{0});
			
			LOG.info("Master sending continue msg: " +  continueProcess);
			
			for (final String peer : bspPeer.getAllPeerNames()) {
				bspPeer.send(peer, bMsg);
			}
		}

		/**
		 * Process notification about the mean values of peers.
		 */
		private void receivePeerMeans(final BSPPeerProtocol bspPeer) throws IOException {
			
			BSPMessage bMsg;
			while((bMsg = bspPeer.getCurrentMessage()) != null) {
				PointMessage pMsg = byteToPointMessage(bMsg);
				assert(peerMeanMap.containsKey((String)pMsg.getTag()));
				peerMeanMap.put(pMsg.getTag(),pMsg.getData().get(0));
			}
						
		}

		//Receive my new points and update my mean, notifying peers of change
		private int assignmentStep(final BSPPeerProtocol bspPeer) throws IOException {
						
			//For each of my points, find new best cluster by geometric distance.			
			final Map<String, List<Point3D>> peerNewPoints = new HashMap<String, List<Point3D>>();
			
			for (String peer : peerMeanMap.keySet()) {
				peerNewPoints.put(peer, new ArrayList<Point3D>());
			}
			
			int changeCount = 0;
						
			for (Iterator<Point3D> pointItr = points.iterator(); pointItr.hasNext();) {
				
				final Point3D obs = pointItr.next();
				
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
				
				if (peerPoints.getValue().size() == 0) {
					continue;
				}
				
				LOG.info("Send " + peerPoints.getValue().size() + " to " + peerPoints.getKey());
				bspPeer.send(peerPoints.getKey(), 
						     pointToByteMessage(new PointMessage("POINTS from " + bspPeer.getPeerName(), peerPoints.getValue())));
			}
				
			return changeCount;
		}

	
		/**
		 * Perform the KMeans Update Step. 
		 * {@link http://en.wikipedia.org/wiki/K-means_clustering#Standard_algorithm}
		 * 
		 * Receive PointMessages from peers that notify me of new points
		 * assigned to my cluster. Calculate the new geometric center of my
		 * cluster.
		 * 
		 * @param bspPeer
		 * @throws IOException
		 */
		private void updateStep(BSPPeerProtocol bspPeer) throws IOException {	
			
			BSPMessage bMsg;
			boolean changed = false;
			LOG.info("Start processing point messages");
			while((bMsg = bspPeer.getCurrentMessage()) != null) {
				final PointMessage pMsg = byteToPointMessage(bMsg);
				LOG.info("Received " + pMsg.getData().size() + " points from tag = " + pMsg.getTag());
				points.addAll(pMsg.getData());
				changed = true;
			}
			
			LOG.info("My point count is now: " + points.size());
						
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
					bspPeer.send(peer, pointToByteMessage(msg));
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
			
			LOG.info("Testing if me (" + bspPeer.getPeerName() + ") is master: " +  
					bspPeer.getPeerName().equals(masterTask));
			
			return bspPeer.getPeerName().equals(masterTask);
		}

		public Configuration getConf() {
			return conf;
		}

		public void setConf(Configuration conf) {
			this.conf = conf;
			this.masterTask = conf.get(CONF_MASTER_TASK);

			try {
				fileSys = FileSystem.get(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	
	public static void main(String[] args) throws InterruptedException,
			IOException, ClassNotFoundException {
		
		if (2 != args.length) {
			System.out.println("Usage: KMeansCluster <num_points> <num_clusters>");
			System.exit(-1);
		}
		
		final int k = Integer.valueOf(args[1]);
		
		// BSP job configuration
		HamaConfiguration conf = new HamaConfiguration();

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
			conf.set(CONF_MASTER_TASK, peerName);
			break;
		}

		System.out.println("Setting number of tasks / clusters to:" + cluster.getGroomServers());
		
		if (k > cluster.getGroomServers()) {
			System.out.println("Request K of " + k + " is greater than number of grooms " + cluster.getGroomServers());
			System.exit(-1);
		}
		
		bsp.setNumBspTask(cluster.getGroomServers());

		FileSystem fileSys = FileSystem.get(conf);

		final long jobTime = System.currentTimeMillis();

		final String srcFileName = "/tmp/kmeans_" + jobTime + "/random-data-in";
		final String fileOutputDir = "/tmp/kmeans_" + jobTime + "/output";

		final Path srcFilePath = new Path(srcFileName);
		final int numPoints = Integer.valueOf(args[0]);
		
		final int range = 100; //Size of X,Y,Z cube containing points

		new SphereRandomPointGenerator(k, 10).generateSourceFile(fileSys, srcFilePath, numPoints, range);

		conf.set(CONF_FILE_SOURCE, srcFilePath.toString());
		conf.set(CONF_FILE_OUT, fileOutputDir);
		
		System.out.println("Src data at: " + srcFileName);
		System.out.println("Out data at: " + fileOutputDir);
		System.out.println("Starting job");
		
		if (bsp.waitForCompletion(true)) {
			System.out.println("Done!");
		}

		String localOut = "/tmp/" + jobTime + "/local/";

		fileSys.copyToLocalFile(new Path(fileOutputDir), new Path(localOut));

		System.out.println("Output in: " + new Path(localOut));

	}

}

