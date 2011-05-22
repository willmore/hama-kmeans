package ee.ut.cs.willmore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SphereRandomPointGenerator implements PointGenerator {

	private final Random random = new Random();
	private int numSpheres;
	private double radius;
	
	private static class Sphere {
		
		private final Random random = new Random();
		
		private Point3D center;
		private double radius;

		Sphere(Point3D center, double radius) {
			this.center = center;
			this.radius = radius;
		}
		
		/**
		 * 
		 * @return Random point within sphere
		 */
		Point3D randomPoint() {
			
			while (true) {
				double x = (center.x - radius) + random.nextDouble() * (2 * radius);
				double y = (center.y - radius) + random.nextDouble() * (2 * radius);
				double z = (center.z - radius) + random.nextDouble() * (2 * radius);
				Point3D p = new Point3D(x,y,z);
				if (p.distance(center) <= radius) {
					return p;
				}
			}
			
		}
	}
	
	
	
	
	public SphereRandomPointGenerator(int numSpheres, double radius) {
		
		this.numSpheres = numSpheres;
		this.radius = radius;
		
	}
	
	/**
	 * Create a randomly generated dataset of points saved on the Hadoop
	 * file-system at the specified {@code fileName}. 
	 * {@code numPoints} of 3D double-value points, with X,Y,Z each within [0,{@code range}).
	 * 
	 * @param fileSys
	 * @param fileName
	 * @param numPoints
	 * @param range
	 * @throws IOException
	 */
	@Override
	public void generateSourceFile(FileSystem fileSys, Path fileName, int numPoints, int range) throws IOException {
		
		final FSDataOutputStream out = fileSys.create(fileName, true);
		
		out.writeInt(numPoints);
		
		final List<Sphere> spheres = new ArrayList<Sphere>(numSpheres);
		
		for (int i = 0; i < numSpheres; i++) {
			
			spheres.add(new Sphere(new Point3D(random.nextDouble()*range, 
											   random.nextDouble()*range, 
											   random.nextDouble()*range),
									radius));
		}
		
		for (int i = 0; i < numPoints; i++) {
			
			Sphere randomSphere = spheres.get(random.nextInt(numSpheres));
			Point3D randomPoint = randomSphere.randomPoint();
			out.writeDouble(randomPoint.x); // X
			out.writeDouble(randomPoint.y); // Y 
			out.writeDouble(randomPoint.z); // Z		
		}
		
		out.close();
	}

}
