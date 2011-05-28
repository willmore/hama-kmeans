package ee.ut.cs.willmore;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RandomPointGenerator implements PointGenerator {

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
	public void generateSourceFile(FileSystem fileSys, Path fileName, int numPoints, int range, int noise, int dimensions) throws IOException {
		
		final FSDataOutputStream out = fileSys.create(fileName, true);
		final Random random = new Random();
		
		out.writeInt(numPoints);
		
		for (int i = 0; i < numPoints; i++) {
			
			out.writeDouble(random.nextDouble()*range); // X
			out.writeDouble(random.nextDouble()*range); // Y 
			out.writeDouble(dimensions == 2 ? 0 : random.nextDouble()*range); // Z		
		}
		
		out.close();
	}

}
