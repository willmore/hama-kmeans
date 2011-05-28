package ee.ut.cs.willmore;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public interface PointGenerator {

	/**
	 * Create dataset of points saved on the Hadoop
	 * file-system at the specified {@code fileName}. 
	 * {@code numPoints} of 3D double-value points, with dimensions X,Y and Z each within [0,{@code range}).
	 * 
	 * Noise is a number of extra random points in range.
	 * 
	 * @param fileSys
	 * @param fileName
	 * @param numPoints
	 * @param range
	 * @param dimensions 
	 * @throws IOException
	 */ 
	void generateSourceFile(FileSystem fileSys, Path fileName, int numPoints,
			int range, int noise, int dimensions) throws IOException;
	
}
