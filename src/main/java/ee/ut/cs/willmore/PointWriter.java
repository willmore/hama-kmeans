package ee.ut.cs.willmore;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.FSDataOutputStream;

public class PointWriter {

	
	private final FSDataOutputStream out;

	public PointWriter(FSDataOutputStream outputStream) {
		this.out = outputStream;
	}
	
	public void write(Point3D point) throws IOException {
		out.writeDouble(point.x); 
		out.writeDouble(point.y); 
		out.writeDouble(point.z); 
	}
	
	public void write(Collection<Point3D> points) throws IOException {
		for (Point3D p : points) {
			write(p);
		}
	}
	
	public void close() throws IOException {
		out.close();
	}
}
