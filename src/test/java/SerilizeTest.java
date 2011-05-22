import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hama.bsp.BSPMessage;

import ee.ut.cs.willmore.Point3D;
import ee.ut.cs.willmore.PointMessage;


public class SerilizeTest {

	
	private static BSPMessage pointToByteMessage(PointMessage pm) throws IOException {
		
		ByteBuffer buffer = ByteBuffer.allocate(pm.getData().size() * 3 * 16);
	    
		
		for (Point3D p : pm.getData()) {
			buffer.putDouble(p.x);
			buffer.putDouble(p.y);
			buffer.putDouble(p.z);
		}
	   
		return new BSPMessage(pm.getTag().getBytes(), buffer.array());
	}
	

	private static PointMessage byteToPointMessage(BSPMessage bm) throws IOException {
		
		ByteBuffer buffer = ByteBuffer.wrap(bm.getData());
		
		List<Point3D> points = new ArrayList<Point3D>();
		
		while (buffer.hasRemaining()) {	
			points.add(new Point3D(buffer.getDouble(), buffer.getDouble(), buffer.getDouble()));
		}
	
		return new PointMessage(new String(bm.getTag()), points);
	}
	
	public static void main(String args[]) throws IOException{
		
		Point3D point = new Point3D(-23.2, 12.2, 67.2);
		List<Point3D> points = new ArrayList<Point3D>();
		points.add(point);
		points.add(point);
		PointMessage pm = new PointMessage("Test", points);
		
		PointMessage p2 = byteToPointMessage(pointToByteMessage(pm));
		
		System.out.println(p2.getData().get(0));
		System.out.println(p2.getData().get(1));
		
	}
	
}
