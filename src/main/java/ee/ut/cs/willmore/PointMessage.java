package ee.ut.cs.willmore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hama.bsp.BSPMessage;

public class PointMessage extends BSPMessage {
	
	private String tag;
	private List<Point3D> points;
	
	public PointMessage(String tag, List<Point3D> points) {
		this.tag = tag;
		this.points = points;
	}
	
	public PointMessage(String tag, Point3D point) {
		this.tag = tag;
		this.points = new ArrayList<Point3D>(1);
		this.points.add(point);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		byte tagBytes[] = new byte[in.readInt()];
	    in.readFully(tagBytes, 0, tagBytes.length);
	    this.tag = new String(tagBytes);
	    
	    int numPoints = in.readInt();
	    this.points = new ArrayList<Point3D>(numPoints);
	    for (int i = 0; i < numPoints; i++) {
	    	this.points.add(new Point3D(in.readDouble(), in.readDouble(), in.readDouble()));
	    }
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBytes(tag);
		for (Point3D p : points) {
			out.writeDouble(p.x);
			out.writeDouble(p.y);
			out.writeDouble(p.z);
		}

	}

	@Override
	public String getTag() {
		return this.tag;
	}

	@Override
	public List<Point3D> getData() {
		return this.points;
	}

}
