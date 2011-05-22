package ee.ut.cs.willmore;

import static java.lang.Math.sqrt;
import static java.lang.Math.pow;

public class Point3D {

	public final double x;
	public final double y;
	public final double z;

	public Point3D(double x, double y, double z) {
		this.x = x;
		this.y = y;
		this.z = z;
	}

	public double distance(Point3D that) {
		return sqrt(pow(this.x - that.x, 2) +
				    pow(this.y - that.y, 2) +
				    pow(this.z - that.z, 2));
	}

	@Override
	public String toString() {
		return "Point [x=" + x + ", y=" + y + ", z=" + z + "]";
	}

}
