package dac;

public class RingNode  {

	private final double value;
	private final String physicalIpAddress;

	public RingNode(double value, String physicalIpAddress){
		this.value = value;
		this.physicalIpAddress = physicalIpAddress;
	}

	public double getValue() {
		return value;
	}

	public String getPhysicalIpAddress() {
		return physicalIpAddress;
	}
}
