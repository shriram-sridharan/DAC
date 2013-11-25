package dac;

import java.util.List;

public class ConsistentHashingImpl implements IConsistentHashing {

	private final List<String> serverIpAddresses;
	private final int virtualServerPerIp;
	private RingNode[] circle;

	public ConsistentHashingImpl(List<String> serverIpAddresses,
			int virtualServerPerIp) {
		this.serverIpAddresses = serverIpAddresses;
		this.virtualServerPerIp = virtualServerPerIp;
		this.circle = new RingNode[virtualServerPerIp
				* serverIpAddresses.size()];
	}

	@Override
	public String getServerLocationFor(String key) throws Exception {
		// I could use some SHA/MD5 message digest. But for now, keeping it simple
		int totalVirtualServers = virtualServerPerIp * serverIpAddresses.size();
		double hashedValue = (double)(Math.abs(key.hashCode()) % totalVirtualServers) / (double) totalVirtualServers;
		System.out.println("hashedval = " + hashedValue + " hashcode = " + key.hashCode() + " mod = " + Math.abs(key.hashCode()) % totalVirtualServers);
		return binarySearch(hashedValue, 0, totalVirtualServers - 1);
	}

	// this will work because the array is sorted.
	private String binarySearch(double hashedValue, int start, int end) throws Exception {
		if(start == end) {
			if(circle[start].getValue() >= hashedValue)
				return circle[start].getPhysicalIpAddress();
			else
				throw new Exception(
						"Unable to find value greater than the hashedValue = "
								+ hashedValue + " in totalVirtServes = "
								+ virtualServerPerIp * serverIpAddresses.size());
		}

		int mid = (start + end) / 2;
		if(circle[mid].getValue() < hashedValue)
			return binarySearch(hashedValue, mid + 1, end);
		else 
			return binarySearch(hashedValue, start, mid);
	}
	

	@Override
	public void initializeConsistentHashRing() throws Exception {
		int totalVirtualServers = virtualServerPerIp * serverIpAddresses.size();
		if(serverIpAddresses.size() == 1)
			throw new Exception("Need more than one server");
		int prevPhysicalServer = -1;
		int remainingPhysicalServer[] = new int[serverIpAddresses.size()];
		
		for (int i = 0; i < serverIpAddresses.size(); i++)
			remainingPhysicalServer[i] = virtualServerPerIp;
		
		for (int i = 0; i < totalVirtualServers; i++) {
			int nextPhysicalServer = (int) (Math.random() * serverIpAddresses.size());
			if (prevPhysicalServer == nextPhysicalServer || remainingPhysicalServer[nextPhysicalServer] == 0) {
				if(i >= totalVirtualServers - totalVirtualServers/virtualServerPerIp) { 
					// last part of virtual servers left
					// Might go in an infinite loop if prev = next. 
					// So, we backtrack to totalVirtualServ/virtualServerPerIp
					for(int j = 1; j <= totalVirtualServers/virtualServerPerIp; j++) {
						int indexOfPhysicalIp = serverIpAddresses.indexOf(circle[i - j].getPhysicalIpAddress());
						remainingPhysicalServer[indexOfPhysicalIp]++;
					}
					i = i - 1 - totalVirtualServers/virtualServerPerIp;
				}
				else {
					i--;
				}
			} else {
				circle[i] = new RingNode((double) i / totalVirtualServers,
						serverIpAddresses.get(nextPhysicalServer));
				prevPhysicalServer = nextPhysicalServer;
				remainingPhysicalServer[nextPhysicalServer]--;
			}
		}
	}
	
	@Override
	public void printCircle() {
		// print
		int totalVirtualServers = virtualServerPerIp * serverIpAddresses.size();
		int total[] = new int[serverIpAddresses.size()];
		for (int i = 0; i < totalVirtualServers; i++) {
			System.out.println("Ring Node i = " + i + " value = "
					+ circle[i].getValue() + " phip = "
					+ circle[i].getPhysicalIpAddress());
			total[serverIpAddresses.indexOf(circle[i].getPhysicalIpAddress())]++;
		}
		for(int i =0 ;i<serverIpAddresses.size();i++)
			System.out.println("Total [i" + i + "] = " + total[i]);
	}
}
