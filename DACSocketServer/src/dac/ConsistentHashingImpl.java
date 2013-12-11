package dac;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

// This implementation found on web does not consider virtual nodes placed near one another.
// We live with this implementation for now.
public class ConsistentHashingImpl implements IConsistentHashing {

	private final int numberOfReplicas;
	private final SortedMap<String, String> circle = new TreeMap<String, String>();

	public ConsistentHashingImpl(int numberOfReplicas) {
		this.numberOfReplicas = numberOfReplicas;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dac.IConsistentHashing#add(java.lang.String)
	 */
	@Override
	public void add(String node) {
		
		try {
			for (int i = 0; i < numberOfReplicas; i++) {
				MessageDigest md = MessageDigest.getInstance("MD5");
				md.update((node + i).getBytes());
				byte byteData[] = md.digest();
				StringBuffer sb = new StringBuffer();
		        for (int j = 0; j < byteData.length; j++) {
		         sb.append(Integer.toString((byteData[j] & 0xff) + 0x100, 16).substring(1));
		        }
				circle.put(sb.toString(), node); 
			}
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dac.IConsistentHashing#remove(java.lang.String)
	 */
	@Override
	public void remove(String node) {
		try {
			for (int i = 0; i < numberOfReplicas; i++) {
				MessageDigest md = MessageDigest.getInstance("MD5");
				md.update((node + i).getBytes());
				byte byteData[] = md.digest();
				StringBuffer sb = new StringBuffer();
		        for (int j = 0; j < byteData.length; j++) {
		         sb.append(Integer.toString((byteData[j] & 0xff) + 0x100, 16).substring(1));
		        }
				circle.remove(sb.toString());
			}
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dac.IConsistentHashing#get(java.lang.String)
	 */
	@Override
	public String get(String key) {
		try {
			if (circle.isEmpty()) {
				return null;
			}
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(key.getBytes());
			byte byteData[] = md.digest();
			StringBuffer sb = new StringBuffer();
	        for (int j = 0; j < byteData.length; j++) {
	         sb.append(Integer.toString((byteData[j] & 0xff) + 0x100, 16).substring(1));
	        }
			String hash = sb.toString();
			if (!circle.containsKey(hash)) {
				SortedMap<String, String> tailMap = circle.tailMap(hash);
				hash = tailMap.isEmpty() ? circle.firstKey() : tailMap
						.firstKey();
			}
			return circle.get(hash);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void print() {
		System.out.println("\n[CONSISTENT HASHING] Printing Consistent Hash Table");
		for(String node: circle.keySet())
			System.out.println(node + " - " + circle.get(node));
	}
	
	@Override
	public ArrayList<String> getNodesToReplicate(String key, int numberOfReplicas) {
		if (circle.isEmpty()) {
			return null;
		}
		
		assert(numberOfReplicas < circle.size());
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(key.getBytes());
			byte byteData[] = md.digest();
			StringBuffer sb = new StringBuffer();
	        for (int j = 0; j < byteData.length; j++) {
	         sb.append(Integer.toString((byteData[j] & 0xff) + 0x100, 16).substring(1));
	        }
			String hash = sb.toString();
			SortedMap<String, String> tailMap = circle.tailMap(hash);

			// relying on sortedness
			Collection<String> values = tailMap.isEmpty() ? circle.values()
					: tailMap.values();
			ArrayList<String> nodesToReplicate = new ArrayList<String>();
			for (String node : values) {
				if (numberOfReplicas <= 0)
					break;

				nodesToReplicate.add(node);
				numberOfReplicas--;
			}

			// wrap around case.
			if (numberOfReplicas > 0) {
				for (String node : circle.values()) {
					if (numberOfReplicas <= 0)
						break;

					nodesToReplicate.add(node);
					numberOfReplicas--;
				}
			}

			assert (nodesToReplicate.size() == numberOfReplicas);
			return nodesToReplicate;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		}
	}

}