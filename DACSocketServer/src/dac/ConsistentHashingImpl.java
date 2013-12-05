package dac;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

// This implementation found on web does not consider virtual nodes placed near one another.
// We live with this implementation for now.
public class ConsistentHashingImpl implements IConsistentHashing {

	private final int numberOfReplicas;
	private final SortedMap<Integer, String> circle = new TreeMap<Integer, String>();

	public ConsistentHashingImpl(int numberOfReplicas, Collection<String> nodes) {
		this.numberOfReplicas = numberOfReplicas;

		for (String node : nodes) {
			add(node);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dac.IConsistentHashing#add(java.lang.String)
	 */
	@Override
	public void add(String node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.put((node.toString() + i).hashCode(), node); // should use
																// MD5 hash here
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dac.IConsistentHashing#remove(java.lang.String)
	 */
	@Override
	public void remove(String node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.remove((node.toString() + i).hashCode());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dac.IConsistentHashing#get(java.lang.String)
	 */
	@Override
	public String get(String key) {
		if (circle.isEmpty()) {
			return null;
		}
		int hash = key.hashCode();
		if (!circle.containsKey(hash)) {
			SortedMap<Integer, String> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}

}