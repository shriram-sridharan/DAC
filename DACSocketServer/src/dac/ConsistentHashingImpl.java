package dac;

import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

// This implementation found on web does not consider virtual nodes placed near one another.
// We live with this implementation for now.
public class ConsistentHashingImpl implements IConsistentHashing {

	private final int numberOfReplicas;
	private final SortedMap<Integer, String> circle = new TreeMap<Integer, String>();

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
	
	@Override
	public ArrayList<String> getNodesToReplicate(String key, int numberOfReplicas) {
		if (circle.isEmpty()) {
			return null;
		}
		
		assert(numberOfReplicas < circle.size());
		int hash = key.hashCode();
		SortedMap<Integer, String> tailMap = circle.tailMap(hash);

		// relying on sortedness
		Collection<String> values = tailMap.isEmpty() ? circle.values()	: tailMap.values();
		ArrayList<String> nodesToReplicate = new ArrayList<String>();
		for (String node : values) {
			if (numberOfReplicas <= 0)
				break;

			nodesToReplicate.add(node);
			numberOfReplicas--;
		}
		
		//wrap around case.
		if(numberOfReplicas > 0)
		{
			for (String node : circle.values()) {
				if (numberOfReplicas <= 0)
					break;

				nodesToReplicate.add(node);
				numberOfReplicas--;
			}
		}
		
		assert(nodesToReplicate.size() == numberOfReplicas);
		return nodesToReplicate;
	}

}