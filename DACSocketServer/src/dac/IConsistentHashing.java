package dac;

import java.util.ArrayList;

public interface IConsistentHashing {

	public abstract void add(String node);

	public abstract void remove(String node);

	public abstract String get(String key);

	public abstract ArrayList<String> getNodesToReplicate(String key, int numberOfReplicas);

}