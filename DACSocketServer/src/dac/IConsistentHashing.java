package dac;

public interface IConsistentHashing {

	public abstract void add(String node);

	public abstract void remove(String node);

	public abstract String get(String key);

}