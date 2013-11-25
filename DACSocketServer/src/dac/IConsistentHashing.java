package dac;

public interface IConsistentHashing {

	public String getServerLocationFor(String key) throws Exception;
	public void initializeConsistentHashRing() throws Exception;
	public abstract void printCircle();
}
