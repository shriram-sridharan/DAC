

package com.yahoo.ycsb.db;
import org.zeromq.ZMQ;

public  class GlueZMQ {
	private static String IPAddress;
	private static String portNumber;
	
	public static boolean isAuthorized(String request) {
		/*
		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket socket = context.socket(ZMQ.REQ);
		socket.connect ("tcp://" + IPAddress+ ":"+portNumber);
		System.out.println("Sending  " + request );
		socket.send(request.getBytes (), 0);
		byte[] reply = socket.recv(0);
		System.out.println("Received " + new String (reply));
		socket.close();
		context.term();
		String replyString = new String(reply);
		if(replyString.trim().equalsIgnoreCase("yes")) {
			return true;
		} else {
			return false;
		}*/
		return true;
	}
	/**
	 * @param iPAddres the iPAddres to set
	 */
	public static void setIPAddres(String iPAddres) {
		IPAddress = iPAddres;
	}

	/**
	 * @return the iPAddres
	 */
	public static String getIPAddres() {
		return IPAddress;
	}

	/**
	 * @param portNumber the portNumber to set
	 */
	public static void setPortNumber(String portNumber) {
		GlueZMQ.portNumber = portNumber;
	}

	/**
	 * @return the portNumber
	 */
	public static String getPortNumber() {
		return portNumber;
	}

}
