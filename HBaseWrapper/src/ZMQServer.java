import org.zeromq.ZMQ;



public class ZMQServer {
	public static void main(String[] args) {
		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket socket = context.socket(ZMQ.REP);
		socket.bind ("tcp://*:15555");

		while (!Thread.currentThread ().isInterrupted ()) {
		byte[] reply = socket.recv(0);
		System.out.println("Received " + new String(reply));
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // Do some 'work'
		String request = "Yes" ;
		socket.send(request.getBytes (), 0);
		}
		socket.close();
		context.term();
		
		
	}

}
