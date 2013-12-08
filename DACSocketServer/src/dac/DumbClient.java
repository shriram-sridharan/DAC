package dac;

import java.util.Random;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class DumbClient {

	    public static void main(String[] args) throws Exception
	    {
	    	new DumbClient().dumbClient("tcp://*:5555"); // frontend
	    }

		public void dumbClient(String connectendpoint) throws InterruptedException {
			ZContext ctx = new ZContext();
	        Socket worker = ctx.createSocket(ZMQ.REQ);
	        Random rand = new Random(System.nanoTime());
	        String identity = String.format("%04X-%04X", rand.nextInt(0x10000), rand.nextInt(0x10000));
	        worker.setIdentity(identity.getBytes());
	        worker.connect(connectendpoint);
	        System.out.println(" My id = " + identity);
//	        String rowid = "www.cnn.com";
	        while(true){
	        	Thread.sleep(1000);
//	        	rowid+= Math.random();
//	        	ZFrame z = new ZFrame("PUT;" +rowid + ";anchor:www.shriram.com;1110111110");
//		        z.send(worker, 0);
	        	
	        	ZFrame z = new ZFrame("GET;usertable;www.abc.com2;age;0111111001");
		        z.send(worker, 0);
		        System.out.println("Sent message - waiting for reply");
		        ZFrame recvFrame = ZFrame.recvFrame(worker);
	            if(recvFrame == null)
	            	break;
	        
	            System.out.println("Dumb client received - " + new String(recvFrame.getData()));
	        }
	        
	        ctx.destroy();
		}
}
