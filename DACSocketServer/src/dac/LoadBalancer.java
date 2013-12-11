package dac;

import java.util.ArrayList;
import java.util.Iterator;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

//
// Paranoid Pirate queue
//

public class LoadBalancer extends Thread {
	private class Worker {
		ZFrame address; // Address of worker
		String identity; // Printable identity
		long expiry; // Expires at this time
		private String storageNodeSocket;

		protected Worker(ZFrame address, String storageNodeSocket) {
			this.address = address;
			this.storageNodeSocket = storageNodeSocket;
			identity = new String(address.getData());
			expiry = System.currentTimeMillis() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;
		}
	};

	private final static int HEARTBEAT_LIVENESS = 3; // 3-5 is reasonable
	private final static int HEARTBEAT_INTERVAL = 1000; // msecs
	private final static String PPP_READY = "\001"; // Signals worker is ready
	private final static String PPP_HEARTBEAT = "\002"; 
	private static int nextWorkerToGet = 0;
	
	ArrayList<Worker> workers = new ArrayList<Worker>();
	private ZContext ctx;
	private Socket frontend;
	private Socket backend;

	// Here we define the worker class; a structure and a set of functions that
	// as constructor, destructor, and methods on worker objects:

	public LoadBalancer(String frontendpoint, String backendpoint) {
		ctx = new ZContext ();
        frontend = ctx.createSocket(ZMQ.ROUTER);
        backend = ctx.createSocket(ZMQ.ROUTER);
        frontend.bind(frontendpoint);    //  For clients
        backend.bind(backendpoint);    //  For workers
	}
	
    private void updateExpiry(String identity) {
        Iterator<Worker> it = workers.iterator();
        while (it.hasNext()) {
            Worker worker = it.next();
            if (identity.equals(worker.identity)) {
            	worker.expiry = System.currentTimeMillis() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;
            	return;
            }
        }
    }
    
    private void addNewWorker(Worker newworker) {
    	String broadcastToNewWorker = "ADD";
    	// Broadcast the addition of a new worker.
    	for(Worker worker : workers){
    		broadcastToNewWorker = broadcastToNewWorker + ";" + worker.storageNodeSocket;
    		worker.address.send(backend, ZFrame.REUSE + ZFrame.MORE); // multi-part message
    		ZFrame frame = new ZFrame ("ADD;"+newworker.storageNodeSocket);
    		frame.send(backend, 0); // 0 => final part
    	}
    	
		System.out.println("\n[ADD WORKER/BROADCAST]: Identity - " + newworker.identity + " Address - " + newworker.storageNodeSocket);
		workers.add(newworker);
		
		broadcastToNewWorker += ";" + newworker.storageNodeSocket;
		// let the new worker know the consistent hash ring
		newworker.address.send(backend, ZFrame.REUSE + ZFrame.MORE); // multi-part message
		ZFrame frame = new ZFrame (broadcastToNewWorker);
		frame.send(backend, 0); // 0 => final part
    }
    
    //  The next method returns the next available worker address:
    private ZFrame getNextWorker() {  // Round-robin DNS
    	// optimistic. worker may be expired
		nextWorkerToGet = (nextWorkerToGet + 1) % workers.size();
		Worker worker = workers.get(nextWorkerToGet);
		System.out.println("[REQUEST HANDLING] Sending to Address - " + worker.address);
		return worker.address;
    }

    //  The purge method looks for and kills expired workers. We hold workers
    //  from oldest to most recent, so we stop at the first alive worker:
    private void purgeExpiredWorkers() {
        Iterator<Worker> it = workers.iterator();
        String broadcastString = "PURGE";
        while (it.hasNext()) {
            Worker worker = it.next();
            if (System.currentTimeMillis() > worker.expiry) {
            	System.out.println("\n[DELETE WORKER/BROADCAST]: Identity - " + worker.identity + " Address - " + worker.storageNodeSocket);
            	broadcastString = broadcastString + ";" + worker.storageNodeSocket;
            	it.remove();
            }
        }
        
     // Broadcast removed workers
		for(Worker worker : workers){
			 worker.address.send(backend, ZFrame.REUSE + ZFrame.MORE); // multi-part message
			 ZFrame frame = new ZFrame (broadcastString);
			 frame.send(backend, 0); // 0 => final part
		}
    }
    
	private void sendHeartBeatToWorkers(Socket backend) {
		for (Worker worker: workers) {
		    worker.address.send(backend, ZFrame.REUSE + ZFrame.MORE); // multi-part message
		    ZFrame frame = new ZFrame (PPP_HEARTBEAT);
		    frame.send(backend, 0); // 0 => final part
		}
	}

	private void handleMessageFromFrontEnd(Socket backend, ZMsg msg) {
		msg.push(getNextWorker());
		msg.send(backend);
	}

	private void handleMessageFromBackend(Socket frontend, ZMsg msg) {
        //  Validate control message, or return reply to client
		ZFrame address = msg.unwrap();
		
		if (msg.size() == 1) {
		    ZFrame frame = msg.getFirst();
		    String data = new String(frame.getData());
		    if (data.startsWith(PPP_READY)) {
		    	String storageNodeSocket = data.split(";")[1];
		    	Worker worker = new Worker(address, storageNodeSocket);
		    	addNewWorker(worker);
		    }
		    else if (data.equals(PPP_HEARTBEAT)) {
		    	updateExpiry(new String(address.getData()));
		    }
		    else {
//		    	System.out.println ("E: invalid message from worker");
		        msg.dump(System.out);
		        msg.destroy();
		    }
		}
		else
		{
			updateExpiry(new String(address.getData()));
		    msg.send(frontend);
		}
	}

	public void startLoadBalancer() {
        System.out.println("[LOAD BALANCER STARTED]");
        //  List of available workers
        //  Send out heartbeats at regular intervals
        long next_heartbeat_send_at = System.currentTimeMillis() + HEARTBEAT_INTERVAL;

        while (true) {
            PollItem items [] = {
                new PollItem( backend,  ZMQ.Poller.POLLIN ),
                new PollItem( frontend, ZMQ.Poller.POLLIN )
            };
            
            int rc = ZMQ.poll(items, HEARTBEAT_INTERVAL);
            if (rc == -1)
                break;              //  Interrupted

            //  Handle worker activity on backend
            if (items [0].isReadable()) {
                //  Use worker address for LRU routing
                ZMsg msg = ZMsg.recvMsg (backend);
                if (msg == null)
                    break;          //  Interrupted

                handleMessageFromBackend(frontend, msg);
            }
            if (items [1].isReadable()) {
//            	System.out.println("Routing Client Request - " + workers.size());
                //  Now get next client request, route to next worker
                ZMsg msg = ZMsg.recvMsg (frontend);
                if (msg == null)
                    break;          //  Interrupted
                handleMessageFromFrontEnd(backend, msg);
            }

            //  We handle heartbeating after any socket activity. First we send
            //  heartbeats to any idle workers if it's time. Then we purge any
            //  dead workers:
            
            if (System.currentTimeMillis() >= next_heartbeat_send_at) {
                sendHeartBeatToWorkers(backend);
                next_heartbeat_send_at = System.currentTimeMillis() + HEARTBEAT_INTERVAL;
            }
            
            purgeExpiredWorkers();
        }

        //  When we're done, clean up properly
        while ( workers.size() > 0) {
            workers.remove(0);
        }
        
        workers.clear();
        ctx.destroy();
	}
	
	public static void main(String[] args) throws Exception {
//		new LoadBalancer("tcp://*:5555", "tcp://*:5556").startLoadBalancer();
		if(args.length < 2)
		{
			System.out.println("Args: FrontEndPoint, BackEndPoint");
			return;
		}
		new LoadBalancer(args[0], args[1]).startLoadBalancer();
	}
}