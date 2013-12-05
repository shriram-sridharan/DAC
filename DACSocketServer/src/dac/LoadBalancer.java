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

public class LoadBalancer {
	private class Worker {
		ZFrame address; // Address of worker
		String identity; // Printable identity
		long expiry; // Expires at this time

		protected Worker(ZFrame address) {
			this.address = address;
			identity = new String(address.getData());
			expiry = System.currentTimeMillis() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;
		}
	};

	private final static int HEARTBEAT_LIVENESS = 2; // 3-5 is reasonable
	private final static int HEARTBEAT_INTERVAL = 1000; // msecs
	private final static String PPP_READY = "\001"; // Signals worker is ready
	private final static String PPP_HEARTBEAT = "\002"; 
	
	ArrayList<Worker> workers = new ArrayList<Worker>();

	// Here we define the worker class; a structure and a set of functions that
	// as constructor, destructor, and methods on worker objects:

    private void addWorkerToEndOfList(Worker newworker) {
        Iterator<Worker> it = workers.iterator();
        while (it.hasNext()) {
            Worker worker = it.next();
            if (newworker.identity.equals(worker.identity)) {
                it.remove();
                break;
            }
        }
        workers.add(newworker);
    }
    
    //  The next method returns the next available worker address:
    private ZFrame getNextWorker() {
        Worker worker = workers.remove(0);
        assert (worker != null);
        ZFrame frame = worker.address;
        return frame;
    }

    //  The purge method looks for and kills expired workers. We hold workers
    //  from oldest to most recent, so we stop at the first alive worker:
    private void purgeExpiredWorkers() {
        Iterator<Worker> it = workers.iterator();
        while (it.hasNext()) {
            Worker worker = it.next();
            if (System.currentTimeMillis() < worker.expiry) {
                break;
            }
            it.remove();
        }
    }
    
	private void sendHeartBeatToWorkers(Socket backend) {
		for (Worker worker: workers) {
		    System.out.println("Sending heartbeat to " + worker.identity);
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
		if (msg.size() == 1) {
		    ZFrame frame = msg.getFirst();
		    String data = new String(frame.getData());
		    if (!data.equals(PPP_READY)
		    &&  !data.equals( PPP_HEARTBEAT)) {
		        System.out.println ("E: invalid message from worker");
		        msg.dump(System.out);
		    }
		    msg.destroy();
		}
		else
		{
			System.out.println("Received Message: forwarding............\n\n\n......................................................... ");
		    msg.send(frontend);
		}
	}

	public void startLoadBalancer(String frontendpoint, String backendpoint) {
		ZContext ctx = new ZContext ();
        Socket frontend = ctx.createSocket(ZMQ.ROUTER);
        Socket backend = ctx.createSocket(ZMQ.ROUTER);
        frontend.bind(frontendpoint);    //  For clients
        backend.bind(backendpoint);    //  For workers

        System.out.println("Load Balancer Started... Can optimize load balancer[later]");
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
            	System.out.println("Worker sending message - " + workers.size());
                //  Use worker address for LRU routing
                ZMsg msg = ZMsg.recvMsg (backend);
                if (msg == null)
                    break;          //  Interrupted

                //  Any sign of life from worker means it's ready
                ZFrame address = msg.unwrap();
                System.out.println("Zframe address = " + new String(address.getData()));
                Worker worker = new Worker(address);
                addWorkerToEndOfList(worker);
                
                System.out.println("REceived msg size = " + msg.size());
                handleMessageFromBackend(frontend, msg);
            }
            if (items [1].isReadable()) {
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
		new LoadBalancer().startLoadBalancer("tcp://*:5555", "tcp://*:5556");
	}
}