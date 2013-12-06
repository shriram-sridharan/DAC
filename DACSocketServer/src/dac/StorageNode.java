package dac;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

//
// Paranoid Pirate worker
//
public class StorageNode {
	// Paranoid Pirate Protocol constants

	private final static int HEARTBEAT_LIVENESS = 3;
	private final static int HEARTBEAT_INTERVAL = 1000; // msecs
	private final static int INTERVAL_INIT = 1000; // Initial reconnect
	private final static int INTERVAL_MAX = 32000; // After exponential backoff
	private final static String PPP_READY = "\001"; // Signals worker is ready
	private final static String PPP_HEARTBEAT = "\002"; // Signals worker
	private Socket lbSocket;
	private Socket mySocket;
	private final String lbconnectToEndPoint;
	private ZContext ctx;
	private Connection conn;
	private PreparedStatement getPrepStatement;
	private final String mySocketBindEndPoint;
	private ConsistentHashingImpl consistentHashingImpl;
	private PreparedStatement putPrepStatement;
	private int numberOfReplicas;

	public StorageNode(String lbconnectToEndPoint, String mySocketBindEndPoint,
			int virtualServersPerIp, int noReplicas, String postgresurl, String username,
			String password) throws SQLException, ClassNotFoundException {
		this.lbconnectToEndPoint = lbconnectToEndPoint;
		this.mySocketBindEndPoint = mySocketBindEndPoint;
		numberOfReplicas = noReplicas;
		ctx = new ZContext();
		
		createNewLBWorkerSocket(false);
		createNewForwardedRequestHandlerSocket();
		Class.forName("org.postgresql.Driver");
		conn = DriverManager.getConnection(postgresurl, username, password);
		getPrepStatement = conn.prepareStatement(" SELECT 1 FROM COLUMNVISIBILITY WHERE "
				+ " ROWID = ? " + " AND COLUMNFAMILY = ? "
				+ " AND ACCESSVECTOR & ? :: bit(10) = ? :: bit(10)"
				+ " LIMIT 1");
		putPrepStatement = conn.prepareStatement("INSERT INTO COLUMNVISIBILITY VALUES(?,?,? :: bit(10))");
		consistentHashingImpl = new ConsistentHashingImpl(virtualServersPerIp);
	}
	
	private void createNewForwardedRequestHandlerSocket() {
		mySocket = ctx.createSocket(ZMQ.ROUTER);
		// Set random identity to make tracing easier
		Random rand = new Random(System.nanoTime());
		String identity = String.format("%04X-%04X", rand.nextInt(0x10000),
				rand.nextInt(0x10000));
		mySocket.setIdentity(identity.getBytes());
		mySocket.bind(mySocketBindEndPoint); // end point is ip:port. *=>localhost
		System.out.println("Bound my socket to - " + mySocketBindEndPoint);
	}

	private void createNewLBWorkerSocket(boolean destroy) {
		if (destroy) {
			System.out.println("I: Destroying worker and creating new one. This will flush old queues");
			ctx.destroySocket(lbSocket);
		}
		lbSocket = ctx.createSocket(ZMQ.DEALER);
		// Set random identity to make tracing easier
		Random rand = new Random(System.nanoTime());
		String identity = String.format("%04X-%04X", rand.nextInt(0x10000),
				rand.nextInt(0x10000));
		lbSocket.setIdentity(identity.getBytes());
		lbSocket.connect(lbconnectToEndPoint); // end point is ip:port. *=>localhost
		
		// Tell queue we're ready for work
		System.out.println("I: worker ready: " + identity);
		ZFrame frame = new ZFrame(PPP_READY + ";" + mySocketBindEndPoint);
		frame.send(lbSocket, 0);
	}

	private int sleepAndBackoffInterval(int interval) {
		System.out.println("W: heartbeat failure, can't reach queue\n");
		System.out.println(String.format("W: reconnecting in %d msec\n", interval));
		try {
			Thread.sleep(interval);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if (interval < INTERVAL_MAX) // exponential backoff
			interval *= 2;
		return interval;
	}

	private boolean isAccessGranted(String[] tokens) throws SQLException {
		
		boolean access = false;
		getPrepStatement.setString(1, tokens[1]);
		getPrepStatement.setString(2, tokens[2]);
		getPrepStatement.setString(3, tokens[3]);
		getPrepStatement.setString(4, tokens[3]);
		ResultSet rs = getPrepStatement.executeQuery();

		if (rs.next())
			access = true;

		rs.close();
		
		return access;
	}
	
	public long handleMessage(long previous_heartbeat_rec_at, ZMsg msg) throws SQLException {
		// Get message
		// - 3-part envelope + content -> request
		// - 1-part HEARTBEAT -> heartbeat
		if(msg.size() == 3) {
//			String requestKey = new String(msg.getLast().getData());
//			ZFrame address = msg.getFirst();
//			System.out.println("Received Message: "
//					+ requestKey + " from: "
//					+ new String(address.getData()));
			handleQuery(msg, lbSocket);
		}
		else if (msg.size() == 1) {
			previous_heartbeat_rec_at = handleSizeOneMessage(previous_heartbeat_rec_at, msg);
		} else {
			System.out.println("E: invalid message\n");
			msg.dump(System.out);
		}
		return previous_heartbeat_rec_at;
	}

	private long handleSizeOneMessage(long previous_heartbeat_rec_at, ZMsg msg) {
		ZFrame frame = msg.getFirst();
		String data = new String(frame.getData());
		if (PPP_HEARTBEAT.equals(data)) {
			previous_heartbeat_rec_at = System.currentTimeMillis(); // reset prev heartbeat time
		} else if (data.startsWith("ADD")) {
			String[] newWorkers = data.split(";");
			for(int i = 1; i < newWorkers.length; i++) {
				System.out.println("++Adding new worker in - " + mySocketBindEndPoint + ". the guy s - " + newWorkers[i]);
				consistentHashingImpl.add(newWorkers[i]);
//				consistentHashingImpl.print();
			}
		} else if (data.startsWith("PURGE")) {
			String[] oldWorkers = data.split(";");
			for(int i = 1; i < oldWorkers.length; i++) {
				System.out.println("--Deleted worker in - " + mySocketBindEndPoint + ". the guy s - " + oldWorkers[i]);
				consistentHashingImpl.remove(oldWorkers[i]);
//				consistentHashingImpl.print();
			}
		}
		else {
			System.out.println("E: invalid message\n");
			msg.dump(System.out);
		}
		msg.destroy();
		return previous_heartbeat_rec_at;
	}

	private void handleQuery(ZMsg msg, final Socket socketToSend)
			throws SQLException {
		ZFrame address = msg.getFirst();
		final String requestKey = new String(msg.getLast().getData());
//		System.out.println("Address = " + address + " Msg = " + requestKey);
		
		// Expecting request to be in form
		// GET;RowID;ColumnFamily:Column;AuthorizationGroupVector
		
		address.send(socketToSend, ZFrame.REUSE + ZFrame.MORE); // multi-part message
		new ZFrame("").send(socketToSend, ZFrame.REUSE + ZFrame.MORE); //**** empty- delimiter frame required
		
		String[] tokens = requestKey.split(";");
		assert(tokens.length > 0);
		
		if(tokens.length != 4 && tokens.length != 5) 
		{
		 	ZFrame frame = new ZFrame ("Ill-Formed Query - GET/PUT;RowID;ColumnFamily:Column;AuthorizationGroupVector");
	    	frame.send(socketToSend, 0); 
		}
		else if ("GET".equals(tokens[0])) {
			handleGET(socketToSend, requestKey, tokens);
		} else if ("PUT".equals(tokens[0])) {
			handlePUT(socketToSend, requestKey, tokens);
		} else if ("REPL".equals(tokens[0])) {
			System.out.println("Handling PUT Replicated");
			putPrepStatement.setString(1, tokens[2]);
			putPrepStatement.setString(2, tokens[3]);
			putPrepStatement.setString(3, tokens[4]);
			putPrepStatement.execute();
			ZFrame frame = new ZFrame ("Put done at " + mySocketBindEndPoint);
	    	frame.send(socketToSend, 0);
		} else {
			ZFrame frame = new ZFrame ("Unhandled Request - " + tokens[0]);
	    	frame.send(socketToSend, 0); 
		}
	}

	private void handleGET(final Socket socketToSend, final String requestKey,
			String[] tokens) throws SQLException {
		System.out.println(tokens[1]);
		System.out.println(tokens[2]);
		System.out.println(tokens[3]);
		
		final String nodeToHandle = consistentHashingImpl.get(tokens[1] + tokens[2]);
		if(nodeToHandle.equals(mySocketBindEndPoint))
		{
			System.out.println("Acting as Coordinator for GET\n");
			ZFrame frame = new ZFrame (isAccessGranted(tokens) ? "Yes" : "No");
			frame.send(socketToSend, 0);
		} else {
			System.out.println("Forwarding GET to Coordinator " + nodeToHandle + "\n");
			// spawn a new thread to handle this.
			handoffToCoordinator(socketToSend, requestKey, nodeToHandle);
		}
	}

	private void handlePUT(final Socket socketToSend, final String requestKey,
			String[] tokens) throws SQLException {
		final ArrayList<String> listofnodestorepl = 
				consistentHashingImpl.getNodesToReplicate(tokens[1] + tokens[2], numberOfReplicas);
		assert(numberOfReplicas > 0);
		
		String nodeToHandle = listofnodestorepl.get(0);
		if(nodeToHandle.equals(mySocketBindEndPoint))
		{
			System.out.println("\nActing as Coordinator to Handle PUT");
			putPrepStatement.setString(1, tokens[1]);
			putPrepStatement.setString(2, tokens[2]);
			putPrepStatement.setString(3, tokens[3]);
			putPrepStatement.execute();
			
			// first send reply to client
			ZFrame frame = new ZFrame ("Put done");
			frame.send(socketToSend, 0);
			
			// Now replicate. Each one spawns a thread
			System.out.println("Replicating In Other Nodes");
			for (int i = 1; i < listofnodestorepl.size(); i++)
				replicateData(requestKey, listofnodestorepl.get(i));
			
		} else {
			System.out.println("\nForwarding PUT to - " + nodeToHandle + " - as per consistent Hashing");
			// spawn a new thread to handle this.
			handoffToCoordinator(socketToSend, requestKey, nodeToHandle);
		}
	}

	private void handoffToCoordinator(final Socket socketToSend,
			final String requestKey, final String nodeToHandle) {
		
		Thread t1 = new Thread() {
			@Override
			public void run() {
				Socket onthefly = ctx.createSocket(ZMQ.REQ);
				// Set random identity to make tracing easier
				Random rand = new Random(System.nanoTime());
				String identity = String.format("%04X-%04X",
						rand.nextInt(0x10000), rand.nextInt(0x10000));
				onthefly.setIdentity(identity.getBytes());
				onthefly.connect(nodeToHandle); // end point is ip:port.
												// *=>localhost
				ZFrame z = new ZFrame(requestKey);
				z.send(onthefly, 0);

//				System.out.println("Blocking Sent to other node to handle");
				ZFrame recvFrame = ZFrame.recvFrame(onthefly);
				String coordinatorResponse = new String(recvFrame.getData());
				System.out.println("Forwarded Node Replied Back with - " + coordinatorResponse);

				ZFrame frame = new ZFrame(coordinatorResponse);
				frame.send(socketToSend, 0);
				onthefly.close();
			}
		};
		t1.start();
	}
	
	private void replicateData(String requestKey, final String nodeToHandle) {
		
		final String modifiedRequest = "REPL;"+requestKey;
		Thread t1 = new Thread() {
			@Override
			public void run() {
				Socket onthefly = ctx.createSocket(ZMQ.REQ);
				// Set random identity to make tracing easier
				Random rand = new Random(System.nanoTime());
				String identity = String.format("%04X-%04X",
						rand.nextInt(0x10000), rand.nextInt(0x10000));
				onthefly.setIdentity(identity.getBytes());
				onthefly.connect(nodeToHandle); // end point is ip:port.
												// *=>localhost
				ZFrame z = new ZFrame(modifiedRequest);
				z.send(onthefly, 0);

				ZFrame recvFrame = ZFrame.recvFrame(onthefly);
				String coordinatorResponse = new String(recvFrame.getData());
				System.out.println("Replication Result at Site - " + coordinatorResponse);
				onthefly.close();
			}
		};
		t1.start();
	}

	private long sendHeartBeat(Socket worker, long next_heartbeat_send_at) {
		if (System.currentTimeMillis() > next_heartbeat_send_at) {
			next_heartbeat_send_at = System.currentTimeMillis()	+ HEARTBEAT_INTERVAL;
			ZFrame frame = new ZFrame(PPP_HEARTBEAT);
			frame.send(worker, 0);
		}
		return next_heartbeat_send_at;
	}
	
	public void startLBRequestHandler() throws SQLException {
		int interval = INTERVAL_INIT;
		long next_heartbeat_send_at = System.currentTimeMillis() + HEARTBEAT_INTERVAL;
		long previous_heartbeat_rec_at = System.currentTimeMillis();

		while (true) {
			PollItem items[] = { new PollItem(lbSocket, ZMQ.Poller.POLLIN) }; // poll socket
			int rc = ZMQ.poll(items, HEARTBEAT_INTERVAL);
			if (rc == -1)
				break; // Interrupted

			if (items[0].isReadable()) {
				ZMsg msg = ZMsg.recvMsg(lbSocket);
				if (msg == null)
					break; // Interrupted

				previous_heartbeat_rec_at = handleMessage(previous_heartbeat_rec_at, msg);
				interval = INTERVAL_INIT;
			} else if (System.currentTimeMillis() - previous_heartbeat_rec_at >= HEARTBEAT_LIVENESS
					* HEARTBEAT_INTERVAL) {
				interval = sleepAndBackoffInterval(interval);
				createNewLBWorkerSocket(true);
				previous_heartbeat_rec_at = System.currentTimeMillis(); 
			}

			// Send heartbeat to queue if it's time
			next_heartbeat_send_at = sendHeartBeat(lbSocket, next_heartbeat_send_at);
		}
		ctx.destroy();
	}
	
	public void startForwardedRequestHandler() throws SQLException {
		while (true) {
			PollItem items[] = { new PollItem(mySocket, ZMQ.Poller.POLLIN) }; // poll socket
			int rc = ZMQ.poll(items, HEARTBEAT_INTERVAL);
			if (rc == -1)
				break; // Interrupted

			if (items[0].isReadable()) {
				ZMsg msg = ZMsg.recvMsg(mySocket);
				if (msg == null)
					break; // Interrupted
				handleQuery(msg, mySocket);
			} 
		}
	}

	public static void main(String[] args) throws Exception {
		if(args.length < 7)
		{
			System.out.println("Args: LBConnectPoint, StorageNodeSocketPoint, NoVirtualServers, " +
					"NoReplicasForPut, PostgresJDBC, PostgresUsername, PostgresPassword");
			return;
		}
		
//		String lbconnectToEndPoint2 = "tcp://*:5556";
//		String mySocketBindEndPoint2 = "tcp://*:4558";
//		int virtualServersPerIp = 1;
//		int noReplicas = 2;
//		final StorageNode sn = new StorageNode(lbconnectToEndPoint2, mySocketBindEndPoint2, virtualServersPerIp,
//				noReplicas, "jdbc:postgresql://localhost:5432/dac1", "shriram", ""); // backend
		
		final StorageNode sn = new StorageNode(args[0], args[1],
				Integer.parseInt(args[2]), Integer.parseInt(args[3]), args[4],
				args[5], args[6]); 
		
		Thread t1 = new Thread() {
			@Override
			public void run() {
				try {
					sn.startLBRequestHandler();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		};

		Thread t2 = new Thread() {
			@Override
			public void run() {
				try {
					sn.startForwardedRequestHandler();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		};

		t2.start(); // start the forwarded request handler socket first
		t1.start();
	}
}