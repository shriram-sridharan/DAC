package dac;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

	private final static int HEARTBEAT_LIVENESS = 2;
	private final static int HEARTBEAT_INTERVAL = 1000; // msecs
	private final static int INTERVAL_INIT = 1000; // Initial reconnect
	private final static int INTERVAL_MAX = 32000; // After exponential backoff
	private final static String PPP_READY = "\001"; // Signals worker is ready
	private final static String PPP_HEARTBEAT = "\002"; // Signals worker
	private Socket workerSocket;
	private final String connectToEndPoint;
	private ZContext ctx;
	private Connection conn;
	private PreparedStatement prepStatement;

	public StorageNode(String connectToEndPoint, String postgresurl, String username, String password) throws SQLException, ClassNotFoundException {
		this.connectToEndPoint = connectToEndPoint;
		ctx = new ZContext();
		createNewSocket(false);
		Class.forName("org.postgresql.Driver");
		conn = DriverManager.getConnection(postgresurl, username, password);
		prepStatement = conn
		.prepareStatement(" SELECT 1 FROM COLUMNVISIBILITY WHERE "
				+ " ROWID = ? " + " AND COLUMNFAMILY = ? "
				+ " AND ACCESSVECTOR & ? :: bit(10) = ? :: bit(10)"
				+ " LIMIT 1");
	}
	
	private Socket createWorkerSocket(ZContext ctx, String endpoint) {
		Socket worker = ctx.createSocket(ZMQ.DEALER);
		// Set random identity to make tracing easier
		Random rand = new Random(System.nanoTime());
		String identity = String.format("%04X-%04X", rand.nextInt(0x10000),
				rand.nextInt(0x10000));
		worker.setIdentity(identity.getBytes());
		worker.connect(endpoint); // end point is ip:port. *=>localhost

		// Tell queue we're ready for work
		System.out.println("I: worker ready\n");
		ZFrame frame = new ZFrame(PPP_READY);
		frame.send(worker, 0);
		return worker;
	}

	private void createNewSocket(boolean destroy) {
		if (destroy) {
			System.out.println("I: Destroying worker and creating new one. This will flush old queues");
			ctx.destroySocket(workerSocket);
		}
		workerSocket = createWorkerSocket(ctx, connectToEndPoint);
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
		prepStatement.setString(1, tokens[1]);
		prepStatement.setString(2, tokens[2]);
		prepStatement.setString(3, tokens[3]);
		prepStatement.setString(4, tokens[3]);
		ResultSet rs = prepStatement.executeQuery();

		if (rs.next())
			access = true;

		rs.close();
		
		return access;
	}
	
	private long handleMessage(long previous_heartbeat_rec_at, ZMsg msg) throws SQLException {
		// Get message
		// - 3-part envelope + content -> request
		// - 1-part HEARTBEAT -> heartbeat
		if(msg.size() == 3) {
			String requestKey = new String(msg.getLast().getData());
			ZFrame address = msg.getFirst();
			System.out.println("Received Message: "
					+ requestKey + " from: "
					+ new String(address.getData()));
			handleQuery(msg);
		}
		else if (msg.size() == 1) {
			System.out.println("Getting heartbeat from server" + msg.size());
			ZFrame frame = msg.getFirst();
			if (PPP_HEARTBEAT.equals(new String(frame.getData()))) {
				previous_heartbeat_rec_at = System.currentTimeMillis(); // reset prev heartbeat time
			} else {
				System.out.println("E: invalid message\n");
				msg.dump(System.out);
			}
			msg.destroy();
		} else {
			System.out.println("E: invalid message\n");
			msg.dump(System.out);
		}
		return previous_heartbeat_rec_at;
	}

	private void handleQuery(ZMsg msg)
			throws SQLException {
		ZFrame address = msg.getFirst();
		String requestKey = new String(msg.getLast().getData());
		System.out.println("Address = " + address + " Msg = " + requestKey);
		
		// Expecting request to be in form
		// GET;RowID;ColumnFamily:Column;AuthorizationGroupVector
		
		address.send(workerSocket, ZFrame.REUSE + ZFrame.MORE); // multi-part message
		new ZFrame("").send(workerSocket, ZFrame.REUSE + ZFrame.MORE); //**** empty- delimiter frame required
		
		String[] tokens = requestKey.split(";");
		if(tokens.length != 4) 
		{
		 	ZFrame frame = new ZFrame ("Ill-Formed Query - GET;RowID;ColumnFamily:Column;AuthorizationGroupVector");
	    	frame.send(workerSocket, 0); 
		}
		else if ("GET".equals(tokens[0])) {
			System.out.println(tokens[1]);
			System.out.println(tokens[2]);
			System.out.println(tokens[3]);
			ZFrame frame = new ZFrame (isAccessGranted(tokens) ? "Yes" : "No");
	    	frame.send(workerSocket, 0); 
		} else {
			ZFrame frame = new ZFrame ("Unhandled Request - " + tokens[0]);
	    	frame.send(workerSocket, 0); 
		}
	}

	private long sendHeartBeat(Socket worker, long next_heartbeat_send_at) {
		if (System.currentTimeMillis() > next_heartbeat_send_at) {
			next_heartbeat_send_at = System.currentTimeMillis()	+ HEARTBEAT_INTERVAL;
			System.out.println("I: worker heartbeat\n");
			ZFrame frame = new ZFrame(PPP_HEARTBEAT);
			frame.send(worker, 0);
		}
		return next_heartbeat_send_at;
	}
	
	public void startWorker() throws SQLException {
		int interval = INTERVAL_INIT;
		long next_heartbeat_send_at = System.currentTimeMillis() + HEARTBEAT_INTERVAL;
		long previous_heartbeat_rec_at = System.currentTimeMillis();

		while (true) {
			PollItem items[] = { new PollItem(workerSocket, ZMQ.Poller.POLLIN) }; // poll socket
			int rc = ZMQ.poll(items, HEARTBEAT_INTERVAL);
			if (rc == -1)
				break; // Interrupted

			if (items[0].isReadable()) {
				ZMsg msg = ZMsg.recvMsg(workerSocket);
				if (msg == null)
					break; // Interrupted

				previous_heartbeat_rec_at = handleMessage(previous_heartbeat_rec_at, msg);
				interval = INTERVAL_INIT;
			} else if (System.currentTimeMillis() - previous_heartbeat_rec_at >= HEARTBEAT_LIVENESS
					* HEARTBEAT_INTERVAL) {
				interval = sleepAndBackoffInterval(interval);
				createNewSocket(true);
				previous_heartbeat_rec_at = System.currentTimeMillis(); 
			}

			// Send heartbeat to queue if it's time
			next_heartbeat_send_at = sendHeartBeat(workerSocket, next_heartbeat_send_at);
		}
		ctx.destroy();
	}

	public static void main(String[] args) throws Exception {
		new StorageNode("tcp://*:5556", "jdbc:postgresql://localhost:5432/dac", "shriram", "").startWorker(); // backend
	}
}