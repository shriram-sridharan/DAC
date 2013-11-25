package dac;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.zeromq.ZMQ;

public class DACServer {

	private ConsistentHashingImpl consistentHashingImpl;
	private String localIpAddress;
	private Connection conn;
	private final String dacserverport;

	public DACServer(List<String> serverIpAddresses, int virtualServerPerIp, String url, String username, String password, String dacserverport) throws Exception {
		consistentHashingImpl = new ConsistentHashingImpl(serverIpAddresses, virtualServerPerIp);
		consistentHashingImpl.initializeConsistentHashRing();
		consistentHashingImpl.printCircle();
		
		Class.forName("org.postgresql.Driver");
		conn = DriverManager.getConnection(url, username, password);
		
		this.localIpAddress = InetAddress.getLocalHost().getHostAddress(); // might need to fix this when deployed in EC2/ App Engine
		this.dacserverport = dacserverport;
	}

	public static void main(String[] args) throws Exception {
		List<String> serverIpAddresses = new ArrayList<String>();
		serverIpAddresses.add("127.0.0.1");
		serverIpAddresses.add("server2");
		
		Properties prop = new Properties();
		prop.load(new FileInputStream(args[0]));
		String url = prop.getProperty("url");
		String username = prop.getProperty("username");
		String password = prop.getProperty("password");
		String dacserverport = prop.getProperty("dacserverport");
		
		DACServer dacServer = new DACServer(serverIpAddresses, 4, url, username, password, dacserverport);
		dacServer.startServer();
	}
	
	public void startServer() throws Exception {
		ZMQ.Context context = ZMQ.context(1);
		// Socket to talk to clients
		ZMQ.Socket socket = context.socket(ZMQ.REP);
		socket.bind("tcp://"+localIpAddress+":"+dacserverport);

		System.out.println("Server Started. Socket bound to " + "tcp://"+localIpAddress+":"+dacserverport);

		PreparedStatement st = conn
				.prepareStatement(" SELECT 1 FROM COLUMNVISIBILITY WHERE "
						+ " ROWID = ? " + " AND COLUMNFAMILY = ? "
						+ " AND ACCESSVECTOR & ? :: bit(10) = ? :: bit(10)"
						+ " LIMIT 1");

		while (!Thread.currentThread().isInterrupted()) {
			byte[] byteRequest = socket.recv();
			String request = new String(byteRequest);

			String[] tokens = request.split(";");
			// Expecting request to be in form
			// GET;RowID;ColumnFamily:Column;AuthorizationGroupVector

			if ("GET".equals(tokens[0])) {
				System.out.println(tokens[1]);
				System.out.println(tokens[2]);
				System.out.println(tokens[3]);
				String serverToContact = consistentHashingImpl.getServerLocationFor(tokens[1]+tokens[2]);
				if(localIpAddress.equals(serverToContact)) {
					socket.send(isAccessGranted(socket, st, tokens) ? "Yes" : "No");
				}
				else {
					socket.send("Need to contact a different server " + serverToContact);
				}
			} else {
				socket.send("No");
			}
		}

		st.close();
		socket.close();
		context.term();
	}

	private boolean isAccessGranted(ZMQ.Socket socket, PreparedStatement st,
			String[] tokens) throws SQLException {
		
		boolean access = false;
		st.setString(1, tokens[1]);
		st.setString(2, tokens[2]);
		st.setString(3, tokens[3]);
		st.setString(4, tokens[3]);
		ResultSet rs = st.executeQuery();

		if (rs.next())
			access = true;

		rs.close();
		
		return access;
	}
}
