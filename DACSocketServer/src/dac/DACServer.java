package dac;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.zeromq.ZMQ;

public class DACServer {
	public static void main(String[] args) throws Exception {
		ZMQ.Context context = ZMQ.context(1);
		// Socket to talk to clients
		ZMQ.Socket socket = context.socket(ZMQ.REP);
		socket.bind("tcp://*:5555");

		System.out.println("Server Started..");
		
		Class.forName("org.postgresql.Driver");
		Properties prop = new Properties();
		
		prop.load(new FileInputStream(args[0]));

		String url = prop.getProperty("url");
		String username = prop.getProperty("username");
		String password = prop.getProperty("password");
		
		Connection conn = DriverManager.getConnection(url,
				username, password);

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
				st.setString(1, tokens[1]);
				st.setString(2, tokens[2]);
				st.setString(3, tokens[3]);
				st.setString(4, tokens[3]);
				ResultSet rs = st.executeQuery();

				if (rs.next())
					socket.send("Yes");
				else
					socket.send("No");
				
				rs.close();
			} else {
				socket.send("No");
			}
		}

		st.close();
		socket.close();
		context.term();
	}
}
