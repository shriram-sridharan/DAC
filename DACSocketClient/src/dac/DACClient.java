package dac;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.zeromq.ZMQ;

public class DACClient {

	public static void main(String[] args) throws IOException {
		ZMQ.Context context = ZMQ.context(1);

		// Socket to talk to server
		System.out.println("Connecting to hello world server");

		ZMQ.Socket socket = context.socket(ZMQ.REQ);
		socket.connect("tcp://127.0.0.1:5555");

		InputStreamReader converter = new InputStreamReader(System.in);
		BufferedReader in = new BufferedReader(converter);

		while (!Thread.currentThread().isInterrupted()) {
			socket.send(in.readLine());
			byte[] reply = socket.recv();
			System.out.println("Access ? = " + new String(reply));
		}
		socket.close();
		context.term();
	}
}
