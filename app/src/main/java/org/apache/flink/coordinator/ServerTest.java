package org.apache.flink.coordinator;

import org.apache.flink.MigrationApi.ClientServerProtocol;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;

public class ServerTest {
	public static void main(String[] args) throws IOException {
		Socket socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portData);
		DataInputStream input = new DataInputStream(socket.getInputStream());
		while(true) System.out.println(input.readUTF());

	}
}
