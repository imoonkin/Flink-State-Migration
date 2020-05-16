package org.apache.flink.MigrationApp;

import org.apache.flink.MigrationApi.ClientServerProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class AppEntry {
	/**
	@param args 0: input dir, 1: output dir, 2: type, 3: parallelism, 4: chunkNum, 5: skewTriggerLength,
	6: rangePerNode, 7: hotKeyTimes, 8: cycle
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 9) {
			System.out.println("arg number wrong");
			return;
		}

		Socket socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portEntry);
		DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
		DataInputStream dis = new DataInputStream(socket.getInputStream());

		dos.writeUTF(String.join(" ", args));
		dos.flush();
		dis.readUTF();

		try {
			FlinkTop.main(args);
		} catch(Exception e){
			System.out.println("app failed to run");
			e.printStackTrace();
		} finally {
			dos.writeUTF("Finished");
			dos.flush();
			socket.close();
		}


	}
}
