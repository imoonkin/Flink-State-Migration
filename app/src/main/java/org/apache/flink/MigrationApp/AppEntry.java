package org.apache.flink.MigrationApp;

import org.apache.flink.MigrationApi.ClientServerProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class AppEntry {
	/**
	@param args
	0: input dir,
	1: output dir,
	2: type, (typeOnce/typeSplit)
	3: parallelism,
	4: chunkNum,
	5: skewTriggerLength, number of records read to start migration
	6: rangePerNode, key domain space per node
	7: hotKeyTimes, hot key repeat times
	8: cycle, number of records read to generate next group of hot keys
	9: prepareLen, number of records read before hot key appears
	10: epsilon, space saving parameter
	11: hotKeyPercent, for data sender
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 12) {
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
