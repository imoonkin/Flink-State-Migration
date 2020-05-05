package org.apache.flink.MigrationApp;

import org.apache.flink.MigrationApi.ClientServerProtocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class AppEntry {
	public static void main(String[] args) throws IOException {
		int downStreamParallelism=2, chunkNum=4, dataCols=3, dataHeight=5;
		String type= ClientServerProtocol.typeOnce;
		if (args.length > 0) {
			if (args.length != 5) {
				System.out.println("arg number wrong");
				return;
			}
			downStreamParallelism = Integer.parseInt(args[0]);
			chunkNum = Integer.parseInt(args[1]);
			type = args[2];
			dataCols = Integer.parseInt(args[3]);
			dataHeight = Integer.parseInt(args[4]);

		}
		float threshold = (((float) dataHeight) / (1 + (downStreamParallelism - 1) * dataCols * dataHeight))*0.9f ;

		Socket socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portEntry);
		DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
		dos.writeInt(downStreamParallelism);
		dos.writeInt(chunkNum);
		dos.writeUTF(type);
		dos.writeInt(dataCols);
		dos.writeInt(dataHeight);
		dos.flush();
		socket.close();
		if (type.equals(ClientServerProtocol.typeOnce)) {
			try {
				AppOnce.main(new String[]{String.valueOf(downStreamParallelism), String.valueOf(threshold), String.valueOf(chunkNum), type});
			} catch (Exception e) {
				System.out.println("app failed to run");
				e.printStackTrace();
			}
		} else if (type.equals(ClientServerProtocol.typeSplit)) {
			try {
				AppSplit.main(new String[]{String.valueOf(downStreamParallelism), String.valueOf(threshold), String.valueOf(chunkNum), type});
			} catch (Exception e) {
				System.out.println("app failed to run");
				e.printStackTrace();
			}
		}
	}
}
