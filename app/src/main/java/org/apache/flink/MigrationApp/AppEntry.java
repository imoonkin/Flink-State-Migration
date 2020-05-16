package org.apache.flink.MigrationApp;

import org.apache.flink.MigrationApi.ClientServerProtocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class AppEntry {
	public static void main(String[] args) throws IOException {
		int downStreamParallelism=2, chunkNum=4, skewTriggerLength=10000;
		String type= ClientServerProtocol.typeOnce;
		if (args.length > 0) {
			if (args.length != 6) {
				System.out.println("arg number wrong");
				return;
			}
			downStreamParallelism = Integer.parseInt(args[0]);
			chunkNum = Integer.parseInt(args[1]);
			type = args[2];
			skewTriggerLength = Integer.parseInt(args[3]);

		}
		float threshold = 0.1f;//TODO: threshold;

		Socket socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portEntry);
		DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
		dos.writeInt(downStreamParallelism);
		dos.writeInt(chunkNum);
		dos.writeUTF(type);
		//dos.writeInt(skewTriggerLength);
		dos.flush();

		try {
			if (type.equals(ClientServerProtocol.typeOnce)) {
				AppOnce.main(new String[]{String.valueOf(downStreamParallelism),
					String.valueOf(threshold), String.valueOf(skewTriggerLength),
					String.valueOf(chunkNum), type, args[4], args[5]});
			} else if (type.equals(ClientServerProtocol.typeSplit)) {
				AppSplit.main(new String[]{String.valueOf(downStreamParallelism),
					String.valueOf(threshold), String.valueOf(skewTriggerLength),
					String.valueOf(chunkNum), type, args[4], args[5]});
			}
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
