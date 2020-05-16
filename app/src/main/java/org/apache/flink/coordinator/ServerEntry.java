package org.apache.flink.coordinator;

import org.apache.flink.MigrationApi.ClientServerProtocol;
import org.apache.flink.MigrationApi.Combiner;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class ServerEntry {
	public static void main(String[] args) throws IOException, InterruptedException {
		ServerSocket serverSocket = new ServerSocket(ClientServerProtocol.portEntry);
		MigrationServer migrationThread=null;
		Controller controllerThread=null;
		//DataSender dataSourceThread=null;

		while (true) {
			Socket socket = serverSocket.accept();

			DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
			DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
			int para = dataInputStream.readInt(), chunkNum = dataInputStream.readInt();
			String type = dataInputStream.readUTF();

			System.out.println("creating new servers");
			HyperRouteProvider<Integer> hyperRouteProvider=null;
			if (type.contains(ClientServerProtocol.typeOnce)) {
				hyperRouteProvider=new HyperRouteProviderOnce<>();
			} else if (type.contains(ClientServerProtocol.typeSplit)) {
				hyperRouteProvider=new HyperRouteProviderSplit<>(chunkNum);
			}
			PFConstructor<Integer> pfc = new PFConstructor<>(30, para, 1.3f, hyperRouteProvider);
			new Thread(migrationThread=new MigrationServer<Integer, Integer>(para, new DownStreamItemHighestPriceCombiner())).start();
			new Thread(controllerThread=new Controller<Integer>(pfc)).start();
			//new Thread(dataSourceThread=new DataSender(dataConstructor, dataHeight, isAppRunning, isSending)).start();
			System.out.println("new servers created");

			try {
				dataInputStream.readUTF();
				socket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}finally {
				migrationThread.setStop();
				controllerThread.setStop();
			}

		}
	}
}


class DownStreamItemHighestPriceCombiner implements Combiner<Integer>, Serializable {
	@Override
	public Integer addOne(Integer t1, Integer t2) {
		return t1.compareTo(t2)>0? t1 : t2; //+" "+t1.f1
	}
	@Override
	public Integer addAll(Integer t1, Integer t2) {
		return t1.compareTo(t2)>0? t1 : t2;//+t1.f1
	}
}
