package org.apache.flink.coordinator;

import org.apache.flink.MigrationApi.ClientServerProtocol;
import org.apache.flink.MigrationApi.Combiner;
import org.apache.flink.api.java.tuple.Tuple2;

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
		DataSender dataSourceThread=null;

		while (true) {
			Socket socket = serverSocket.accept();

			DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
			DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
			String[] arg = dataInputStream.readUTF().split(" ");

			System.out.println("creating new servers param: " + String.join(" ", arg));

			HyperRouteProvider<Integer> hyperRouteProvider=null;
			if (arg[2].contains(ClientServerProtocol.typeOnce)) {
				hyperRouteProvider=new HyperRouteProviderOnce<>();
			} else if (arg[2].contains(ClientServerProtocol.typeSplit)) {
				hyperRouteProvider=new HyperRouteProviderSplit<>(Integer.parseInt(arg[4]));
			}
			PFConstructor<Integer> pfc = new PFConstructor<>(30, Integer.parseInt(arg[3]),
				Integer.parseInt(arg[6]), 1.3f, hyperRouteProvider);

			new Thread(migrationThread=new MigrationServer<Integer, Tuple2<Integer, String>>(
				Integer.parseInt(arg[3]), new DownStreamValueCombiner())).start();
			new Thread(controllerThread=new Controller<Integer>(pfc)).start();
			new Thread(dataSourceThread = new DataSender(Integer.parseInt(arg[3]),
				Integer.parseInt(arg[6]), Integer.parseInt(arg[7]), Integer.parseInt(arg[8]),
				Integer.parseInt(arg[9]))).start();

			dataOutputStream.writeUTF("aa");
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


class DownStreamValueCombiner implements Combiner<Tuple2<Integer, String>>, Serializable {
	@Override
	public Tuple2<Integer, String> addOne(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
		return Tuple2.of(t1.f0 + t2.f0, t1.f0 + t2.f1); //+" "+t1.f1
	}
	@Override
	public Tuple2<Integer, String> addAll(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
		return Tuple2.of(t1.f0 + t2.f0, t1.f0 + t2.f1);//+t1.f1
	}
}
