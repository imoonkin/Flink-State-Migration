package org.apache.flink.coordinator;

import org.apache.flink.MigrationApi.ClientServerProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
		AtomicBoolean isAppRunning = new AtomicBoolean(false), isSending= new AtomicBoolean(false);
		DataConstructor dataConstructor=new DataConstructor(isSending);

		while (true) {
			Socket socket = serverSocket.accept();
			isAppRunning.set(true);
			DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
			DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
			int para = dataInputStream.readInt(), chunkNum = dataInputStream.readInt();
			String type = dataInputStream.readUTF();
			int dataCols = dataInputStream.readInt(), dataHeight = dataInputStream.readInt();

			System.out.println("creating new servers");
			dataConstructor.construct(para, dataCols);
			HyperRouteProvider<Integer> hyperRouteProvider=null;
			if (type.contains(ClientServerProtocol.typeOnce)) {
				hyperRouteProvider=new HyperRouteProviderOnce<>();
			} else if (type.contains(ClientServerProtocol.typeSplit)) {
				hyperRouteProvider=new HyperRouteProviderSplit<>(chunkNum);
			}
			PFConstructor<Integer> pfc = new PFConstructor<>(30, para, 1.3f, hyperRouteProvider);
			new Thread(migrationThread=new MigrationServer(para)).start();
			new Thread(controllerThread=new Controller<Integer>(pfc)).start();
			new Thread(dataSourceThread=new DataSender(dataConstructor, dataHeight, isAppRunning, isSending)).start();
			System.out.println("new servers created");

			while (isAppRunning.get()) ;
			migrationThread.setStop();
			controllerThread.setStop();
		}
	}
}

class DataConstructor {
	String all = "A BB CCC\n";
	String skewness="a bb dddd eeeee ggggggg hhhhhhhh\n";
	private StringBuffer extra=new StringBuffer("");
	private AtomicBoolean isSending;

	DataConstructor(AtomicBoolean isSending) {
		this.isSending=isSending;
		startUserInput();
	}
	void construct(int para, int cols) {
		isSending.set(false);
		StringBuffer sbUnit = new StringBuffer(""), sbWhole=new StringBuffer("");
		for (int i = 0; i < cols; i++) {
			for (int j = 0; j < para; j++) {
				if (i!=0 || j!=0){
					sbUnit.append("a");
					if (j!=0) sbWhole.append(sbUnit.toString()).append(" ");
				}
				if (i==1 && j==0) all=sbWhole.toString()+sbUnit.toString();
			}
		}
		skewness = sbWhole.toString() + "\n";
		all = all + "\n";
	}
	private void startUserInput() {
		Scanner scanner = new Scanner(System.in);
		new Thread(() -> {
			while (scanner.hasNext()) {
				String input = scanner.nextLine();
				if (input.contains("stop") || input.contains("exit")) break;
				else if (input.contains("on")) isSending.set(true);
				else if (input.contains("off")) isSending.set(false);
				else extra.append(input.trim().replaceAll("\\s+$", ""));
				System.out.println(input.trim());
			}
		}).start();
	}

}
