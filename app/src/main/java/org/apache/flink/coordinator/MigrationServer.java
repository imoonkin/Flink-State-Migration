package org.apache.flink.coordinator;

import org.apache.flink.app.ClientServerProtocol;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class MigrationServer implements Runnable{
	static ArrayList<HashMap<Integer, Integer>> mapList;
	static PFConstructor pfc;
	private MigrationServer() {
		pfc=new PFConstructor();
	}
	@Override
	public void run() {
		int downStreamParallelism=23;
		mapList=new ArrayList<>();
		for (int i=0; i<downStreamParallelism; i++) mapList.add(new HashMap<>());

		ServerSocket serverSocket;
		try {
			serverSocket = new ServerSocket(ClientServerProtocol.port);
			while(true) {
				Socket socket = serverSocket.accept();
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				String cli = ois.readUTF();
				if (cli.contains(ClientServerProtocol.downStreamStart)) {
					new Thread(new DownStreamHandler(ois, oos, socket))
					.start();
				} else if (cli.contains(ClientServerProtocol.upStreamStart)){
					//System.out.println("UpStream");
					new Thread(new UpStreamHandler(ois, oos, socket))
					.start();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws Exception{
		Thread t=new Thread(new MigrationServer());
		t.start();
		t.join();
	}
}

class UpStreamHandler implements Runnable{
	private Socket socket;
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	UpStreamHandler(ObjectInputStream ois, ObjectOutputStream oos, Socket s) {
		this.socket =s;
		this.oos=oos;
		this.ois=ois;
	}
	@Override
	public void run() {
		//System.out.println("UpStreamHandler");
		try {
			oos.writeObject(MigrationServer.pfc.getPF());
			//System.out.println("wrote");
			oos.flush();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

class DownStreamHandler implements Runnable{
	private int cliIndex;
	private Socket socket;
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	DownStreamHandler(ObjectInputStream ois, ObjectOutputStream oos, Socket s) throws IOException {
		this.oos=oos;
		this.ois=ois;
		cliIndex=ois.readInt();
		socket=s;
	}
	@Override
	public void run() {
		while(true){
			try {
				String cmd=ois.readUTF();
				if (cmd.contains(ClientServerProtocol.downStreamPull)) { //partition function & hash map
					System.out.println("downStream: "+cliIndex+" pulling");

					oos.writeObject(MigrationServer.pfc.getPF());
					oos.writeObject(MigrationServer.mapList.get(cliIndex));
					MigrationServer.mapList.get(cliIndex).clear();
				} else if (cmd.contains(ClientServerProtocol.downStreamPush)) {
					System.out.println("downStream: "+cliIndex+" pushing");

					int ind=ois.readInt();
					Integer key=(Integer)ois.readObject();
					Integer value=(Integer)ois.readObject();
					MigrationServer.mapList.get(ind).merge(key, value, (localkey, localvalue)-> localvalue+value);
				} else if (cmd.contains(ClientServerProtocol.downStreamClose)) {
					socket.close();
					System.out.println("downStream: "+cliIndex+" closed");
					break;
				}
			} catch (IOException | ClassNotFoundException e) {
				//e.printStackTrace();
				break;
			}
		}
	}

}
