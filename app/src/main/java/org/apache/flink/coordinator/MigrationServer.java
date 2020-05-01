package org.apache.flink.coordinator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.MigrationApi.ClientServerProtocol;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MigrationServer implements Runnable{
	static ArrayList<ConcurrentHashMap<Integer, Tuple2<Integer, String>>> mapList;

	@Override
	public void run() {
		int downStreamParallelism=23;
		mapList=new ArrayList<>();
		for (int i=0; i<downStreamParallelism; i++) mapList.add(new ConcurrentHashMap<>());

		AtomicInteger pushed=new AtomicInteger(0);
		AtomicBoolean pullable=new AtomicBoolean(false);
		ServerSocket serverSocket;
		try {
			serverSocket = new ServerSocket(ClientServerProtocol.portMigration);
			while(true) {
				Socket socket = serverSocket.accept();
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				String cli = ois.readUTF();
				if (cli.contains(ClientServerProtocol.downStreamSplitMigrationStart)) {
					new Thread(new DownStreamSplitHandler(ois, oos, socket))
					.start();
				} else if (cli.contains(ClientServerProtocol.downStreamOnceMigrationStart)) {
					new Thread(new DownStreamOnceHandler(ois, oos, socket, pushed, pullable))
						.start();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/*public static void main(String[] args) throws Exception{
		Thread t=new Thread(new MigrationServer(new PFConstructor()));
		t.start();
		t.join();
	}*/
}

class DownStreamSplitHandler implements Runnable{
	private int cliIndex;
	private Socket socket;
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	DownStreamSplitHandler(ObjectInputStream ois, ObjectOutputStream oos, Socket s){
		this.oos=oos;
		this.ois=ois;
		socket=s;
	}
	@Override
	public void run() {
		try {
			cliIndex=ois.readInt();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//System.out.println("migration thread");
		while(true){
			try {
				String cmd=ois.readUTF();
				if (cmd.contains(ClientServerProtocol.downStreamPull)) { //partition function & hash map
					//System.out.println("downStream: "+cliIndex+" pulling");

					oos.writeObject(MigrationServer.mapList.get(cliIndex));
					oos.flush();
					MigrationServer.mapList.get(cliIndex).clear();
				} else if (cmd.contains(ClientServerProtocol.downStreamPush)) {
					//System.out.println("downStream: "+cliIndex+" pushing");

					int ind=ois.readInt();
					Integer key=(Integer)ois.readObject();
					Tuple2<Integer, String> value=(Tuple2<Integer, String>)ois.readObject();

					MigrationServer.mapList.get(ind).merge(key, value,
						(v1, v2) -> Tuple2.of(v1.f0+v2.f0, v1.f1 + v2.f1));
					System.out.println("Server get "+key+" "+value+" from "+cliIndex);
				} else if (cmd.contains(ClientServerProtocol.downStreamClose)) {
					socket.close();
					//System.out.println("downStream: "+cliIndex+" closed");
					break;
				}
			} catch (IOException | ClassNotFoundException e) {
				//e.printStackTrace();
				break;
			}
		}
	}

}

class DownStreamOnceHandler implements Runnable {
	private int cliIndex;
	private Socket socket;
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	private AtomicInteger pushed;
	private AtomicBoolean pullable;

	DownStreamOnceHandler(ObjectInputStream ois, ObjectOutputStream oos, Socket s,
						  AtomicInteger pushed, AtomicBoolean pullable) {
		this.oos = oos;
		this.ois = ois;
		socket = s;
		this.pushed=pushed;
		this.pullable=pullable;
	}

	@Override
	public void run() {
		try {
			cliIndex = ois.readInt();
			//System.out.println("migration thread");
			while (true) {
				String cmd = ois.readUTF();
				if (cmd.contains(ClientServerProtocol.downStreamPull)) { //
					//System.out.println("downStream: "+cliIndex+" pulling");
					if (pushed.incrementAndGet() == ClientServerProtocol.downStreamParallelism) { // pull means push end
						pullable.set(true);
					}
					while (true) {
						if (pullable.get()) {
							oos.writeObject(MigrationServer.mapList.get(cliIndex));
							oos.flush();
							MigrationServer.mapList.get(cliIndex).clear();
							break;
						}
					}

				} else if (cmd.contains(ClientServerProtocol.downStreamPush)) {
					//System.out.println("downStream: "+cliIndex+" pushing");
					int ind = ois.readInt();
					Integer key = (Integer) ois.readObject();
					Tuple2<Integer, String> value = (Tuple2<Integer, String>) ois.readObject();

					MigrationServer.mapList.get(ind).merge(key, value,
						(v1, v2) -> Tuple2.of(v1.f0 + v2.f0, v1.f1 + v2.f1));
					System.out.println("OnceServer get " + key + " " + value + " from " + cliIndex);
				} else if (cmd.contains(ClientServerProtocol.downStreamClose)) {
					socket.close();
					if (pushed.decrementAndGet() == ClientServerProtocol.downStreamParallelism) {
						pullable.set(false);
					}
					//System.out.println("downStream: "+cliIndex+" closed");
					break;
				}
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}

