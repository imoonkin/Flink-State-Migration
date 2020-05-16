package org.apache.flink.coordinator;

import org.apache.flink.MigrationApi.Combiner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.MigrationApi.ClientServerProtocol;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MigrationServer<K, V> implements Runnable {
	private ArrayList<ConcurrentHashMap<K, V>> mapList;
	private Combiner<V> combiner;
	private boolean isRunning;
	static int parallelism;
	private ServerSocket serverSocket;

	MigrationServer(int parallelism, Combiner<V> combiner) {
		this.isRunning=true;
		MigrationServer.parallelism =parallelism;
		serverSocket=null;
		this.combiner=combiner;
	}

	void setStop() throws IOException {
		isRunning = false;
		serverSocket.close();
	}

	@Override

	public void run() {
		int downStreamParallelism = 23;
		mapList = new ArrayList<>();
		for (int i = 0; i < downStreamParallelism; i++) mapList.add(new ConcurrentHashMap<>());

		AtomicInteger pushed = new AtomicInteger(0);
		AtomicBoolean pullable = new AtomicBoolean(false);

		try {
			serverSocket = new ServerSocket(ClientServerProtocol.portMigration);
			//System.out.println("migration server opened " + serverSocket.isClosed());
			while (isRunning) {
				Socket socket = serverSocket.accept();
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				String cli = ois.readUTF();
				if (cli.contains(ClientServerProtocol.downStreamSplitMigrationStart)) {
					new Thread(new DownStreamSplitHandler<K, V>(ois, oos, socket, mapList, combiner))
						.start();
				} else if (cli.contains(ClientServerProtocol.downStreamOnceMigrationStart)) {
					new Thread(new DownStreamOnceHandler<K, V>(ois, oos, socket,  mapList, combiner, pushed, pullable))
						.start();
				}
			}
		} catch (SocketException e1) {
			System.out.println("last migration server closed");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (serverSocket != null) {
				try {
					serverSocket.close();
					System.out.println("finally migration server closed");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	/*public static void main(String[] args) throws Exception{
		Thread t=new Thread(new MigrationServer(new PFConstructor()));
		t.start();
		t.join();
	}*/
}

class DownStreamSplitHandler<K, V> implements Runnable{
	int cliIndex;
	Socket socket;
	ObjectInputStream ois;
	ObjectOutputStream oos;
	ArrayList<ConcurrentHashMap<K, V>> mapList;
	Combiner<V> combiner;
	DownStreamSplitHandler(ObjectInputStream ois, ObjectOutputStream oos, Socket s,
						   ArrayList<ConcurrentHashMap<K, V>> mapList, Combiner<V> combiner){
		this.oos=oos;
		this.ois=ois;
		socket=s;
		this.mapList=mapList;
		this.combiner=combiner;
	}
	@Override
	public void run() {
		long startTime=System.currentTimeMillis();
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

					oos.writeObject(mapList.get(cliIndex));
					oos.flush();
					mapList.get(cliIndex).clear();
				} else if (cmd.contains(ClientServerProtocol.downStreamPush)) {
					//System.out.println("downStream: "+cliIndex+" pushing");

					int ind=ois.readInt();
					K key=(K)ois.readObject();
					V value=(V)ois.readObject();

					mapList.get(ind).merge(key, value,
						(v1, v2) -> combiner.addAll(v1, v2));
					//System.out.println("Server get "+key+" "+value+" from "+cliIndex);
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
		System.out.println("Migration Time: "+cliIndex+" "+(System.currentTimeMillis()-startTime));

	}

}

class DownStreamOnceHandler<K,V> extends DownStreamSplitHandler<K, V> implements Runnable {

	private AtomicInteger pushed;
	private AtomicBoolean pullable;

	DownStreamOnceHandler(ObjectInputStream ois, ObjectOutputStream oos, Socket s,
						  ArrayList<ConcurrentHashMap<K, V>> mapList, Combiner<V> combiner,
						  AtomicInteger pushed, AtomicBoolean pullable) {
		super(ois, oos, s, mapList, combiner);
		this.pushed=pushed;
		this.pullable=pullable;
	}

	@Override
	public void run() {
		long startTime=System.nanoTime();
		try {
			cliIndex = ois.readInt();
			//System.out.println("migration thread");
			while (true) {
				String cmd = ois.readUTF();
				if (cmd.contains(ClientServerProtocol.downStreamPull)) { //
					//System.out.println("downStream: "+cliIndex+" pulling");
					if (pushed.incrementAndGet() == MigrationServer.parallelism) { // pull means push end
						pullable.set(true);
					}
					while (true) {
						if (pullable.get()) {
							oos.writeObject(mapList.get(cliIndex));
							oos.flush();
							mapList.get(cliIndex).clear();
							break;
						}
					}

				} else if (cmd.contains(ClientServerProtocol.downStreamPush)) {
					//System.out.println("downStream: "+cliIndex+" pushing");
					int ind = ois.readInt();
					K key = (K) ois.readObject();
					V value = (V) ois.readObject();

					mapList.get(ind).merge(key, value,
						(v1, v2) -> combiner.addAll(v1, v2));
					//System.out.println("OnceServer get " + key + " " + value + " from " + cliIndex);
				} else if (cmd.contains(ClientServerProtocol.downStreamClose)) {
					System.out.println("Migration Time: "+cliIndex+" "+((System.nanoTime()-startTime)/1000000));
					socket.close();
					if (pushed.decrementAndGet() ==0) {
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

