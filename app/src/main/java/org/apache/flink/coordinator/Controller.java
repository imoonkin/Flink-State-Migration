package org.apache.flink.coordinator;

import org.apache.flink.app.ClientServerProtocol;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Controller implements Runnable{
	private PFConstructor pfc;
	Controller(PFConstructor pfc) {
		this.pfc=pfc;
	}
	@Override
	public void run() {
		ServerSocket serverSocket;

		int startID=4, endID=9999;

		try {
			serverSocket = new ServerSocket(ClientServerProtocol.portController);
			while (true) {
				Socket socket = serverSocket.accept();
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				String cli = ois.readUTF();
				if (cli.contains(ClientServerProtocol.sourceStart)) {
					// receive hot key, send to PF constructor, update PF, set barrier seq(startID, endID=startID+3),
					startID = ois.readInt();
					endID=startID+2;
					new Thread(new SourceCmd(ois, oos, socket, startID, endID, pfc)).start();
				} else if (cli.contains(ClientServerProtocol.upStreamStart)) {
					new Thread(new UpStreamCmd(ois, oos, socket, startID, endID, pfc)).start();

				} else if (cli.contains(ClientServerProtocol.downStreamStart)) {
					new Thread(new DownStreamCmd(ois, oos, socket, startID, endID, pfc)).start();
				}
			}
		}catch (Exception e){
			System.out.println("controller error"); e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception{
		PFConstructor pfc=new PFConstructor();
		Thread t=new Thread(new MigrationServer(pfc));
		t.start();
		Thread t1=new Thread(new Controller(pfc));
		t1.start();
		t.join();
		t1.join();
	}
}

class SourceCmd implements Runnable {
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	private Socket socket;
	private int barrierID, startID, endID;
	private PFConstructor pfc;
	SourceCmd(ObjectInputStream ois, ObjectOutputStream oos, Socket s,
				int start, int end, PFConstructor pf) throws Exception {
		this.oos = oos;
		this.ois = ois;
		socket = s;
		startID=start;
		endID=end;
		pfc=pf;
	}
	@Override
	public void run() {
		try {
			pfc.updatePF();
			//System.out.println("===PF UPDATED==="+pfc.getPF().partition(3, 10));
			oos.writeUTF(ClientServerProtocol.sourceEnd);
			oos.flush();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

class UpStreamCmd implements Runnable {
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	private Socket socket;
	private int barrierID, startID, endID;
	private PFConstructor pfc;
	UpStreamCmd(ObjectInputStream ois, ObjectOutputStream oos, Socket s,
				int start, int end, PFConstructor pf) throws Exception {
		this.oos = oos;
		this.ois = ois;
		socket = s;
		startID=start;
		endID=end;
		pfc=pf;
	}
	@Override
	public void run() {
		//id=0 fetchPF
		//id in seq
		try {
			barrierID = ois.readInt();
			String cmd="";
			if (barrierID==0 || (startID<=barrierID && barrierID<=endID)) cmd=cmd+ClientServerProtocol.upStreamFetch;
			if (startID<=barrierID && barrierID<=endID) cmd=cmd+ClientServerProtocol.upStreamMetricStart;
			oos.writeUTF(cmd);
			oos.flush();
			if (cmd.contains(ClientServerProtocol.upStreamFetch)) {
				oos.writeObject(pfc.getPF());
				oos.flush();
			}
			if (cmd.contains(ClientServerProtocol.upStreamMetricStart)){

			}
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

class DownStreamCmd implements Runnable {
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	private Socket socket;
	private int barrierID, startID, endID;
	private PFConstructor pfc;

	DownStreamCmd(ObjectInputStream ois, ObjectOutputStream oos, Socket s,
				  int start, int end, PFConstructor pf) throws Exception {
		this.oos = oos;
		this.ois = ois;
		socket = s;
		startID=start;
		endID=end;
		pfc=pf;
	}
	@Override
	public void run() {
		//id in seq
		try {
			barrierID = ois.readInt();
			String cmd="";
			if (startID<=barrierID && barrierID<=endID)	cmd=cmd+ClientServerProtocol.downStreamMigrationStart;
			if (startID<=barrierID && barrierID<=endID) cmd=cmd+ClientServerProtocol.downStreamMetricStart;
			oos.writeUTF(cmd);
			oos.flush();
			if (cmd.contains(ClientServerProtocol.downStreamMetricStart)){

			}
			if (cmd.contains(ClientServerProtocol.downStreamMigrationStart)) {
				oos.writeObject(pfc.getPF());
				oos.flush();
			}

			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
