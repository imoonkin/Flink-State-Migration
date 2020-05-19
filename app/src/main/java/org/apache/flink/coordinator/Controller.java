package org.apache.flink.coordinator;

import org.apache.flink.MigrationApi.ClientServerProtocol;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Controller<K> implements Runnable{
	private PFConstructor<K> pfc;
	private boolean isRunning;
	private ServerSocket serverSocket;
	private BlockingQueue<Integer> stateSizeQueue;
	private List<Float> imbalance;
	private int parallel;
	Controller(PFConstructor<K> pfc, int parallel) {
		this.pfc=pfc;
		this.isRunning=true;
		serverSocket=null;
		stateSizeQueue = new LinkedBlockingDeque<>();
		imbalance = new LinkedList<>();
		this.parallel=parallel;
	}

	void setStop() throws IOException {
		isRunning = false;
		serverSocket.close();
	}

	@Override
	public void run() {

		int startID=0, endID=0;

		try {
			serverSocket = new ServerSocket(ClientServerProtocol.portController);
			while (isRunning) {
				Socket socket = serverSocket.accept();
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				String cli = ois.readUTF();
				if (cli.contains(ClientServerProtocol.sourceStart)) {
					// receive hot key, send to PF constructor,  set barrier seq(startID, endID=startID+3),
					startID = ois.readInt();
					System.out.println("# " + startID);
					endID = startID + 3;
					new Thread(new SourceCmd<>(ois, oos, socket, startID, endID, pfc)).start();
				} else if (cli.contains(ClientServerProtocol.upStreamStart)) {
					new Thread(new UpStreamCmd(ois, oos, socket, startID, endID, pfc)).start();

				} else if (cli.contains(ClientServerProtocol.downStreamStart)) {//update PF,
					new Thread(new DownStreamSplitCmd<>(ois, oos, socket, startID, endID, pfc,
						stateSizeQueue, imbalance, parallel)).start();
				}
			}
		} catch (SocketException e) {
			System.out.println("last controller closed ");

		} catch (Exception e) {
			System.out.println("controller error ");
			e.printStackTrace();
		} finally {
			imbalance.forEach((x) -> System.out.format("%.4f ", x)); //print all the way imbalance
			System.out.println(parallel);
			if (serverSocket != null) {
				try {
					serverSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}

class SourceCmd<K> implements Runnable {
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	private Socket socket;
	private int barrierID, startID, endID;
	private PFConstructor<K> pfc;
	SourceCmd(ObjectInputStream ois, ObjectOutputStream oos, Socket s,
				int start, int end, PFConstructor<K> pf) throws Exception {
		this.oos = oos;
		this.ois = ois;
		socket = s;
		startID=start;
		endID=end;
		pfc=pf;
	}
	@Override
	public void run() { // source client needs to wait for server to complete
		try {
			String request=ois.readUTF();

			if (pfc.isMetric()) {
				pfc.setMigrating();
				System.out.println("setMigrating");
			} // set metric then start migrating
			if (pfc.isMigrating()) {
				if (pfc.hasNext()) {
					pfc.updateToNext();
					System.out.println("set updateToNext : "+ pfc.getPF().getHyperRoute());
				}else {
					pfc.setIdle();
					System.out.println("set Idle");
				}
			} else if (request.contains(ClientServerProtocol.sourceHotKey)) {
				oos.writeUTF(ClientServerProtocol.sourceAcceptHotKey);
				oos.flush();
				int hotKeyNum = ois.readInt(), total = ois.readInt();
				HashMap<K, Float> hotKey = new HashMap<>();
				for (int i = 0; i < hotKeyNum; i++) {
					K tmp_key = (K) ois.readObject();
					int tmp_value = ois.readInt();
					hotKey.put(tmp_key, ((float) tmp_value) / total);
				}
				pfc.setHotKey(hotKey);
				pfc.setMetric();
				System.out.println("Source hot key : "+hotKey);
			}
			oos.flush();
			//System.out.println("===PF UPDATED==="+pfc.getPF().partition(3, 10));
			socket.close();
		} catch (IOException | ClassNotFoundException e) {
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
				int start, int end, PFConstructor pf) {
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
			if (barrierID==0 || pfc.isMigrating()) cmd=cmd+ClientServerProtocol.upStreamFetch;
			if (pfc.isMetric()) cmd=cmd+ClientServerProtocol.upStreamMetricStart;
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

class DownStreamSplitCmd<K> implements Runnable {
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	private Socket socket;
	private int barrierID, startID, endID;
	private PFConstructor<K> pfc;
	private final BlockingQueue<Integer> queue;
	private List<Float> imbalance;
	private int parallel;

	DownStreamSplitCmd(ObjectInputStream ois, ObjectOutputStream oos, Socket s,
					   int start, int end, PFConstructor<K> pf, BlockingQueue<Integer> queue,
					   List<Float> imbalance, int parallel) {
		this.oos = oos;
		this.ois = ois;
		socket = s;
		startID=start;
		endID=end;
		pfc=pf;
		this.queue=queue;
		this.imbalance = imbalance;
		this.parallel=parallel;
	}
	@Override
	public void run() {
		//id in seq
		try {
			barrierID = ois.readInt();
			int index=ois.readInt(), stateSize=ois.readInt();
			System.out.println("D: "+index+" ["+stateSize+"]");
			synchronized (queue) {
				queue.offer(stateSize);
				if (queue.size() >= parallel) {
					int max = 0, min = Integer.MAX_VALUE, avg = 0;
					while (!queue.isEmpty()) {
						int top = queue.poll();
						max = Math.max(max, top);
						min = Math.min(min, top);
						avg += top;
					}
					imbalance.add((max - min) / (pfc.theta * avg));
				}
			}
			String cmd="";
			if (pfc.isMigrating())	cmd=cmd+ClientServerProtocol.downStreamSplitMigrationStart;
			if (pfc.isMetric()) cmd=cmd+ClientServerProtocol.downStreamMetricStart;
			//if (pfc.migrationOccurCount>0) cmd = cmd + "Silence";//TODO: silent after migration
			oos.writeUTF(cmd);
			oos.flush();
			boolean needUpdate=false;
			if (cmd.contains(ClientServerProtocol.downStreamMetricStart)){
				Set<K> entireHotKeySet = pfc.getNewHotKeySet();
				oos.writeInt(entireHotKeySet.size());
				for (K key: entireHotKeySet) oos.writeObject(key);
				oos.flush();

				int cnt=ois.readInt();
				List<K> hotKeyArray = new LinkedList<>();
				for (int i=0; i<cnt; i++) hotKeyArray.add((K)ois.readObject());
				System.out.println(index+" hot key "+hotKeyArray);
				needUpdate=pfc.addMetric(index, hotKeyArray);  // updatePF called in addMetric
				if (needUpdate) System.out.println("all Metric sent");
			}
			if (cmd.contains(ClientServerProtocol.downStreamSplitMigrationStart)) {
				oos.writeObject(pfc.getPF());
				oos.flush();
			}
			socket.close();
			if (needUpdate) pfc.updatePFnew();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}


@Deprecated
class TailCmd<K> implements Runnable {
	private ObjectInputStream ois;
	private ObjectOutputStream oos;
	private Socket socket;
	private int barrierID, startID, endID;
	private PFConstructor<K> pfc;
	TailCmd(ObjectInputStream ois, ObjectOutputStream oos, Socket s,
			int start, int end, PFConstructor<K> pf) {
		this.oos = oos;
		this.ois = ois;
		socket = s;
		startID = start;
		endID = end;
		pfc = pf;
	}
	@Override
	public void run() {
		try {
			barrierID=ois.readInt();
			System.out.println("Ends #"+barrierID);
			if (barrierID==startID){
				pfc.updatePFnew();
				System.out.println("pfc updated ");
			}

			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
