package org.apache.flink.app;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.coordinator.MyPF;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashSet;

public class UpStreamPF<K> implements Partitioner<K> {
	private MyPF<K> pf;
	private HashSet<K> set=new HashSet<>();

	UpStreamPF() { fetchPF(0);
	}

	private void fetchPF(int barrierID) {
		try {
			Socket socket=new Socket(ClientServerProtocol.host, ClientServerProtocol.portController);
			ObjectOutputStream oos=new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream ois=new ObjectInputStream(socket.getInputStream());

			oos.writeUTF(ClientServerProtocol.upStreamStart);
			oos.writeInt(barrierID);
			oos.flush();
			String cmd=ois.readUTF();
			if (cmd.contains(ClientServerProtocol.upStreamFetch)){
				pf = (MyPF<K>) ois.readObject();
				//System.out.println("PF Fetched");
			}
			if (cmd.contains(ClientServerProtocol.upStreamMetricStart)) {

			}
			socket.close();


		} catch (IOException | ClassNotFoundException e) {
			System.out.println("fetchPF failed");
			e.printStackTrace();
		}
	}


	@Override
	public int partition(K key, int numPartitions) {
		//System.out.println("PPP "+Thread.currentThread().getName()+" "+Thread.currentThread().getId());
		set.add(key);
		//System.out.print("UP: ");
		int index=Thread.currentThread().getName().indexOf('#'), barrierID;
		if ( index >= 0) {
			barrierID=(Integer.parseInt(Thread.currentThread().getName().substring(index+1)));
			//System.out.println("#"+barrierID+set);
			fetchPF(barrierID);
			Thread.currentThread().setName(Thread.currentThread().getName().substring(0, index));
		}
		//System.out.println(key+"=>"+pf.partition(key, numPartitions));
		return pf.partition(key, numPartitions);
	}


}
