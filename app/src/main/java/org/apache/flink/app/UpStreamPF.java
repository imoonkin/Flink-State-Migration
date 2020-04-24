package org.apache.flink.app;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.coordinator.MyPF;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class UpStreamPF<K> implements Partitioner<K> {
	private MyPF<K> pf;
	public UpStreamPF() {
		pf=fetchPF();
	}

	private MyPF<K> fetchPF() {
		try {
			Socket socket=new Socket(ClientServerProtocol.host, ClientServerProtocol.port);
			ObjectOutputStream oos=new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream ois=new ObjectInputStream(socket.getInputStream());
			oos.writeUTF(ClientServerProtocol.upStreamStart);
			oos.flush();
			MyPF<K> tmp=(MyPF<K>) ois.readObject();
			socket.close();
			return tmp;
		} catch (IOException | ClassNotFoundException e) {
			System.out.println("fetchPF failed");
			e.printStackTrace();
			return null;
		}
	}


	@Override
	public int partition(K key, int numPartitions) {
		//System.out.println("PPP "+Thread.currentThread().getName()+" "+Thread.currentThread().getId());
		return pf.partition(key, numPartitions);
	}


}
