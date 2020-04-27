package org.apache.flink.app;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SkewnessDetector<T, K> extends RichFlatMapFunction<T, T> {

	private SpaceSaving<K> spaceSaving=new SpaceSaving<>();
	private List lastHK;
	private KeySelector<T, K> keySelector;

	SkewnessDetector(KeySelector<T, K> ks) {
		keySelector= ks;
	}

	@Override
	public void flatMap(T value, Collector<T> out) throws Exception {
		spaceSaving.addKey(keySelector.getKey(value));

		int index=Thread.currentThread().getName().indexOf('#');
		int barrierID;
		//System.out.println("Detectorname: "+Thread.currentThread().getName());
		//System.out.println(keySelector.getKey(value));
		if ( index >= 0) {
			HashMap<K, Integer> curHK=spaceSaving.getHotKey();
			barrierID = Integer.parseInt(Thread.currentThread().getName().substring(index + 1));
			System.out.println("Detector: " + barrierID);
			Socket socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portController);
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			oos.writeUTF(ClientServerProtocol.sourceStart);
			oos.writeInt(barrierID);
			if (differentHK(curHK, barrierID)) { //in differentHK, barrierID>0 && barrierID%5==0 not needed
				oos.writeUTF(ClientServerProtocol.sourceHotKey);
				oos.flush();
				if (ois.readUTF().contains(ClientServerProtocol.sourceAcceptHotKey)) {
					oos.writeInt(curHK.size());
					oos.writeInt(spaceSaving.getTotal());
					oos.flush();
					for (HashMap.Entry<K, Integer> entry : curHK.entrySet()) {
						oos.writeObject(entry.getKey());
						oos.writeInt(entry.getValue());
					}
					oos.flush();
				}

			}else{
				oos.writeUTF(ClientServerProtocol.sourceEnd);
				oos.flush();
			}
			oos.flush();
			socket.close();
			Thread.currentThread().setName(Thread.currentThread().getName().substring(0, index));
		}

		out.collect(value);
	}
	private boolean differentHK(HashMap<K, Integer> curHK, int barrierID) {
		return barrierID > 0 && barrierID % 5 == 0;
	}
}


class SpaceSaving<K> implements Serializable {
	//TODO: space saving
	private HashMap<K, Integer> hotKey;
	private int total;
	SpaceSaving() {
		hotKey=new HashMap<>();
		total=0;
	}
	HashMap<K, Integer> getHotKey() {
		HashMap<K, Integer> above20 = new HashMap<>();
		for (HashMap.Entry<K, Integer> entry: hotKey.entrySet()) if (((float)entry.getValue())/total>0.25)
			above20.put(entry.getKey(), entry.getValue());
		return above20;
	}
	void addKey(K key) {
		if (key instanceof Integer) {
			hotKey.put(key, 1+hotKey.getOrDefault(key, 0));
			total++;
		}
	}

	int getTotal() {
		return total;
	}
}
