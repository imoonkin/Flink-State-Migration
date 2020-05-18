package org.apache.flink.MigrationApi;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;

public class SkewnessDetector<T, K> extends RichFlatMapFunction<T, T> {

	private SkewRecorder<K> skewRecorder;
	private List lastHK;
	private KeySelector<T, K> keySelector;
	private boolean migrated;
	private int migrationTriggerThreshold;
	public SkewnessDetector(KeySelector<T, K> ks, float hotKeyThreshold, int migrationTriggerThreshold) {
		keySelector= ks;
		skewRecorder =new SkewRecorder<>(hotKeyThreshold);
		migrated=false;
		this.migrationTriggerThreshold =migrationTriggerThreshold;
	}

	@Override
	public void flatMap(T value, Collector<T> out) throws Exception {
		skewRecorder.addKey(keySelector.getKey(value));

		int index=Thread.currentThread().getName().indexOf('#');
		int barrierID;
		//System.out.println("Detectorname: "+Thread.currentThread().getName());
		//System.out.println(keySelector.getKey(value));
		//Thread.sleep(100);
		if ( index >= 0) {
			HashMap<K, Integer> curHK= skewRecorder.getHotKey();
			barrierID = Integer.parseInt(Thread.currentThread().getName().substring(index + 1));
			System.out.println("Detector: #" + barrierID);
			Socket socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portController);
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			oos.writeUTF(ClientServerProtocol.sourceStart);
			oos.writeInt(barrierID);
			if (differentHK(curHK, barrierID, skewRecorder.getTotal())) { //in differentHK, barrierID>0 && barrierID%5==0 not needed
				oos.writeUTF(ClientServerProtocol.sourceHotKey);
				oos.flush();
				if (ois.readUTF().contains(ClientServerProtocol.sourceAcceptHotKey)) {
					oos.writeInt(curHK.size());
					oos.writeInt(skewRecorder.getTotal());
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
	private boolean differentHK(HashMap<K, Integer> curHK, int barrierID, int total) {
		if (!migrated && barrierID > 0 && total> migrationTriggerThreshold) {
			migrated=true;
			return true;
		}
		return false;
	}
}


class SkewRecorder<K> implements Serializable {
	//TODO: space saving
	private HashMap<K, Integer> hotKey;
	private int total;
	private float threshold;

	SkewRecorder(float threshold) {
		hotKey=new HashMap<>();
		total=0;
		this.threshold=threshold;
		System.out.println("threshold "+threshold);
	}
	HashMap<K, Integer> getHotKey() {
		HashMap<K, Integer> above20 = new HashMap<>();
		for (HashMap.Entry<K, Integer> entry: hotKey.entrySet()) {
			if (((float)entry.getValue())/total>threshold)
				above20.put(entry.getKey(), entry.getValue());
			//if (entry.getValue()>10) System.out.print(((float)entry.getValue())/total + " ");
		}
		System.out.println("hotKeys: "+total +" "+above20);
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

	public void setThreshold(float threshold) {
		this.threshold = threshold;
	}
}
