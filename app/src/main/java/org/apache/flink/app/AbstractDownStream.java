package org.apache.flink.app;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.security.Key;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

interface Combiner<T> {
	T addOne(T t1, T t2);
	T addAll(T t1, T t2);
}


public abstract class AbstractDownStream<IN, OUT, K, V> extends RichFlatMapFunction<IN, OUT> {

	/**
	 * The ValueState handle. The first field is the count, the second field a running sum.
	 */

	private HashMap<K, V> m= new HashMap<>();
	private KeySelector<IN, K> keySelector;
	private KeySelector<IN, V> valueSelector;
	private Combiner<V> combiner;
	private V defaultV;
	AbstractDownStream(KeySelector<IN, K> keySelector, KeySelector<IN, V> valueSelector,
					   Combiner<V> combiner, V defaultV){
		System.out.println("The Window is Inited");
		this.keySelector=keySelector;
		this.valueSelector=valueSelector;
		this.combiner=combiner;
		this.defaultV=defaultV;
	}

	@Override
	public void flatMap(IN input, Collector<OUT> out) throws Exception {


		int index=Thread.currentThread().getName().indexOf('#');
		if ( index >= 0) {
			int barrierID=Integer.parseInt(Thread.currentThread().getName().substring(index+1));
			System.out.print("#"+barrierID);
			downStreamOnBarrier(barrierID);
			Thread.currentThread().setName(Thread.currentThread().getName().substring(0, index));
		}

		m.put(selectKey(input),
			addOne(m.getOrDefault(selectKey(input), defaultV),selectValue(input)));
			//Modified default value suit V
		System.out.println("Down:"+getRuntimeContext().getIndexOfThisSubtask()+" "+m);

		udf(input, out);
	}

	public void udf(IN input, Collector<OUT> out) {

	}


	@Override
	public void open(Configuration config) {
		System.out.print(getRuntimeContext().getIndexOfThisSubtask()+" PID "+Thread.currentThread().getId()+": ");
	}
	private K selectKey(IN input) throws Exception {
		return keySelector.getKey(input);
	}
	private V selectValue(IN input) throws Exception {
		return valueSelector.getKey(input);
	}
	private V addOne(V v1, V v2) {
		return combiner.addOne(v1, v2);
	}
	private V addAll(V v1, V v2) {
		return combiner.addAll(v1, v2);
	}
	private void downStreamOnBarrier(int barrierID) {
		try {
			Socket socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portController);
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			oos.writeUTF(ClientServerProtocol.downStreamStart);
			oos.writeInt(barrierID);
			oos.flush();
			String cmd=ois.readUTF();
			if (cmd.contains(ClientServerProtocol.downStreamMetricStart)) {
				int entireHotKeyNum=ois.readInt();
				LinkedList<K> localHotKey=new LinkedList<>();
				for (int i = 0; i < entireHotKeyNum; i++){
					K tmp_key=(K) ois.readObject();
					if (m.containsKey(tmp_key)) localHotKey.add(tmp_key);
				}

				oos.writeInt(getRuntimeContext().getIndexOfThisSubtask());
				oos.writeInt(localHotKey.size());
				for (K i : localHotKey) oos.writeObject(i);
				oos.flush();
			}
			if (cmd.contains(ClientServerProtocol.downStreamMigrationStart)) {
				Partitioner<K> partitionFunction = (Partitioner<K>) ois.readObject();
				socket.close();
				socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portMigration);
				oos = new ObjectOutputStream(socket.getOutputStream());
				ois = new ObjectInputStream(socket.getInputStream());
				//System.out.println("down stream start migrate");
				oos.writeUTF(ClientServerProtocol.downStreamMigrationStart);
				oos.writeInt(getRuntimeContext().getIndexOfThisSubtask());
				oos.writeUTF(ClientServerProtocol.downStreamPull);
				oos.flush();
				HashMap<K, V> incomingMap = (HashMap<K, V>) ois.readObject();
				downStreamPush(partitionFunction, oos);
				downStreamMerge(incomingMap);
			}

			socket.close();
		} catch (Exception e) {
			System.out.println("downStream on barrier error");
			e.printStackTrace();
		}
	}


	private void downStreamPush(Partitioner<K> p, ObjectOutputStream oos) {
		//System.out.println(" "+getRuntimeContext().getIndexOfThisSubtask()+" pushing");
		for (Iterator<HashMap.Entry<K, V>> it= m.entrySet().iterator(); it.hasNext();) {
			HashMap.Entry<K, V> entry=it.next();
			int tar = p.partition(entry.getKey(), getRuntimeContext().getNumberOfParallelSubtasks());
			if (tar != getRuntimeContext().getIndexOfThisSubtask()) {
				try {
					oos.writeUTF(ClientServerProtocol.downStreamPush);
					oos.writeInt(tar);
					oos.writeObject(entry.getKey());
					oos.writeObject(entry.getValue());
					//oos.writeObject(entry);
					it.remove();
				} catch (IOException e) {e.printStackTrace();}
			}
		}
		try {
			oos.writeUTF(ClientServerProtocol.downStreamClose);
			oos.flush();
		} catch (IOException e) { e.printStackTrace(); }

	}
	private void downStreamMerge(HashMap<K, V> incomingMap) {
		System.out.print("\n\n"+m+"+"+incomingMap+"=");
		incomingMap.forEach((key, value) -> m.merge(key, value, this::addAll ));
		System.out.println(m+"\n");
	}
}

