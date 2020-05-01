package org.apache.flink.MigrationApi;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;


public abstract class AbstractDownStream<IN, OUT, K, V> extends RichFlatMapFunction<IN, OUT> {

	/**
	 * The ValueState handle. The first field is the count, the second field a running sum.
	 */

	HashMap<K, V> m= new HashMap<>();
	private KeySelector<IN, K> keySelector;
	private KeySelector<IN, V> valueSelector;
	private Combiner<V> combiner;
	private SizeCalculator<K, V> sizeCalculator;
	private V defaultV;
	int stateSize;

	AbstractDownStream(KeySelector<IN, K> keySelector, KeySelector<IN, V> valueSelector,
					   Combiner<V> combiner, SizeCalculator<K, V> sizeCalculator, V defaultV) {
		System.out.println("The Window is Inited");
		this.keySelector = keySelector;
		this.valueSelector = valueSelector;
		this.combiner = combiner;
		this.sizeCalculator=sizeCalculator;
		this.defaultV = defaultV;
		this.stateSize = 0;
	}

	@Override
	public void flatMap(IN input, Collector<OUT> out) throws Exception {


		int index=Thread.currentThread().getName().indexOf('#');
		if ( index >= 0) {
			int barrierID=Integer.parseInt(Thread.currentThread().getName().substring(index+1));
			//System.out.print("#"+barrierID);
			downStreamOnBarrier(barrierID);
			Thread.currentThread().setName(Thread.currentThread().getName().substring(0, index));
		}

		m.put(selectKey(input),
			addOne(m.getOrDefault(selectKey(input), defaultV),selectValue(input)));
		stateSize+=selectSize(selectKey(input), selectValue(input));
		System.out.println("Down:"+getRuntimeContext().getIndexOfThisSubtask()+" ["+stateSize+"] "+m);

		udf(input, out);
	}

	public void udf(IN input, Collector<OUT> out) {

	}
	private void downStreamOnBarrier(int barrierID) {
		try {
			Socket socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portController);
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			oos.writeUTF(ClientServerProtocol.downStreamStart);
			oos.writeInt(barrierID);
			oos.writeInt(getRuntimeContext().getIndexOfThisSubtask());
			oos.writeInt(stateSize);
			oos.flush();
			String cmd=ois.readUTF();
			if (cmd.contains(ClientServerProtocol.downStreamMetricStart)) {
				int entireHotKeyNum=ois.readInt();
				LinkedList<K> localHotKey=new LinkedList<>();
				for (int i = 0; i < entireHotKeyNum; i++){
					K tmp_key=(K) ois.readObject();
					if (m.containsKey(tmp_key)) localHotKey.add(tmp_key);
				}

				oos.writeInt(localHotKey.size());
				for (K i : localHotKey) oos.writeObject(i);
				oos.flush();
			}
			if (cmd.contains(ClientServerProtocol.downStreamSplitMigrationStart)
				|| cmd.contains(ClientServerProtocol.downStreamOnceMigrationStart)) {
				Partitioner<K> partitionFunction = (Partitioner<K>) ois.readObject();
				socket.close();
				downStreamMerge(migrate(partitionFunction));
			}
			socket.close();
		} catch (Exception e) {
			System.out.println("downStream on barrier error");
			e.printStackTrace();
		}
	}
	HashMap<K, V> migrate(Partitioner<K> partitionFunction) {
		return null;
	}
	private void downStreamMerge(HashMap<K, V> incomingMap) {
		System.out.print("\n\n"+m+"+"+incomingMap+"=");
		incomingMap.forEach((key, value) -> {
			stateSize+=selectSize(key, value);
			m.merge(key, value, this::addAll);
		});
		System.out.println(m+"\n");
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
	int selectSize(K key, V value) {
		return sizeCalculator.size(key, value);
	}

}

