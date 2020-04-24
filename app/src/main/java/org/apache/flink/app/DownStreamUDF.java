package org.apache.flink.app;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.coordinator.MyPF;
import org.apache.flink.util.Collector;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;

public class DownStreamUDF extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

	/**
	 * The ValueState handle. The first field is the count, the second field a running sum.
	 */

	private ObjectOutputStream oos;
	private HashMap<Integer, Integer> m= new HashMap<>();

	DownStreamUDF(){
		System.out.println("The Window is Inited");
	}
	@Override
	public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> out) throws Exception {

		System.out.print("Down: "+Thread.currentThread().getName()+" ");
		if (Thread.currentThread().getName().indexOf('#') >= 0) {
			downStreamPull();
		}

		m.put(input.f0, m.getOrDefault(input.f0, 0)+input.f1); //Modified default value suit V
		System.out.println(m);

		out.collect(input);
	}

	@Override
	public void open(Configuration config) {
		System.out.print(getRuntimeContext().getIndexOfThisSubtask()+" PID "+Thread.currentThread().getId()+": ");
	}

	private void downStreamPull() {
		try (Socket socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.port)) {
			oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeUTF(ClientServerProtocol.downStreamStart);
			oos.writeInt(getRuntimeContext().getIndexOfThisSubtask());
			oos.writeUTF(ClientServerProtocol.downStreamPull);
			oos.flush();

			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			Partitioner<Integer> partitionFunction = (Partitioner<Integer>) ois.readObject();
			HashMap<Integer, Integer> incomingMap = (HashMap<Integer, Integer>) ois.readObject();
			downStreamPush(partitionFunction);
			downStreamMerge(incomingMap);

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("socket open error");
		}

	}
	private void downStreamPush(Partitioner<Integer> p) {
		System.out.println(getRuntimeContext().getIndexOfThisSubtask()+" pushing");
		for (HashMap.Entry<Integer, Integer> entry : m.entrySet()) {
			int tar = p.partition(entry.getKey(), getRuntimeContext().getNumberOfParallelSubtasks());
			if (tar != getRuntimeContext().getIndexOfThisSubtask()) {
				try {
					oos.writeUTF(ClientServerProtocol.downStreamPush);
					oos.writeInt(tar);
					oos.writeObject(entry.getKey()); oos.writeObject(entry.getValue());
					m.remove(entry.getKey());
				} catch (IOException e) {e.printStackTrace();}
			}
		}
		try {
			oos.writeUTF(ClientServerProtocol.downStreamClose);
		} catch (IOException e) { e.printStackTrace(); }

	}
	private void downStreamMerge(HashMap<Integer, Integer> incomingMap) {
		incomingMap.forEach((key, value) -> m.merge(
			key, value,
			(mk, mv) -> value + mv
		));
	}
}

