package org.apache.flink.app;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DownStreamUDF extends RichFlatMapFunction<Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, String>> {

	/**
	 * The ValueState handle. The first field is the count, the second field a running sum.
	 */

	private HashMap<Integer, Tuple2<Integer, String>> m= new HashMap<>();

	DownStreamUDF(){
		System.out.println("The Window is Inited");
	}
	@Override
	public void flatMap(Tuple3<Integer, Integer, String> input, Collector<Tuple3<Integer, Integer, String>> out) throws Exception {

		System.out.print("Down:"+getRuntimeContext().getIndexOfThisSubtask()+" ");
		int index=Thread.currentThread().getName().indexOf('#');
		if ( index >= 0) {
			int barrierID=Integer.parseInt(Thread.currentThread().getName().substring(index+1));
			System.out.print("#"+barrierID);
			downStreamOnBarrier(barrierID);
			Thread.currentThread().setName(Thread.currentThread().getName().substring(0, index));
		}

		m.put(input.f0, new Tuple2<>(m.getOrDefault(input.f0, Tuple2.of(0, "")).f0+input.f1,
			input.f2+" "+m.getOrDefault(input.f0, Tuple2.of(0, "")).f1)); //Modified default value suit V
		System.out.println(m);

		out.collect(input);
	}

	@Override
	public void open(Configuration config) {
		System.out.print(getRuntimeContext().getIndexOfThisSubtask()+" PID "+Thread.currentThread().getId()+": ");
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

			}
			if (cmd.contains(ClientServerProtocol.downStreamMigrationStart)) {
				Partitioner<Integer> partitionFunction = (Partitioner<Integer>) ois.readObject();
				socket.close();
				socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portMigration);
				oos = new ObjectOutputStream(socket.getOutputStream());
				ois = new ObjectInputStream(socket.getInputStream());
				//System.out.println("down stream start migrate");
				oos.writeUTF(ClientServerProtocol.downStreamMigrationStart);
				oos.writeInt(getRuntimeContext().getIndexOfThisSubtask());
				oos.writeUTF(ClientServerProtocol.downStreamPull);
				oos.flush();
				HashMap<Integer, Tuple2<Integer, String>> incomingMap = (HashMap<Integer, Tuple2<Integer, String>>) ois.readObject();
				downStreamPush(partitionFunction, oos);
				downStreamMerge(incomingMap);

			}
			socket.close();
		} catch (Exception e) {
			System.out.println("downStream on barrier error");
			e.printStackTrace();
		}
	}


	private void downStreamPush(Partitioner<Integer> p, ObjectOutputStream oos) {
		//System.out.println(" "+getRuntimeContext().getIndexOfThisSubtask()+" pushing");
		for (Iterator<HashMap.Entry<Integer, Tuple2<Integer, String>>> it= m.entrySet().iterator(); it.hasNext();) {
			HashMap.Entry<Integer, Tuple2<Integer, String>> entry=it.next();
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
		} catch (IOException e) { e.printStackTrace(); }

	}
	private void downStreamMerge(HashMap<Integer, Tuple2<Integer, String>> incomingMap) {
		System.out.print("\n"+m+"+"+incomingMap+"=");
		incomingMap.forEach((key, value) -> m.merge(
			key, value, (v1, v2) -> Tuple2.of(v1.f0+v2.f0, v1.f1 + v2.f1)
		));
		System.out.println(m+"\n");
	}
}

