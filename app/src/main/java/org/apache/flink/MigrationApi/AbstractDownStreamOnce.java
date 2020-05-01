package org.apache.flink.MigrationApi;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractDownStreamOnce<IN, OUT, K, V> extends AbstractDownStream<IN, OUT, K, V> {
	public AbstractDownStreamOnce(KeySelector<IN, K> keySelector, KeySelector<IN, V> valueSelector,
								  Combiner<V> combiner, SizeCalculator<K, V> sizeCalculator, V defaultV) {
		super(keySelector, valueSelector, combiner, sizeCalculator, defaultV);
	}

	@Override
	HashMap<K, V> migrate(Partitioner<K> partitionFunction) {
		try {
			Socket socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portMigration);
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			//System.out.println("down stream start migrate");
			oos.writeUTF(ClientServerProtocol.downStreamOnceMigrationStart);
			oos.writeInt(getRuntimeContext().getIndexOfThisSubtask());

			//System.out.println(" "+getRuntimeContext().getIndexOfThisSubtask()+" pushing");
			for (Iterator<HashMap.Entry<K, V>> it = m.entrySet().iterator(); it.hasNext();) {
				HashMap.Entry<K, V> entry=it.next();
				int tar = partitionFunction.partition(entry.getKey(), getRuntimeContext().getNumberOfParallelSubtasks());
				if (tar != getRuntimeContext().getIndexOfThisSubtask()) {
					oos.writeUTF(ClientServerProtocol.downStreamPush);
					oos.writeInt(tar);
					oos.writeObject(entry.getKey());
					oos.writeObject(entry.getValue());
					stateSize -= selectSize(entry.getKey(), entry.getValue());
					//oos.writeObject(entry);
					it.remove();
				}
			}

			oos.writeUTF(ClientServerProtocol.downStreamPull);
			oos.flush();
			HashMap<K, V> incomingMap = new HashMap<>((ConcurrentHashMap<K, V>) ois.readObject());



			oos.writeUTF(ClientServerProtocol.downStreamClose);
			oos.flush();
			socket.close();
			return incomingMap;

		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("Migration Failed");
			return null;
		}
	}
}
