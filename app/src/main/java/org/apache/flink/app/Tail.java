package org.apache.flink.app;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.coordinator.MyPF;
import org.apache.flink.util.Collector;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

//deprecated
public class Tail<T> extends RichFlatMapFunction<T, T> {
	private T lastElement=null;
	@Override
	public void flatMap(T value, Collector<T> out) throws Exception {
		if (lastElement!=value) lastElement=value;
		int index=Thread.currentThread().getName().indexOf('#'), barrierID;
		if ( index >= 0) {
			barrierID=(Integer.parseInt(Thread.currentThread().getName().substring(index+1)));
			//System.out.println("#"+barrierID+set);
			Socket socket=new Socket(ClientServerProtocol.host, ClientServerProtocol.portController);
			ObjectOutputStream oos=new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream ois=new ObjectInputStream(socket.getInputStream());

			oos.writeUTF(ClientServerProtocol.tailStart);
			oos.writeInt(barrierID);
			oos.flush();
			socket.close();

			Thread.currentThread().setName(Thread.currentThread().getName().substring(0, index));
		}
		out.collect(value);
	}
}
