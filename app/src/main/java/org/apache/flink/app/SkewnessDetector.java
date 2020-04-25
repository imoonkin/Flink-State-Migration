package org.apache.flink.app;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class SkewnessDetector<T> extends RichFlatMapFunction<T, T> {

	@Override
	public void flatMap(T value, Collector<T> out) throws Exception {
		int index=Thread.currentThread().getName().indexOf('#');
		int barrierID;
		//System.out.println("Detectorname: "+Thread.currentThread().getName());
		if ( index >= 0) {
			barrierID=Integer.parseInt(Thread.currentThread().getName().substring(index+1));
			System.out.println("Detector: "+barrierID);
			if (barrierID>0 && barrierID%5==0) {
				Socket socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portController);
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				oos.writeUTF(ClientServerProtocol.sourceStart);
				oos.writeInt(barrierID);
				oos.flush();
				//System.out.println("===Detected===");
				ois.readUTF();
				socket.close();
			}
			Thread.currentThread().setName(Thread.currentThread().getName().substring(0, index));

		}
		out.collect(value);
	}
}
