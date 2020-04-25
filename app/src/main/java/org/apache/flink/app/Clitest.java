package org.apache.flink.app;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.coordinator.MyPF;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;

public class Clitest {
	public static void main(String[] args) throws Exception {
		HashMap<Integer, Tuple2<Integer, String>> m=new HashMap<>(), incomingMap=new HashMap<>();
		m.put(2, Tuple2.of(2, "AB AB "));
		incomingMap.put(2, Tuple2.of(4, "AB AB AB AB "));
		incomingMap.put(4, Tuple2.of(1, "ABCD "));

		System.out.print("\n"+m+"+"+incomingMap+"=");
		incomingMap.forEach((key, value) -> m.merge(
			key, value, (v1, v2) -> Tuple2.of(v1.f0+v2.f0, v1.f1 + v2.f1)
		));
		System.out.println(m+"\n");

		/*
		Socket s=new Socket(ClientServerProtocol.host, ClientServerProtocol.portMigration);
		ObjectOutputStream oos=new ObjectOutputStream(s.getOutputStream());
		ObjectInputStream ois=new ObjectInputStream(s.getInputStream());
		oos.writeUTF(ClientServerProtocol.upStreamStart);
		oos.flush();
		MyPF<Integer> tmp=(MyPF<Integer>) ois.readObject();
		System.out.println("got "+tmp.hashCode());
		s.close();
		*/


	}

}
