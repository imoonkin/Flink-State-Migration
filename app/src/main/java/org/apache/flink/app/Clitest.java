package org.apache.flink.app;

import org.apache.flink.coordinator.MyPF;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Clitest {
	public static void main(String[] args) throws Exception {
		Socket s=new Socket(ClientServerProtocol.host, ClientServerProtocol.port);
		ObjectOutputStream oos=new ObjectOutputStream(s.getOutputStream());
		ObjectInputStream ois=new ObjectInputStream(s.getInputStream());
		oos.writeUTF(ClientServerProtocol.upStreamStart);
		oos.flush();
		MyPF<Integer> tmp=(MyPF<Integer>) ois.readObject();
		System.out.println("got "+tmp.hashCode());
		s.close();
	}

}
