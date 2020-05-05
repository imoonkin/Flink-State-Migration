package org.apache.flink.coordinator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class DataSender implements Runnable{
	private ServerSocket serverSocket;
	private DataConstructor dataConstructor;
	private AtomicBoolean isAppRunning, isSending;
	private int height;
	DataSender(DataConstructor dataConstructor, int height, AtomicBoolean isAppRunning, AtomicBoolean isSending) {
		serverSocket=null;
		this.dataConstructor=dataConstructor;
		this.isAppRunning = isAppRunning;
		this.isSending=isSending;
		this.height=height;
	}
	void setStop() throws IOException {
		serverSocket.close();
		//scanner.close();
	}
	@Override
	public void run() {
		try {
			serverSocket = new ServerSocket(9999);
			Socket socket = serverSocket.accept();

			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
			dos.writeBytes(dataConstructor.all);
			dos.flush();
			while (!isSending.get()) {
			}
			while (isSending.get()) {
				//Socket s=serverSocket.accept();
				//dos = new DataOutputStream(s.getOutputStream());
				dos.writeBytes(dataConstructor.all);
				for (int i = 0; i < height; i++) {
					dos.writeBytes(dataConstructor.skewness);
					dos.flush();
					//System.out.println(i);
				}
				dos.flush();
				Thread.sleep(1000);
			}
		} catch (SocketException e) {
			System.out.println("last datasource closed");
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		} finally {
			isSending.set(false);
			isAppRunning.set(false);
			if (serverSocket != null)
				try {
					serverSocket.close();
					System.out.println("close datasource server socket");
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println("close datasource server socket failed");
				}
			//if (scanner != null) scanner.close();
		}


	}
}
