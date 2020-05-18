package org.apache.flink.coordinator;

import org.apache.flink.MigrationApi.ClientServerProtocol;
import org.apache.flink.skewnessDetector.SpaceSaving;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class DataSender implements Runnable{
	private int parallel;
	private int rangePerNode;
	private String valueUnit;
	private int hotKeyTimes;
	private int cycle;
	private int prepareLen;

	DataSender(int parallel, int rangePerNode, int hotKeyTimes, int cycle, int prepareLen) {
		this.parallel=parallel;
		this.rangePerNode=rangePerNode;
		this.hotKeyTimes=hotKeyTimes;
		this.cycle=cycle;
		this.prepareLen=prepareLen;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 100; i++) sb.append("A");
		valueUnit=sb.toString();

	}

	@Override
	public void run() {
		ServerSocket serverSocket=null; Socket socket=null;
		try {
			serverSocket=new ServerSocket(ClientServerProtocol.portData);
			socket=serverSocket.accept();
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());

			long cnt=0;
			Set<Integer> hotKeys = new HashSet<>();
			while (true) {
				for (int i = 0; i < parallel; i++) {
					out.writeBytes(getRandData(i));  // send normal key
				}
				for (int k=0; k<hotKeyTimes; k++) {
					for (Integer i : hotKeys) {
						out.writeBytes(getHotData(i));  // send hot key
					}
				}
				out.flush();


				cnt+=parallel+hotKeys.size()*hotKeyTimes;
				if (prepareLen > 0 && cnt > prepareLen) {
					prepareLen=-1;
					cnt=0;
					hotKeys=hotKeyGen();
				}
				if (cnt > cycle && prepareLen<0) {
					cnt=0;
					hotKeys=hotKeyGen();
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				Objects.requireNonNull(socket).close();
				Objects.requireNonNull(serverSocket).close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private String getRandData(int channel) {
		int key=ThreadLocalRandom.current().nextInt(channel*rangePerNode, (channel+1)*rangePerNode); //key
		return String.valueOf(key) + " " + valueUnit + "\n"; //key + value
	}

	private String getHotData(int key) {
		return String.valueOf(key) + " " + valueUnit + "\n";
	}
	private Set<Integer> hotKeyGen() {
		Set<Integer> hotOperator =new HashSet<>(), hotKeys=new HashSet<>();
		int i=0, channel=-1;
		while (i < parallel/2) { //half operators have hot key
			do {
				channel = ThreadLocalRandom.current().nextInt(parallel);
			} while (hotOperator.contains(channel));
			hotOperator.add(channel);
			i++;
		}

		//got hot operators, now gen hot keys

		int hotKeyNumPerOP=(int)(parallel*rangePerNode*0.1)/hotOperator.size(); //evenly distribute
		int key=-1;
		for (Integer op : hotOperator) {
			i=0;
			while(i<hotKeyNumPerOP) {
				do {
					key = ThreadLocalRandom.current().nextInt(op*rangePerNode, (op+1)*rangePerNode);
				} while (hotKeys.contains(key));
				hotKeys.add(key);
				i++;
			}
		}
		return hotKeys;
	}

	public static void main1(String[] args) throws IOException {
		DataSender ds = new DataSender(4, 10, 6, 1000, 100);
		new Thread(ds).start();
		SpaceSaving<Integer> ss=new SpaceSaving<>(0.05f);
		Socket socket = new Socket(ClientServerProtocol.host, ClientServerProtocol.portData);
		DataInputStream input = new DataInputStream(socket.getInputStream());
		int cnt=0; //long start, s2;
		for (int i = 0; i < 40; i++) {
			ss.readItem(i);
		}
		while (true) {
			//start=System.currentTimeMillis();
			ss.readItem(Integer.valueOf(input.readUTF().split(" ")[0]));
			//System.out.println("readTime: "+(System.currentTimeMillis()-start));
			cnt++;
			if (cnt % 1000 == 0) {
				//s2=System.currentTimeMillis();
				System.out.println(ss.getTopK(10));
				//System.out.println(" "+(System.currentTimeMillis()-s2));
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		DataSender ds = new DataSender(4, 10, 6, 1000, 100);
		ds.run();
	}

}
