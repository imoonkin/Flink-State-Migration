package org.apache.flink.coordinator;

public class ServerEntrySplit {
	public static void main(String[] args) throws Exception{
		HyperRouteProvider<Integer> split=new HyperRouteProviderSplit<>(2);
		PFConstructor<Integer> pfc= new PFConstructor<>(30, 2,1.3f, split);
		Thread t=new Thread(new MigrationServer(2));
		t.start();
		Thread t1=new Thread(new Controller<Integer>(pfc));
		t1.start();
		t.join();
		t1.join();
	}
}
