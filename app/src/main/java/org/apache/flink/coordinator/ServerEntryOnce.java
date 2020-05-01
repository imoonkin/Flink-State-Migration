package org.apache.flink.coordinator;

public class ServerEntryOnce {
	public static void main(String[] args) throws Exception{
		HyperRouteProvider<Integer> once=new HyperRouteProviderOnce<>();
		PFConstructor<Integer> pfc= new PFConstructor<>(30, 1.3f, once);
		Thread t=new Thread(new MigrationServer());
		t.start();
		Thread t1=new Thread(new Controller<Integer>(pfc));
		t1.start();
		t.join();
		t1.join();
	}
}
