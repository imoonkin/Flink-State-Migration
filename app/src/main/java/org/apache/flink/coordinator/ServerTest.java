package org.apache.flink.coordinator;

public class ServerTest {
	public static void main(String[] args) {
		PFConstructor<Integer> pfc=new PFConstructor<>(40);
		System.out.println(pfc.getPF().partition(3, 10));
		pfc.updatePF();
		System.out.println(pfc.getPF().partition(3, 10));
	}
}
