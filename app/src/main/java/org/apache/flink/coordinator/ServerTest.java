package org.apache.flink.coordinator;

import java.util.HashMap;

public class ServerTest {
	public static void main(String[] args) {
		HyperRouteProviderSplit<Integer> ms=new HyperRouteProviderSplit<>();
		ms.addKey(0, 8,0, 0.3f);
		ms.addKey(1, 8,0, 0.25f);
		ms.addKey(2, 8,1, 0.4f);
		ms.addKey(3, 8,0, 0.03f);
		HashMap<Integer, Integer> ori=new HashMap<>(), tar=new HashMap<>();
		ori.put(3, 1);
		ori.put(2, 1);
		ori.put(1, 0);
		ori.put(0, 1);

		tar.put(2, 1);
		tar.put(0, 0);
		ms.prepare(tar);
		while (ms.hasNextHyperRoute()) {
			System.out.println(ms.nextHyperRoute());
		}



		System.out.println("=======");
		ms.addKey(0, 7,1, 0.1f);
		ms.addKey(1, 7,1, 0.25f);
		ms.addKey(2, 7,0, 0.05f);
		ms.addKey(3, 7,0, 0.15f);
		ori=new HashMap<>();
		ori.put(3, 1);
		ori.put(2, 1);
		ori.put(1, 0);
		ori.put(0, 1);
		ms.prepare(tar);
		while (ms.hasNextHyperRoute()) {
			System.out.println(" "+ms.nextHyperRoute());
		}

		/*
		PFConstructor<Integer> pfc=new PFConstructor<>(40, 1.3f);
		HashMap<Integer, Float> hashMapHotKey=new HashMap<>();
		hashMapHotKey.put(1, 0.38f); hashMapHotKey.put(3, 0.42f);

		pfc.setHotKey(hashMapHotKey);
		pfc.addMetric(1, new ArrayList<>(Arrays.asList(1, 3)));
		pfc.updatePFnew();

		System.out.println(pfc.getPF().getHyperRoute());

		System.out.println(pfc.getPF().partition(3, 10));
		 */

	}
}
