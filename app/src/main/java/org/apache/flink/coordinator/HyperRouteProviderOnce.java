package org.apache.flink.coordinator;

import java.util.HashMap;

public class HyperRouteProviderOnce<K> implements HyperRouteProvider<K>{
	private boolean provide=false;
	private HashMap<K, Integer> finalHyperRoute;
	@Override
	public void addKey(K key, int oriPos, int newPos, float frequency) {
	}

	@Override
	public void prepare(HashMap<K, Integer> finalHyperRoute) {
		provide=true;
		this.finalHyperRoute=finalHyperRoute;
	}

	@Override
	public boolean hasNextHyperRoute() {
		return provide;
	}
	@Override
	public HashMap<K, Integer> nextHyperRoute() {
		provide=false;
		return finalHyperRoute;
	}
}
