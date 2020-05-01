package org.apache.flink.coordinator;

import java.util.HashMap;

public interface HyperRouteProvider<K> {
	void addKey(K key, int oriPos, int newPos, float frequency);
	void prepare(HashMap<K, Integer> finalHyperRoute);
	boolean hasNextHyperRoute();
	HashMap<K, Integer> nextHyperRoute();

}
