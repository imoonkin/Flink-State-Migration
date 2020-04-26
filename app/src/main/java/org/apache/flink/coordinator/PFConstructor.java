package org.apache.flink.coordinator;

import org.apache.flink.app.ClientServerProtocol;

import java.util.*;

class PFConstructor<K> {
	private MyPF<K> pf;
	private HashMap<K, Float> hk, newhk;
	private ArrayList<HashSet<K>> metric;
	private final int parallelism= ClientServerProtocol.downStreamParallelism;
	private final int maxParallelism;
	private int metricCnt=0;
	PFConstructor(int maxP) {
		pf = new MyPF<K>();
		metric = new ArrayList<>();
		for (int i=0; i<maxP; i++) metric.add(new HashSet<>());
		maxParallelism=maxP;
	}
	MyPF<K> getPF() {
		return pf;
	}

	void updatePF() {
		MyConsistentHash<K> newHb=new MyConsistentHash<>();
		if (pf.getHb().getYu() == 0) newHb.setYu(1);
		else newHb.setYu(0);

		pf.setHb(newHb);

		System.out.println("\nNEW HOT KEY : "+newhk+"\n");
		for (int i=0; i<parallelism; i++) System.out.println(metric.get(i));

		hk=newhk;
		for (int i=0; i<maxParallelism; i++) metric.get(i).clear();
		metricCnt=-1;
	}

	void updatePFnew() {
		//new hb
		MyConsistentHash<K> newHb=new MyConsistentHash<>();
		if (pf.getHb().getYu() == 0) newHb.setYu(1);
		else newHb.setYu(0);

		//Algorithm 1 => new hyper route
		Set<K> D_o = new HashSet<>(hk.keySet()), D_a=new HashSet<>(hk.keySet());
		D_o.removeAll(newhk.keySet());
		D_a.addAll(newhk.keySet());
		float m=0, mCeil=0;
		for (K key : D_o) if (pf.partition(key, parallelism) != newHb.hash(key)) {
			m += hk.get(key);
		}
		for (K key:D_a) mCeil += newhk.containsKey(key) ? newhk.get(key) : hk.get(key);
		HashMap<K, Integer> hyperRouteBuffer = new HashMap<>();
		ArrayList<K> D_c=new ArrayList<>(newhk.keySet());
		D_c.sort((x, y)-> { return Float.compare(newhk.get(y), newhk.get(x)); });
		for (K key : D_c) {
			int j=-1, h=pf.partition(key, parallelism); float u=Float.MAX_VALUE;
			for (int l = 0; l < parallelism; l++) {
				float a=balancePenalty(hyperRouteBuffer, key, l),
					r=migrationPenalty(m, newhk.get(key), l, h, mCeil),
					cur_u=computeUtil();
				if (cur_u < u) {
					j=l; u=cur_u;
				}
			}
			if (j!=h) m+=hk.get(key);
			hyperRouteBuffer.put(key, j);
		}

		//update MyPF
		pf.setHyperRoute(hyperRouteBuffer);
		pf.setHb(newHb);

		hk=newhk;
		for (int i=0; i<maxParallelism; i++) metric.get(i).clear();
		metricCnt=-1;

	}
	private float balancePenalty(HashMap<K, Integer> route, K key, int l) {
		return 0;
	}
	private float migrationPenalty(float m, float f, int l, int h, float mCeil) {
		if (l!=h) m+=f;
		return m/mCeil;
	}
	private float computeUtil() {
		return 0;
	}

	void setHotKey(HashMap<K, Float> hotKey) {
		newhk=hotKey;

	}

	Set<K> getNewHotKeySet() {
		return newhk.keySet();
	}

	synchronized boolean addMetric(int index, List<K> arr) {
		metric.get(index).addAll(arr);
		metricCnt++;
		return metricCnt == parallelism;
	}

}
