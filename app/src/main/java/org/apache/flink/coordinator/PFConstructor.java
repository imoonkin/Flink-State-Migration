package org.apache.flink.coordinator;

import org.apache.flink.app.ClientServerProtocol;

import java.util.*;

class PFConstructor<K> {
	private MyPF<K> pf;
	private HashMap<K, Float> hk, newhk;
	private ArrayList<HashSet<K>> metric;
	private ArrayList<Float> operatorLoad;
	private final int parallelism= ClientServerProtocol.downStreamParallelism;
	private final int maxParallelism;
	private final float theta;
	private int metricCnt=0;
	PFConstructor(int maxP, float alpha) {
		pf = new MyPF<K>();
		metric = new ArrayList<>();
		operatorLoad = new ArrayList<>();
		for (int i=0; i<maxP; i++) {
			metric.add(new HashSet<>());
			operatorLoad.add(0f);
		}
		maxParallelism=maxP;
		theta=(alpha*parallelism)/(alpha+parallelism-1)-1;
		hk=new HashMap<>();
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
		Set<K> D_o = new HashSet<>(), D_a=new HashSet<>();
		if (!hk.isEmpty()) {
			D_o.addAll(hk.keySet());
			D_a.addAll(hk.keySet());
		}
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
			System.out.println("setting key : "+key+" "+operatorLoad);
			int j=-1, h=pf.partition(key, parallelism); float u=Float.MAX_VALUE;
			for (int l = 0; l < parallelism; l++) {
				float a=balancePenalty(operatorLoad, key, l, hyperRouteBuffer.size()),
					r=migrationPenalty(m, newhk.get(key), l, h, mCeil),
					cur_u=computeUtil(a, r);
				System.out.println("   to "+l+" "+cur_u);
				if (cur_u < u) {
					j=l; u=cur_u;
				}
			}
			if (j!=h) m+=newhk.get(key);
			hyperRouteBuffer.put(key, j);
			operatorLoad.set(j, newhk.get(key) + operatorLoad.get(j));
		}

		//update MyPF
		pf.setHyperRoute(hyperRouteBuffer);
		pf.setHb(newHb);

		hk=newhk;
		for (int i=0; i<maxParallelism; i++) {
			metric.get(i).clear();
			operatorLoad.set(i, 0f);
		}
		metricCnt=-1;

	}
	private float balancePenalty(ArrayList<Float> operatorLoad, K d, int l, int cnt) {
		float max=-1f, min=Float.MAX_VALUE, avg=0;
		operatorLoad.set(l, operatorLoad.get(l)+newhk.get(d));
		for (int i = 0; i < parallelism; i++) {
			max = Math.max(max, operatorLoad.get(i));
			min = Math.min(min, operatorLoad.get(i));
			avg += operatorLoad.get(i);
		}
		System.out.println(d+"=>"+l+"");
		operatorLoad.set(l, operatorLoad.get(l)-newhk.get(d));
		avg = cnt == 0 ? 1 : avg / cnt;
		return (max-min)/(theta*avg);

	}
	private float migrationPenalty(float m, float f, int l, int h, float mCeil) {
		if (l!=h) m+=f;
		return m/mCeil;
	}
	private float computeUtil(float a, float r) {
		return a+r;
		//TODO: try some other functions
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
