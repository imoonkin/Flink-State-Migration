package org.apache.flink.coordinator;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.MigrationApi.ClientServerProtocol;

import java.util.HashMap;
import java.util.PriorityQueue;

class HyperRouteProviderSplit<K> implements HyperRouteProvider<K>{
	private PriorityQueue<Tuple3<K, Integer, Float>> queue;
	private int totalChunkNum, curChunkNum; //TODO: not accurate;
	private HashMap<K, Integer> curRoute, finalHyperRoute;
	private float totalFrequency, perChunkFrequency;
	private int delay;
	private boolean inProgress;

	HyperRouteProviderSplit(int chunkNum) {
		queue=new PriorityQueue<>((x, y) -> Float.compare(y.f2, x.f2));
		totalChunkNum =chunkNum;
		curChunkNum=0;
		totalFrequency=0;
		delay=1;
		inProgress=false;
		curRoute = new HashMap<>();
	}

	@Override
	public void addKey(K key, int oriPos, int newPos, float frequency) { // new key, old pos, new pos
		if (oriPos==newPos) System.out.println("WARNING: same pos key added!!!");
		curRoute.put(key, oriPos);
		queue.offer(Tuple3.of(key, newPos, frequency));
		totalFrequency+=frequency;
	}

	@Override
	public void prepare(HashMap<K, Integer> finalHyperRoute) { // original pos
		this.finalHyperRoute=finalHyperRoute;
		curChunkNum=0;
		inProgress=true;
		perChunkFrequency=totalFrequency/totalChunkNum;
		//System.out.println("init "+perChunkFrequency+" "+totalFrequency+" "+totalChunkNum);
	}

	@Override
	public boolean hasNextHyperRoute() {
		if (!inProgress) return false;
		if (queue.size() > 0 || delay > 0) {
			return true;
		} else { 						//reset
			totalFrequency=0f;
			curChunkNum=0;
			delay=1;
			inProgress=false;
			curRoute.clear();
			return false;
		}
	}
	@Override
	public HashMap<K, Integer> nextHyperRoute(){
		float curFrequency=0f;
		Tuple3<K, Integer, Float> head;
		if (!queue.isEmpty()) {
			head = queue.poll();
			curRoute.put(head.f0, head.f1);
			curFrequency += head.f2;
		} else if (delay>0){
			delay--;
			return finalHyperRoute;
		} else {
			System.out.println("ERROR: Migration Splitter required empty queue");
		}
		while (!queue.isEmpty() && queue.peek().f2 + curFrequency <= perChunkFrequency) {
			head=queue.poll();
			curRoute.put(head.f0, head.f1);
			curFrequency += head.f2;
		}
		//System.out.println(curChunkNum+" "+ curFrequency+" q:"+queue+" "+curRoute);
		return curRoute;
	}
}
