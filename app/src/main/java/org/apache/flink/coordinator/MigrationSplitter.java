package org.apache.flink.coordinator;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.app.ClientServerProtocol;

import java.util.HashMap;
import java.util.PriorityQueue;

class MigrationSplitter<K> {
	private PriorityQueue<Tuple3<K, Integer, Float>> queue;
	private int totalChunkNum, curChunkNum; //TODO: not accurate;
	private HashMap<K, Integer> curRoute;
	private float totalFrequency, perChunkFrequency;
	private int delay;
	private boolean inProgress;
	MigrationSplitter() {
		queue=new PriorityQueue<>((x, y) -> Float.compare(y.f2, x.f2));
		totalChunkNum =ClientServerProtocol.chunkNum;
		curChunkNum=0;
		totalFrequency=0;
		delay=1;
		inProgress=false;
	}

	void addKey(K key, int pos, float frequency) { // new key, new pos
		queue.offer(Tuple3.of(key, pos, frequency));
		totalFrequency+=frequency;
	}
	void split(HashMap<K, Integer> oriPosition) { // original pos
		curRoute=oriPosition;
		totalChunkNum =ClientServerProtocol.chunkNum;
		curChunkNum=0;
		inProgress=true;
		perChunkFrequency=totalFrequency/totalChunkNum;
		//System.out.println("init "+perChunkFrequency+" "+totalFrequency+" "+totalChunkNum);
	}

	boolean hasNextHyperRoute() {
		if (!inProgress) return false;
		if (queue.size() > 0 || delay > 0) {
			return true;
		} else { 						//reset
			totalFrequency=0f;
			curChunkNum=0;
			delay=1;
			inProgress=false;
			return false;
		}
	}
	HashMap<K, Integer> nextHyperRoute(){
		float curFrequency=0f;
		Tuple3<K, Integer, Float> head;
		if (!queue.isEmpty()) {
			head = queue.poll();
			curRoute.put(head.f0, head.f1);
			curFrequency += head.f2;
		} else if (delay>0){
			delay--;
			return curRoute;
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
