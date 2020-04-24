package org.apache.flink.coordinator;

import org.apache.flink.api.common.functions.Partitioner;

import java.io.Serializable;
import java.util.HashSet;

public class MyPF<K> implements Serializable, Partitioner<K> {
	private HashSet<K> set=new HashSet<>();

	public int partition(K key, int n) {
		set.add(key);
		System.out.print("UP: "+Thread.currentThread().getName()+" "+Thread.currentThread().getId()+" ");
		System.out.println(set);
		int kk=key instanceof Integer ? ((Integer) key) : 0;
		if (kk%2==0)
			return 0;
		else return 1;
	}
}
