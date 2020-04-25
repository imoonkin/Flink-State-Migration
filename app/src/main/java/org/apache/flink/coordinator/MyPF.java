package org.apache.flink.coordinator;

import org.apache.flink.api.common.functions.Partitioner;

import java.io.Serializable;
import java.util.HashSet;

public class MyPF<K> implements Serializable, Partitioner<K> {
	private int yu=0;
	public int partition(K key, int n) {
		int kk=key instanceof Integer ? ((Integer) key) : 0;
		if (kk%2==yu)
			return 0;
		else return 1;
	}
	void setYu(int y) {
		yu=y;
	}
	int getYu() {
		return yu;
	}
}
