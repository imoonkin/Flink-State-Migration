package org.apache.flink.coordinator;

import java.io.Serializable;

class MyConsistentHash<K> implements Serializable {

	private int yu=0;
	int hash(K key) {
		int kk=key instanceof Integer ? ((Integer) key) : 0;
		if (kk%2==yu)
			return 0;
		return 1;
	}
	void setYu(int y) {
		yu=y;
	}
	int getYu() {
		return yu;
	}

}
