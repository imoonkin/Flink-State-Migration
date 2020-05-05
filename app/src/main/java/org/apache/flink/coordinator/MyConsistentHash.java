package org.apache.flink.coordinator;

import org.apache.flink.MigrationApi.ClientServerProtocol;

import java.io.Serializable;

class MyConsistentHash<K> implements Serializable {
//TODO: Consistent Hash
	private int yu;

	MyConsistentHash(int parallelism) {
		yu=parallelism;
	}
	int hash(K key) {
		int kk=key instanceof Integer ? ((Integer) key) : 0;
		return kk%yu;
	}

}
