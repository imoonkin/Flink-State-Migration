package org.apache.flink.MigrationApi;

public interface SizeCalculator<K, V> {
	int size(K key, V value);
}
