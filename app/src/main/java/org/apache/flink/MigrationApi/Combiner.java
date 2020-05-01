package org.apache.flink.MigrationApi;

public interface Combiner<T> {
	T addOne(T t1, T t2);
	T addAll(T t1, T t2);
}
