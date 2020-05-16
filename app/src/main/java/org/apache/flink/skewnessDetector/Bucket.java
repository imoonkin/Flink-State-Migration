package org.apache.flink.skewnessDetector;

import java.util.LinkedList;

public class Bucket<T> {
	private long value;
	private LinkedList<Counter<T>> childrenCounter = new LinkedList<>();

	public Bucket() {
	}

	public Bucket(long value) {
		this.value = value;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public LinkedList<Counter<T>> getChildrenCounter() {
		return childrenCounter;
	}

	public void setChildrenCounter(LinkedList<Counter<T>> childrenCounter) {
		this.childrenCounter = childrenCounter;
	}

	public Counter<T> getFirstCounter() {
		return childrenCounter.getFirst();
	}
}
