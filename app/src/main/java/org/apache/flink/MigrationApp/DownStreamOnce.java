package org.apache.flink.MigrationApp;

import org.apache.flink.MigrationApi.AbstractDownStreamOnce;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class DownStreamOnce extends AbstractDownStreamOnce<Tuple3<Integer, Integer, String>,
	Tuple3<Integer, Integer, String>, Integer, Tuple2<Integer, String>> {
	DownStreamOnce() {
		super(new DownStreamKeySelector(), new DownStreamValueSelector(), new DownStreamCombiner(),
			new DownStreamSizeSelector(), Tuple2.of(0, ""));
	}
	@Override
	public void udf(Tuple3<Integer, Integer, String> input, Collector<Tuple3<Integer, Integer, String>> out) {
		out.collect(input);
	}
}
