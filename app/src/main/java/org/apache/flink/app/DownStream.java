package org.apache.flink.app;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class DownStream extends AbstractDownStream<Tuple3<Integer, Integer, String>,
	Tuple3<Integer, Integer, String>, Integer, Tuple2<Integer, String>> {
	DownStream() {
		super(new DownStreamKeySelector(), new DownStreamValueSelector(), new DownStreamCombiner(), Tuple2.of(0, ""));
	}
	@Override
	public void udf(Tuple3<Integer, Integer, String> input, Collector<Tuple3<Integer, Integer, String>> out) {
		out.collect(input);
	}
}

class DownStreamCombiner implements Combiner<Tuple2<Integer, String>>, Serializable {
	@Override
	public Tuple2<Integer, String> addOne(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
		return Tuple2.of(t1.f0+t2.f0, t2.f1+" "+t1.f1);
	}
	@Override
	public Tuple2<Integer, String> addAll(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
		return Tuple2.of(t1.f0+t2.f0, t2.f1+t1.f1);
	}
}
class DownStreamKeySelector implements KeySelector<Tuple3<Integer, Integer, String>, Integer>{
	@Override
	public Integer getKey(Tuple3<Integer, Integer, String> value){
		return value.f0;
	}
}
class DownStreamValueSelector implements KeySelector<Tuple3<Integer, Integer, String>, Tuple2<Integer, String>>{
	@Override
	public Tuple2<Integer, String> getKey(Tuple3<Integer, Integer, String> value){
		return Tuple2.of(value.f1, value.f2);
	}
}

