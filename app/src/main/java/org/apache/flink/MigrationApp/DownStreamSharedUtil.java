package org.apache.flink.MigrationApp;

import org.apache.flink.MigrationApi.Combiner;
import org.apache.flink.MigrationApi.SizeCalculator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.io.Serializable;



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
class DownStreamSizeSelector implements SizeCalculator<Integer, Tuple2<Integer, String>>, Serializable {
	@Override
	public int size(Integer key, Tuple2<Integer, String> value) {
		return key*value.f0;
	}
}

class DownStreamKeySelector implements KeySelector<Tuple3<Integer, Integer, String>, Integer> {
	@Override
	public Integer getKey(Tuple3<Integer, Integer, String> value){
		return value.f0;
	}
}
class DownStreamValueSelector implements KeySelector<Tuple3<Integer, Integer, String>, Tuple2<Integer, String>> {
	@Override
	public Tuple2<Integer, String> getKey(Tuple3<Integer, Integer, String> value){
		return Tuple2.of(value.f1, value.f2);
	}
}
class KeyGen implements FlatMapFunction<String, Tuple2<Integer, String>> {
	@Override
	public void flatMap(String s, Collector<Tuple2<Integer, String>> out) throws Exception {
		for (String word: s.split(" ")) {
			out.collect(new Tuple2<>(word.length(), word));
		}
	}
}
class KS implements KeySelector<Tuple2<Integer, String>, Integer> {
	@Override
	public Integer getKey(Tuple2<Integer, String> value) throws Exception {
		return value.f0;
	}
}

class Splitter implements FlatMapFunction<Tuple2<Integer, String>, Tuple3<Integer, Integer, String>> {
	@Override
	public void flatMap(Tuple2<Integer, String> in, Collector<Tuple3<Integer, Integer, String>> out) throws Exception {
		out.collect(new Tuple3<Integer, Integer, String>(in.f0, 1, in.f1.toUpperCase()));
	}
}
