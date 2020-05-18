package org.apache.flink.MigrationApp;

import org.apache.flink.MigrationApi.AbstractDownStreamOnce;
import org.apache.flink.MigrationApi.AbstractDownStreamSplit;
import org.apache.flink.MigrationApi.Combiner;
import org.apache.flink.MigrationApi.SizeCalculator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.io.Serializable;

class DownStreamOnce extends AbstractDownStreamOnce<Tuple4<Long, Integer, Integer, String>,
	Tuple4<Long, Integer, Integer, String>, Integer, Tuple2<Integer, String>> {
	DownStreamOnce() {
		super(new DownStreamKeySelector(),
			new DownStreamValueSelector(),
			new DownStreamValueCombiner(),
			new DownStreamSizeSelector(), Tuple2.of(0, ""));
	}
	@Override
	public void udf(Tuple4<Long, Integer, Integer, String> input,
					Collector<Tuple4<Long, Integer, Integer, String>> out) {
		out.collect(input);
	}
}
class DownStreamSplit extends AbstractDownStreamSplit<Tuple4<Long, Integer, Integer, String>,
	Tuple4<Long, Integer, Integer, String>, Integer, Tuple2<Integer, String>> {
	DownStreamSplit() {
		super(new DownStreamKeySelector(),
			new DownStreamValueSelector(),
			new DownStreamValueCombiner(),
			new DownStreamSizeSelector(), Tuple2.of(0, ""));
	}
	@Override
	public void udf(Tuple4<Long, Integer, Integer, String> input,
					Collector<Tuple4<Long, Integer, Integer, String>> out) {
		out.collect(input);
	}
}



class DownStreamValueCombiner implements Combiner<Tuple2<Integer, String>>, Serializable {
	@Override
	public Tuple2<Integer, String> addOne(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
		return Tuple2.of(t1.f0 + t2.f0, t1.f0 + t2.f1); //+" "+t1.f1
	}
	@Override
	public Tuple2<Integer, String> addAll(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
		return Tuple2.of(t1.f0 + t2.f0, t1.f0 + t2.f1);//+t1.f1
	}
}
class DownStreamKeySelector implements KeySelector<Tuple4<Long, Integer, Integer, String>,
	Integer> {
	@Override
	public Integer getKey(Tuple4<Long, Integer, Integer, String> value){
		return value.f1;
	}
}
class DownStreamValueSelector implements KeySelector<Tuple4<Long, Integer, Integer, String>,
	Tuple2<Integer, String>> {
	@Override
	public Tuple2<Integer, String> getKey(Tuple4<Long, Integer, Integer, String> input){
		return Tuple2.of(input.f2, input.f3);
	}
}
class DownStreamSizeSelector implements SizeCalculator<Integer, Tuple2<Integer, String>>, Serializable {
	@Override
	public int size(Integer key, Tuple2<Integer, String> value) {
		return value.f0;
	}
}


class KeyGen implements FlatMapFunction<String, Tuple4<Long, Integer, Integer, String>> {
	@Override
	public void flatMap(String s, Collector<Tuple4<Long, Integer, Integer, String>> out) throws Exception {
		String[] ss=s.split(" ");
		out.collect(new Tuple4<>(System.currentTimeMillis(),
			Integer.parseInt(ss[0]), 1, ss[1].replaceAll("\n", "")));
	}
}
class KS implements KeySelector<Tuple4<Long, Integer, Integer, String>, Integer> {
	//select key from KeyGen's result
	@Override
	public Integer getKey(Tuple4<Long, Integer, Integer, String> value) throws Exception {
		return value.f1;
	}
}
class Splitter implements FlatMapFunction<Tuple4<Long, Integer, Integer, String>,
	Tuple4<Long, Integer, Integer, String>> {
	@Override
	public void flatMap(Tuple4<Long, Integer, Integer, String> in,
						Collector<Tuple4<Long, Integer, Integer, String>> out) throws Exception {
		out.collect(in);
	}
}
