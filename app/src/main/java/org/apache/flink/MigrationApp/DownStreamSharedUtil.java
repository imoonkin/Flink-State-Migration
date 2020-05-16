package org.apache.flink.MigrationApp;

import org.apache.flink.MigrationApi.AbstractDownStreamOnce;
import org.apache.flink.MigrationApi.AbstractDownStreamSplit;
import org.apache.flink.MigrationApi.Combiner;
import org.apache.flink.MigrationApi.SizeCalculator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.io.Serializable;

class DownStreamOnce extends AbstractDownStreamOnce<Tuple5<Long, Integer, Integer, Integer, Integer>,
	Tuple5<Long, Integer, Integer, Integer, Integer>, Integer, Integer> {
	DownStreamOnce() {
		super(new DownStreamItemHighestPriceKeySelector(),
			new DownStreamItemHighestPriceValueSelector(),
			new DownStreamItemHighestPriceCombiner(),
			new DownStreamItemHighestPriceSizeSelector(), 0);
	}
	@Override
	public void udf(Tuple5<Long, Integer, Integer, Integer, Integer> input,
					Collector<Tuple5<Long, Integer, Integer, Integer, Integer>> out) {
		out.collect(input);
	}
}
class DownStreamSplit extends AbstractDownStreamSplit<Tuple5<Long, Integer, Integer, Integer, Integer>,
	Tuple5<Long, Integer, Integer, Integer, Integer>, Integer, Integer> {
	DownStreamSplit() {
		super(new DownStreamItemHighestPriceKeySelector(),
			new DownStreamItemHighestPriceValueSelector(),
			new DownStreamItemHighestPriceCombiner(),
			new DownStreamItemHighestPriceSizeSelector(), 0);
	}
	@Override
	public void udf(Tuple5<Long, Integer, Integer, Integer, Integer> input,
					Collector<Tuple5<Long, Integer, Integer, Integer, Integer>> out) {
		out.collect(input);
	}
}



class DownStreamItemHighestPriceCombiner implements Combiner<Integer>, Serializable {
	@Override
	public Integer addOne(Integer t1, Integer t2) {
		return t1.compareTo(t2)>0? t1 : t2; //+" "+t1.f1
	}
	@Override
	public Integer addAll(Integer t1, Integer t2) {
		return t1.compareTo(t2)>0? t1 : t2;//+t1.f1
	}
}
class DownStreamItemHighestPriceKeySelector implements KeySelector<Tuple5<Long, Integer, Integer, Integer, Integer>,
	Integer> {
	@Override
	public Integer getKey(Tuple5<Long, Integer, Integer, Integer, Integer> value){
		return value.f3;
	}
}
class DownStreamItemHighestPriceValueSelector implements KeySelector<Tuple5<Long, Integer, Integer, Integer, Integer>,
	Integer> {
	@Override
	public Integer getKey(Tuple5<Long, Integer, Integer, Integer, Integer> value){
		return value.f4;
	}
}
class DownStreamItemHighestPriceSizeSelector implements SizeCalculator<Integer, Integer>, Serializable {
	@Override
	public int size(Integer key, Integer value) {
		return 1;
	}
}


class KeyGen implements FlatMapFunction<String, Tuple5<Long, Integer, Integer, Integer, Integer>> {
	@Override
	public void flatMap(String s, Collector<Tuple5<Long, Integer, Integer, Integer, Integer>> out) throws Exception {
		String[] ss=s.split(" ");
		out.collect(new Tuple5<>(System.currentTimeMillis(), Integer.parseInt(ss[0]), Integer.parseInt(ss[1]),
			Integer.parseInt(ss[2]), Integer.parseInt(ss[3])));
	}
}
class KS implements KeySelector<Tuple5<Long, Integer, Integer, Integer, Integer>, Integer> {
	//select key from KeyGen's result
	@Override
	public Integer getKey(Tuple5<Long, Integer, Integer, Integer, Integer> value) throws Exception {
		return value.f3;
	}
}
class Splitter implements FlatMapFunction<Tuple5<Long, Integer, Integer, Integer, Integer>,
	Tuple5<Long, Integer, Integer, Integer, Integer>> {
	@Override
	public void flatMap(Tuple5<Long, Integer, Integer, Integer, Integer> in,
						Collector<Tuple5<Long, Integer, Integer, Integer, Integer>> out) throws Exception {
		out.collect(in);
	}
}
