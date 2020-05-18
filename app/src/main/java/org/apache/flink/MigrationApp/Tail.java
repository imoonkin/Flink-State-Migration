package org.apache.flink.MigrationApp;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class Tail extends RichFlatMapFunction<Tuple4<Long, Integer, Integer, String>, Long> {
	private int para;
	private long maxlatency, cnt;

	Tail(int para, int start) {
		this.para = para;
		maxlatency = 0;
		cnt = start - 1000;
	}

	@Override
	public void flatMap(Tuple4<Long, Integer, Integer, String> value, Collector<Long> out) throws Exception {
		if (cnt > 0) cnt--;
		if (cnt <= 0) {
			long la = System.currentTimeMillis() - value.f0;
			out.collect(la);
//			if (la > maxlatency) {
//				maxlatency=la;
//				out.collect(-1L);
//			}
		}

	}
}


