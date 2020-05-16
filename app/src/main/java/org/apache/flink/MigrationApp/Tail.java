package org.apache.flink.MigrationApp;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

//deprecated
public class Tail extends RichFlatMapFunction<Tuple4<Long, Integer, Integer, String>, Long> {
	//private transient Histogram histogram;
	private int para;
	private long maxlatency, cnt;
	Tail(int para, int start) {
		this.para=para;
		maxlatency=0;
		cnt= start*9 /10;
	}
	@Override
	public void flatMap(Tuple4<Long, Integer, Integer, String> value, Collector<Long> out) throws Exception {
		if (cnt > 0) cnt--;
		if (cnt<=0) {
			long la=System.currentTimeMillis() - value.f0;
			out.collect(la);
			if (la > maxlatency) {
				maxlatency=la;
				out.collect(-1L);
			}
		}
		//this.histogram.update(System.currentTimeMillis()-value.f0);

	}
	@Override
	public void open(Configuration config) {
		//this.histogram = getRuntimeContext().getMetricGroup().histogram("my Histogram", new DescriptiveStatisticsHistogram(100));
	}
}


