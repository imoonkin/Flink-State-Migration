package org.apache.flink.app;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class Tail extends RichFlatMapFunction {
	@Override
	public void flatMap(Object value, Collector out) throws Exception {

	}
}
