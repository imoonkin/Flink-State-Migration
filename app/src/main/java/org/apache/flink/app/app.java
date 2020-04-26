/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.app;

	import org.apache.flink.api.common.functions.FlatMapFunction;
	import org.apache.flink.api.java.functions.KeySelector;
	import org.apache.flink.api.java.tuple.Tuple3;
	import org.apache.flink.configuration.Configuration;
	import org.apache.flink.streaming.api.CheckpointingMode;
	import org.apache.flink.streaming.api.windowing.time.Time;
	import org.apache.flink.api.java.tuple.Tuple2;
	import org.apache.flink.streaming.api.datastream.DataStream;
	import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
	import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class app {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		Configuration conf=new Configuration();
		//conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		//StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */


		DataStream<Tuple3<Integer, Integer, String>> dataStream = env
			.socketTextStream("localhost", 9999).setParallelism(1)
			.flatMap(new KeyGen()).setParallelism(1).startNewChain()
			.flatMap(new SkewnessDetector<Tuple2<Integer, String>, Integer>(new KS())).setParallelism(1)
			.flatMap(new Splitter()).setParallelism(3)
			.partitionCustom(new UpStreamPF<Integer>(), 0)
			.flatMap(new DownStreamUDF()).setParallelism(ClientServerProtocol.downStreamParallelism)
			//.flatMap(new Tail<>()).setParallelism(1)
			.keyBy(0)
			.timeWindow(Time.seconds(5))
			.sum(1).setParallelism(1);

		//dataStream.print();
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

}
class KeyGen implements FlatMapFunction<String, Tuple2<Integer, String>> {
	@Override
	public void flatMap(String s, Collector<Tuple2<Integer, String>> out) throws Exception {
		for (String word: s.split(" ")) {
			out.collect(new Tuple2<Integer, String>(word.length(), s));
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
