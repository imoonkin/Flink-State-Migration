package org.apache.flink.MigrationApp;

import org.apache.flink.MigrationApi.ClientServerProtocol;
import org.apache.flink.MigrationApi.SkewnessDetector;
import org.apache.flink.MigrationApi.UpStreamPF;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AppOnce {
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		Configuration conf = new Configuration();
		//conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

		/*
		first one for local test;
		second one for cluster;
		 */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		//final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		env.enableCheckpointing(5000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

		DataStream<Tuple3<Integer, Integer, String>> dataStream = env
			.socketTextStream("localhost", 9999).setParallelism(1)
			.flatMap(new KeyGen()).setParallelism(1).startNewChain()
			.flatMap(new SkewnessDetector<Tuple2<Integer, String>, Integer>(new KS())).setParallelism(1)
			.flatMap(new Splitter()).setParallelism(3)
			.partitionCustom(new UpStreamPF<Integer>(), 0)
			.flatMap(new DownStreamOnce()).setParallelism(ClientServerProtocol.downStreamParallelism)
			//.flatMap(new Tail<>()).setParallelism(1)
			.keyBy(0)
			.timeWindow(Time.seconds(5))
			.sum(1).setParallelism(1);

		env.execute("Flink Streaming Java API Skeleton");
	}

}
