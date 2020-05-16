package org.apache.flink.MigrationApp;

import org.apache.flink.MigrationApi.ClientServerProtocol;
import org.apache.flink.MigrationApi.SkewnessDetector;
import org.apache.flink.MigrationApi.UpStreamPF;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;
@Deprecated
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
		//env.getConfig().setLatencyTrackingInterval(100);


		env.enableCheckpointing(5000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		env.setBufferTimeout(1);


		DataStream<Long> dataStream = env
			.socketTextStream(ClientServerProtocol.host, ClientServerProtocol.portData).setParallelism(1)
			.flatMap(new KeyGen()).setParallelism(1).startNewChain()
			.flatMap(new SkewnessDetector<>(
				new KS(), 0.1f, Integer.parseInt(args[5]))).setParallelism(1)
			.flatMap(new Splitter()).setParallelism(3)
			.partitionCustom(new UpStreamPF<Integer>(), 1)
			.flatMap(new DownStreamOnce()).setParallelism(Integer.parseInt(args[3]))
			.flatMap(new Tail(Integer.parseInt(args[3]), Integer.parseInt(args[5]))).setParallelism(1);

		StringBuilder s= new StringBuilder(args[6]);
		for (int i=0; i<args.length-2; i++) s.append(args[i]).append("-");
		s.append("bid.txt");
		System.out.println("output file: "+s);
		dataStream.writeAsText(s.toString()).setParallelism(1);


		env.execute("Flink Streaming Java API Skeleton");
		while (true) {

		}
	}

}
