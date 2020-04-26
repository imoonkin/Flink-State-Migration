package org.apache.flink.app;

public class ClientServerProtocol {
	public static String host="127.0.0.1";
	public static int portMigration =8989;
	public static int portController =8990;

	public static String downStreamPull="downStreamPull";
	public static String downStreamPush="downStreamPush";
	public static String downStreamStart="downStreamStart";
	public static String downStreamClose="downStreamClose";
	public static String downStreamMetricStart="downStreamMetricStart";
	public static String downStreamMigrationStart="downStreamMigrationStart";

	public static String upStreamStart="upStreamStart";
	public static String upStreamFetch="upStreamFetch";
	public static String upStreamMetricStart="upStreamMetricStart";

	public static String sourceStart="sourceStart";
	public static String sourceEnd="sourceEnd";

	public static String tailStart = "tailStart";

	public static int downStreamParallelism=2;


}
