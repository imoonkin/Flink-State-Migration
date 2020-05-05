package org.apache.flink.MigrationApi;

public class ClientServerProtocol {
	public static String host = "127.0.0.1";
	public static int portMigration = 8989;
	public static int portController = 8990;
	public static int portEntry = 8988;

	public static String typeOnce = "typeOnce";
	public static String typeSplit="typeSplit";



	public static String downStreamStart = "downStreamStart";
	public static String downStreamSplitStart = "downStreamSplitStart";
	public static String downStreamOnceStart = "downStreamOnceStart";

	public static String downStreamClose = "downStreamClose";
	public static String downStreamMetricStart = "downStreamMetricStart";
	public static String downStreamSplitMigrationStart = "downStreamSplitMigrationStart";
	public static String downStreamOnceMigrationStart = "downStreamOnceMigrationStart";
	public static String downStreamPull = "downStreamPull";
	public static String downStreamPush = "downStreamPush";
	public static String downStreamPushEnd = "downStreamPushEnd";

	public static String upStreamStart = "upStreamStart";
	public static String upStreamFetch = "upStreamFetch";
	public static String upStreamMetricStart = "upStreamMetricStart";

	public static String sourceStart = "sourceStart";
	public static String sourceHotKey = "sourceHotKey";
	public static String sourceAcceptHotKey = "sourceAcceptHotKey";
	public static String sourceRejectHotKey = "sourceRejectHotKey";
	public static String sourceEnd = "sourceEnd";

	public static String tailStart = "tailStart";

	public static int downStreamParallelism = 3;
	public static int chunkNum = 2;


}
