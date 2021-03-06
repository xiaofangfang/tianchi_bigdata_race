package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

	// 这些是写tair key的前缀
	public static String prex_tmall = "platformTmall_";
	public static String prex_taobao = "platformTaobao_";
	public static String prex_ratio = "ratio_";
	public static String teamcode = "42500hzpdx_";

	// 这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
	public static String JstormTopologyName = "42500hzpdx";
	public static String MetaConsumerGroup = "42500hzpdx";
	public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
	public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TBOrder";
	public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TMOrder";
	public static String TairConfigServer = "10.101.72.127:5198";
	public static String TairSalveConfigServer = "10.101.72.128:5198";

	//public static String TairConfigServer = "192.168.187.128:5198";
	//public static String TairSalveConfigServer = "192.168.187.128:5198";

    public static String TairGroup = "group_tianchi";
	//public static String TairGroup = "group_1";
	public static Integer TairNamespace = 8494;
	public static final String TB_flag = "tb";
	public static final String TM_flag = "tm";
	// 该流处理无线交易与PC每分时刻交易比值
	public static final String PAY_STREAM_ID = "pay";

	// 该流处理没秒TM和TB每秒交易总量
	public static final String ORDERID_STREAM_ID = "orderid";
	public static final long orderId_end = 123456789l;
	public static final String pay_end = "end";
	public static final int div=5000;
	public static final int pushNumber=6;

}
