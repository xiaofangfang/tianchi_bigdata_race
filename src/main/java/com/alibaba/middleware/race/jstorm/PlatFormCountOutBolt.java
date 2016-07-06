package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.aloha.meta.MetaTuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.taobao.tair.impl.DefaultTairManager;

public class PlatFormCountOutBolt implements IRichBolt {
	OutputCollector collector;
	Map<String, Double> counts = new HashMap<String, Double>();

	long _beginStmap;
	long _endStamp;
	int countTimes = 0;
	String _timeStampValue;

	static Map<String, Map<Long, Double>> paysMap = new HashMap<String, Map<Long, Double>>();
	private transient TairOperatorImpl tairOperator;

	@Override
	public void execute(Tuple tuple) {

		String str = tuple.getString(0);
		System.out.println("--------------str====================="+str);
		//String data[] = str.split(":");
		
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields("orderid"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	private boolean writeData(String _timeStamp, Double sum, String platFrom) {
		if (platFrom.contains(RaceConfig.MqTmallTradeTopic)) {
			String key = "platformTmall_teamcode_" + _timeStamp;
			return tairOperator.write(key, sum);
		}

		if (platFrom.contains(RaceConfig.MqTaobaoTradeTopic)) {
			String key = "platformTaobao_teamcode_" + _timeStamp;
			return tairOperator.write(key, sum);
		}
		return false;
	}
}