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
import com.alibaba.rocketmq.common.message.MessageExt;

public class OrderIdBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 154654564654654L;
	OutputCollector collector;
	Map<String, Double> counts = new HashMap<String, Double>();

	long _beginStmap;
	long _endStamp;
	int countTimes = 0;
	String _timeStampValue;

	private transient TairOperatorImpl tairOperator;

	@Override
	public void execute(Tuple tuple) {
		// String[] data = tuple.getString(0).split(":");
		MetaTuple metas = (MetaTuple) tuple.getValue(0);
		String consumer_TP = metas.getConsumer();

		for (MessageExt msg : metas.getMsgList()) {
			byte[] body = msg.getBody();
			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				continue;
			}
			OrderMessage paymentMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
			if (consumer_TP.equals(RaceConfig.MqTmallTradeTopic)) {
				collector.emit(tuple,new Values("tm", paymentMessage.getOrderId()));
			}
			if (consumer_TP.equals(RaceConfig.MqTaobaoTradeTopic)) {
				collector.emit(tuple,new Values("tb", paymentMessage.getOrderId()));
			}

		}
		collector.ack(tuple);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","orderid"));
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

}