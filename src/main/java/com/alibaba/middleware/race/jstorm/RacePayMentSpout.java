package com.alibaba.middleware.race.jstorm;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.aloha.meta.MetaTuple;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.Consumer;
import com.alibaba.middleware.race.rocketmq.ProducerSingleton;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RacePayMentSpout implements IRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1564646544646545L;
	private static Logger LOG = LoggerFactory.getLogger(RacePayMentSpout.class);
	SpoutOutputCollector _collector;
	Random _rand;
	long sendingCount;
	long startTime;
	boolean isStatEnable;
	int sendNumPerNexttuple;
	static List<PaymentMessage> pays;

	int workNumber = 4;
	static boolean isSendAllPays = false;

	protected transient DefaultMQPushConsumer consumer;
	protected transient LinkedBlockingDeque<MetaTuple> sendingQueue;
	protected boolean flowControl = false;
	protected boolean autoAck = true;
	private String consumer_PT;

	public RacePayMentSpout() {

	}

	public RacePayMentSpout(String consumer_PT) {
		this.consumer_PT = consumer_PT;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		//sendingQueue = ProducerSingleton.getInstance(consumer_PT);
		ProducerSingleton p=ProducerSingleton.getInstance();
		sendingQueue=p.getQues(consumer_PT);

	}

	@Override
	public void nextTuple() {
		MetaTuple metaTuple = null;
		try {
			metaTuple = sendingQueue.take();
		} catch (InterruptedException e) {
		}
		if (metaTuple == null) {
			return;
		}
		sendTuple(metaTuple);

	}

	@Override
	public void ack(Object id) {

		// Ignored
	}

	@Override
	public void fail(Object id) {
		LOG.error("------------ack fail,msgI-------------" + id);
		_collector.emit(new Values(id), id);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("payment"));
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void sendTuple(MetaTuple metaTuple) {
		metaTuple.updateEmitMs();
		metaTuple.setConsumer(this.consumer_PT);
		_collector.emit(new Values(metaTuple));

	}

}