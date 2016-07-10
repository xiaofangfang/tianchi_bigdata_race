package com.alibaba.middleware.race.jstorm;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.aloha.meta.MetaTuple;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.ProducerSingleton;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MetaDataSpout implements IRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1564646544646545L;
	private static Logger LOG = LoggerFactory.getLogger(MetaDataSpout.class);
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

	public MetaDataSpout() {

	}

	public MetaDataSpout(String consumer_PT) {
		this.consumer_PT = consumer_PT;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		ProducerSingleton p = ProducerSingleton.getInstance();
		sendingQueue = p.getQues(consumer_PT);

	}

	@Override
	public void nextTuple() {
		MetaTuple metaTuple = null;
		try {
			metaTuple = sendingQueue.take();
			//RaceUtils.method1_WriteText("----rev tupe--" + metaTuple + ":" + sendingQueue.size());
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
		declarer.declare(new Fields("consumer"));
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
		metaTuple.setConsumer(this.consumer_PT);
		_collector.emit(new Values(metaTuple));

	}

}