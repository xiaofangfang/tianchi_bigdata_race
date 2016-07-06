package com.alibaba.middleware.race.rocketmq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.collections.bag.SynchronizedBag;

import com.alibaba.aloha.meta.MetaTuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

public class ProducerSingleton {
	private static LinkedBlockingDeque<MetaTuple> sendingQueue_tb = new LinkedBlockingDeque<MetaTuple>();
	private static LinkedBlockingDeque<MetaTuple> sendingQueue_tm = new LinkedBlockingDeque<MetaTuple>();
	private static LinkedBlockingDeque<MetaTuple> sendingQueue_pay = new LinkedBlockingDeque<MetaTuple>();

	private static ProducerSingleton single = null;

	public static void main(String[] args) {
		ProducerSingleton p = new ProducerSingleton();
		p = new ProducerSingleton();
		p = new ProducerSingleton();
	}

	private ProducerSingleton() {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
		consumer.setNamesrvAddr("192.168.159.130:9876");
		try {
			consumer.subscribe(RaceConfig.MqPayTopic, "*");
			consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
		} catch (Exception e) {
			e.printStackTrace();
		}
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
		consumer.registerMessageListener(new MessageListenerConcurrently() {
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				List<MessageExt> tb = new ArrayList<MessageExt>();
				List<MessageExt> tm = new ArrayList<MessageExt>();
				List<MessageExt> pay = new ArrayList<MessageExt>();
				for (MessageExt message : msgs) {
					if (message.getTopic().equals(RaceConfig.MqPayTopic)) {
						pay.add(message);
					}
					if (message.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)) {
						tb.add(message);
					}
					if (message.getTopic().equals(RaceConfig.MqTmallTradeTopic)) {
						tm.add(message);
					}
				}
				MetaTuple metaTuple = null;

				if (pay.size() > 0) {
					metaTuple = new MetaTuple(pay, context.getMessageQueue());
					sendingQueue_pay.offer(metaTuple);
				}
				if (tb.size() > 0) {
					metaTuple = new MetaTuple(tb, context.getMessageQueue());
					sendingQueue_tb.offer(metaTuple);
				}
				if (tm.size() > 0) {
					metaTuple = new MetaTuple(tm, context.getMessageQueue());
					sendingQueue_tm.offer(metaTuple);
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});
		try {
			consumer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
		}
	}

	public LinkedBlockingDeque<MetaTuple> getQues(String topci) {
		if (RaceConfig.MqTmallTradeTopic.equals(topci)) {
			return sendingQueue_tm;
		}
		if (RaceConfig.MqTaobaoTradeTopic.equals(topci)) {
			return sendingQueue_tb;
		}
		if (RaceConfig.MqPayTopic.equals(topci)) {
			return sendingQueue_pay;

		}
		return null;
	}

	// 静态工厂方法
	public static ProducerSingleton getInstance() {
		if (single == null) {
			synchronized (ProducerSingleton.class) {
				if (single == null)
					single = new ProducerSingleton();
			}
		}
		return single;

	}
}
