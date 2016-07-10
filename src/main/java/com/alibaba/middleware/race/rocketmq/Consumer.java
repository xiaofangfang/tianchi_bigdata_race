package com.alibaba.middleware.race.rocketmq;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * Consumer，订阅消息
 */

/**
 * RocketMq消费组信息我们都会再正式提交代码前告知选手
 */
public class Consumer implements MessageListenerConcurrently{

	private DefaultMQPushConsumer consumer;

	private static List<PaymentMessage> pays = new ArrayList<PaymentMessage>();

	public static List<PaymentMessage> getPays() {
		return pays;
	}

	/*** 只初始化一次 **/
	public void initConsumer() throws MQClientException {
		System.out.println("begin to consumer");
		consumer = new DefaultMQPushConsumer("yuhaifang_916");
		consumer.setNamesrvAddr("192.168.187.128:9876");
		consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		consumer.registerMessageListener(new MessageListenerConcurrently() {
			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				// 得到多个消息之后，交个多个bolt序列化并且计算金额，以为是多线程的，可以通知
				// List<PaymentMessage> lists=new ArrayList<PaymentMessage>();
				for (MessageExt msg : msgs) {
					byte[] body = msg.getBody();
					if (body.length == 2 && body[0] == 0 && body[1] == 0) {
						// Info: 生产者停止生成数据, 并不意味着马上结束
						System.out.println("Got the end signal");
						continue;
					}
					OrderMessage paymentMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
					// 拿到序列化中的对象之后，交给jstorm的spout
					 System.out.println(paymentMessage);
					//pays.add(paymentMessage);

				}
				// RaceSentenceSpout.setPayMents(lists);
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

		consumer.start();
	}

	public static void main(String[] args) throws InterruptedException, MQClientException {
		new Consumer().initConsumer();
	}

	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
		
		return null;
	}
}
