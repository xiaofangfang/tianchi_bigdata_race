
package com.alibaba.middleware.race.rocketmq;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.model.TestServrizableObj;
import com.alibaba.rocketmq.client.exception.MQClientException;

/**
 * Producer，发送消息
 */
public class Producer {

	private static Random rand = new Random();
	private static int count = 500;

	/**
	 * 这是一个模拟堆积消息的程序，生成的消息模型和我们比赛的消息模型是一样的， 所以选手可以利用这个程序生成数据，做线下的测试。
	 * 
	 * @param args
	 * @throws MQClientException
	 * @throws InterruptedException
	 */
	public static long getLongTime(long crateTime) {
		//
		long time = (crateTime / 1000) * 1000;
		Date d = new Date(time);
		time = time - d.getSeconds() * 1000;
		return time;
	}

	public static void main(String[] args) throws Exception {
		// List<OrderMessage> tm=new ArrayList<OrderMessage>();
		// List<OrderMessage> tb=new ArrayList<OrderMessage>();
		// List<PaymentMessage>pays=new ArrayList<PaymentMessage>();
//		DefaultMQProducer producer = new DefaultMQProducer(RaceConfig.MetaConsumerGroup);
//		producer.setNamesrvAddr("192.168.159.130:9876");
//		producer.start();
//		final String[] topics = new String[] { RaceConfig.MqTaobaoTradeTopic, RaceConfig.MqTmallTradeTopic };
//		final Semaphore semaphore = new Semaphore(0);
		List<OrderMessage> tmlist = new ArrayList<OrderMessage>();
		List<OrderMessage> tblist = new ArrayList<OrderMessage>();
		List<PaymentMessage> pays = new ArrayList<PaymentMessage>();
		for (int i = 0; i < count; i++) {
			try {
                Thread.sleep(500);
				final int platform = rand.nextInt(2);
				final OrderMessage orderMessage = (platform == 0 ? OrderMessage.createTbaoMessage()
						: OrderMessage.createTmallMessage());
				orderMessage.setCreateTime(System.currentTimeMillis());
				if (platform == 0) {
					tblist.add(orderMessage);

				}
				if (platform == 1) {
					tmlist.add(orderMessage);
				}
				// byte[] body = RaceUtils.writeKryoObject(orderMessage);
				// Message msgToBroker = new Message(topics[platform], body);
				// producer.send(msgToBroker, new SendCallback() {
				// public void onSuccess(SendResult sendResult) {
				// semaphore.release();
				// }
				//
				// public void onException(Throwable throwable) {
				// throwable.printStackTrace();
				// }
				// });

				PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
				double amount = 0;
				for (final PaymentMessage paymentMessage : paymentMessages) {
					int retVal = Double.compare(paymentMessage.getPayAmount(), 0);
					if (retVal < 0) {
						throw new RuntimeException("price < 0 !!!!!!!!");
					}

					if (retVal > 0) {
						amount += paymentMessage.getPayAmount();
						pays.add(paymentMessage);
						// final Message messageToBroker = new
						// Message(RaceConfig.MqPayTopic,
						// RaceUtils.writeKryoObject(paymentMessage));
						//
						// producer.send(messageToBroker, new SendCallback() {
						// public void onSuccess(SendResult sendResult) {
						// }
						//
						// public void onException(Throwable throwable) {
						// throwable.printStackTrace();
						// }
						// }
						// );
					} else {
						//
					}
				}

				if (Double.compare(amount, orderMessage.getTotalPrice()) != 0) {
					throw new RuntimeException("totalprice is not equal.");
				}

			} catch (Exception e) {
				e.printStackTrace();
				Thread.sleep(1000);
			}
		}

		//semaphore.acquire(count);

		// 用一个short标识生产者停止生产数据
//		byte[] zero = new byte[] { 0, 0 };
//		Message endMsgTB = new Message(RaceConfig.MqTaobaoTradeTopic, zero);
//		Message endMsgTM = new Message(RaceConfig.MqTmallTradeTopic, zero);
//		Message endMsgPay = new Message(RaceConfig.MqPayTopic, zero);
//
//		try {
//			producer.send(endMsgTB);
//			producer.send(endMsgTM);
//			producer.send(endMsgPay);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		producer.shutdown();
		// 消息发出之后，序列化对象保存

		FileOutputStream fos = new FileOutputStream("./src/obj.txt");
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		TestServrizableObj t = new TestServrizableObj();
		t.setPay(pays);
		t.setTb(tblist);
		t.setTm(tmlist);
		System.out.println("-----pays---"+pays.size());
		System.out.println("-----tblist---"+tblist.size());
		System.out.println("-----tmlist---"+tmlist.size());
		oos.writeObject(t);
		oos.flush();
		oos.close();
		fos.close();

	}
}
