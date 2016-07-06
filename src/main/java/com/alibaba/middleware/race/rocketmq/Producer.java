
package com.alibaba.middleware.race.rocketmq;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * Producer，发送消息
 */
public class Producer {

	private static Random rand = new Random();
	private static int count = 300;

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

	public static void main(String[] args) throws MQClientException, InterruptedException {
		DefaultMQProducer producer = new DefaultMQProducer("yuhaifang_916");

		long tm_lastime = 0;
		long tb_lastime = 0;
		boolean tm_begin = false;
		boolean tb_begin = false;
		int count1 = 0;
		// 在本地搭建好broker后,记得指定nameServer的地址
		producer.setNamesrvAddr("192.168.159.130:9876");
		producer.start();
		final String[] topics = new String[] { RaceConfig.MqTaobaoTradeTopic, RaceConfig.MqTmallTradeTopic };
		final Semaphore semaphore = new Semaphore(0);
		double sum1 = 0;
		double sum0 = 0;
		double tm_sum = 0;
		double tb_sum = 0;
		long countAll = 0;
		boolean count_begain = false;
		List<Long> tb = new ArrayList<Long>();
		List<Long> tm = new ArrayList<Long>();

		for (int i = 0; i < count; i++) {
			try {
				Thread.sleep(1000);
				final int platform = rand.nextInt(2);
				final OrderMessage orderMessage = (platform == 0 ? OrderMessage.createTbaoMessage()
						: OrderMessage.createTmallMessage());
				orderMessage.setCreateTime(System.currentTimeMillis());
				if (platform == 0) {
					tb.add(orderMessage.getOrderId());
				}
				if (platform == 1) {
					tm.add(orderMessage.getOrderId());
				}

				byte[] body = RaceUtils.writeKryoObject(orderMessage);
				Message msgToBroker = new Message(topics[platform], body);
				producer.send(msgToBroker, new SendCallback() {
					public void onSuccess(SendResult sendResult) {
						semaphore.release();
					}

					public void onException(Throwable throwable) {
						throwable.printStackTrace();
					}
				});

				PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
				double amount = 0;
				for (final PaymentMessage paymentMessage : paymentMessages) {
					int retVal = Double.compare(paymentMessage.getPayAmount(), 0);
					// long time = paymentMessage.getCreateTime();

					if (!count_begain) {
						count_begain = true;
						countAll = getLongTime(paymentMessage.getCreateTime());
					}
					if (getLongTime(paymentMessage.getCreateTime()) > countAll) {
						System.out.println(paymentMessage.getCreateTime()+"_"+RaceUtils.formatDate(getLongTime(paymentMessage.getCreateTime()))+"sum0=="+sum0+"----sum1="+sum1);
						countAll = getLongTime(paymentMessage.getCreateTime());
					}
					if (tb.contains(paymentMessage.getOrderId())) {
						long _tmptime = getLongTime(paymentMessage.getCreateTime());
						if (!tb_begin) {
							tb_lastime = _tmptime;
							tb_begin = true;
						}
						if (_tmptime > tb_lastime) {
							//System.out.println("tb--" + tb);
							System.out.println(tb_lastime+"_"+RaceUtils.formatDate(tb_lastime) + "-----tb----" + tb_sum);
							tb_sum = 0;
							tb_lastime = _tmptime;
							tb.remove(paymentMessage.getOrderId());
						}
						tb_sum = tb_sum + paymentMessage.getPayAmount();
					}

					if (tm.contains(paymentMessage.getOrderId())) {
						long _tmptime = getLongTime(paymentMessage.getCreateTime());
						if (!tm_begin) {
							tm_lastime = _tmptime;
							tm_begin = true;
						}
						if (_tmptime > tm_lastime) {
							//System.out.println("tm--" + tm);
							System.out.println(tm_lastime+"_"+RaceUtils.formatDate(tm_lastime) + "-----tm-----" + tm_sum);
							tm_sum = 0;
							tm_lastime = _tmptime;
							tm.remove(paymentMessage.getOrderId());
							// tm_sum = tm_sum + paymentMessage.getPayAmount();
						}
						tm_sum = tm_sum + paymentMessage.getPayAmount();
					}

					//
					if (retVal < 0) {
						throw new RuntimeException("price < 0 !!!!!!!!");
					}

					if (retVal > 0) {
						amount += paymentMessage.getPayAmount();
						if (paymentMessage.getPayPlatform() == 1) {
							sum1 = sum1 + paymentMessage.getPayAmount();
						}
						if (paymentMessage.getPayPlatform() == 0) {
							sum0 = sum0 + paymentMessage.getPayAmount();
						}

						count1++;
						// System.out.println("----"+paymentMessage);
						final Message messageToBroker = new Message(RaceConfig.MqPayTopic,
								RaceUtils.writeKryoObject(paymentMessage));

						producer.send(messageToBroker, new SendCallback() {
							public void onSuccess(SendResult sendResult) {

								// System.out.println(paymentMessage);
							}

							public void onException(Throwable throwable) {
								throwable.printStackTrace();
							}
						});
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

		semaphore.acquire(count);
		// System.out.println("tm--"+tm);
		// System.out.println("tb----"+tb);
		// System.out.println(lastime+"----tm--tb====="+tm_sum+"---"+tb_sum);
		// System.out.println("sum====="+sum0+"---"+sum1);
		// System.out.println("count===" + count1);

		// 用一个short标识生产者停止生产数据
		byte[] zero = new byte[] { 0, 0 };
		Message endMsgTB = new Message(RaceConfig.MqTaobaoTradeTopic, zero);
		Message endMsgTM = new Message(RaceConfig.MqTmallTradeTopic, zero);
		Message endMsgPay = new Message(RaceConfig.MqPayTopic, zero);

		try {
			producer.send(endMsgTB);
			producer.send(endMsgTM);
			producer.send(endMsgPay);
		} catch (Exception e) {
			e.printStackTrace();
		}
		producer.shutdown();
	}
}
