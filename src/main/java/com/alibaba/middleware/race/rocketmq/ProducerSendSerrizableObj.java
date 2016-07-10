
package com.alibaba.middleware.race.rocketmq;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.model.TestServrizableObj;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import edu.emory.mathcs.backport.java.util.Collections;

/**
 * Producer，发送消息
 */
public class ProducerSendSerrizableObj {

	private static Random rand = new Random();
	private static int count = 5000;

	/**
	 * 这是一个模拟堆积消息的程序，生成的消息模型和我们比赛的消息模型是一样的， 所以选手可以利用这个程序生成数据，做线下的测试。
	 * 
	 * @param args
	 * @throws Exception
	 * @throws MQClientException
	 * @throws InterruptedException
	 */
	private List<OrderMessage> tmlists;
	private List<OrderMessage> tblists;
	private List<PaymentMessage> pyalists;
	byte[] zero = new byte[] { 0, 0 };
	Message endMsgTB = new Message(RaceConfig.MqTaobaoTradeTopic, zero);
	Message endMsgTM = new Message(RaceConfig.MqTmallTradeTopic, zero);
	Message endMsgPay = new Message(RaceConfig.MqPayTopic, zero);
	DefaultMQProducer producer;
	private static int countt = 0;

	public void setProducer(DefaultMQProducer producer) {
		this.producer = producer;
	}

	private Map<Long, Double> pt1 = new HashMap<Long, Double>();
	private Map<Long, Double> pt0 = new HashMap<Long, Double>();

	private Map<Long, Double> tm = new HashMap<Long, Double>();
	private Map<Long, Double> tb = new HashMap<Long, Double>();
	private long getChouTime(String str) {
		long time = Long.parseLong(str);
		long createtime = (time / 1000) * 1000;
		Date d = new Date(createtime);
		time = createtime - d.getSeconds() * 1000;
		time = time / 1000;
		return time;
	}
	private void outPutMap(Map<Long, Double> map,String pt)
	{
		List<Long> times=new ArrayList<Long>(map.keySet());
		Collections.sort(times);
		for(int i=0;i<times.size();i++)
		{
			long time=times.get(i);
			double price=map.get(time);
			price = (double) Math.round(price * 100) / 100.0;
			System.out.println("-----"+pt+"---"+time+"---"+price);
		}
	}
	private void aclValue(final Map<Long, Double> pmap1, final Map<Long, Double> pmap0) {
		// String key = RaceConfig.prex_ratio + RaceConfig.teamcode;

		List<Long> tmpList = new ArrayList<Long>(pmap1.keySet());
		Collections.sort(tmpList);
		double p1 = 0;
		double p0 = 0;
		int length0 = pmap0.keySet().size();
		int lenght1 = pmap1.keySet().size();
		for (int i = 0; i < tmpList.size() && i < length0 && i < lenght1; i++) {
			long time = tmpList.get(i);
			p1 = p1 + pmap1.get(time);
			p0 = p0 + pmap0.get(time);
			double value = p1 / p0;
			double plast = (double) (Math.round(value * 100) / 100.0);
			System.out.println("--p1/p0----"+time+"----"+plast);
			// RaceUtils.method1_WriteText("----write--data---" + time + ":" +
			// plast);
		}

	}
	//public void add()
	public void readObjcetc() throws Exception {
		String filename = "./src/obj.txt";
		FileInputStream fs = new FileInputStream(filename);
		ObjectInputStream ois = new ObjectInputStream(fs);
		TestServrizableObj test = (TestServrizableObj) ois.readObject();
		ois.close();
		fs.close();
		tmlists = test.getTm();
		tblists = test.getTb();
		pyalists = test.getPay();
//        List<Long> tmid=new ArrayList<Long>();
//        List<Long> tbid=new ArrayList<Long>();
//		for(OrderMessage o:tmlists)
//		{
//			tmid.add(o.getOrderId());
//		}
//		for(OrderMessage o:tblists)
//		{
//			tbid.add(o.getOrderId());
//		}
//		for (PaymentMessage pay : pyalists) {
//			if(tmid.contains(pay.getOrderId()))
//			{
//				long time = pay.getCreateTime();
//				time = getChouTime(time + "");
//				Double price=tm.get(time);
//				if(price==null)
//					tm.put(time, pay.getPayAmount());
//				else
//					tm.put(time,price+pay.getPayAmount());
//			}
//			if(tbid.contains(pay.getOrderId()))
//			{
//				long time = pay.getCreateTime();
//				time = getChouTime(time + "");
//				Double price=tb.get(time);
//				if(price==null)
//					tb.put(time, pay.getPayAmount());
//				else
//					tb.put(time,price+pay.getPayAmount());
//			}
//			
//			if (pay.getPayPlatform() == 1) {
//				long time = pay.getCreateTime();
//				time = getChouTime(time + "");
//				Double price=pt1.get(time);
//				if(price==null)
//					pt1.put(time, pay.getPayAmount());
//				else
//					pt1.put(time,price+pay.getPayAmount());
//			}
//			if (pay.getPayPlatform() == 0) {
//				long time = pay.getCreateTime();
//				time = getChouTime(time + "");
//				Double price=pt0.get(time);
//				if(price==null)
//					pt0.put(time, pay.getPayAmount());
//				else
//					pt0.put(time,price+pay.getPayAmount());
//			}
//		}
		System.out.println("------tmlist size=" + tmlists.size());
		System.out.println("------tblists size=" + tblists.size());
		System.out.println("------pyalists size=" + pyalists.size());
//		outPutMap(tm,"tm");
//		outPutMap(tb,"tb");
//		aclValue(pt1,pt0);
		
	}
	

	public void sendTmList() {
		for (int i = 0; i < tmlists.size(); i++) {
			final Message messageToBroker = new Message(RaceConfig.MqTmallTradeTopic,
					RaceUtils.writeKryoObject(tmlists.get(i)));
			sendMessage(messageToBroker);
		}
		sendMessage(endMsgTM);
	}

	public void sendTbList() {
		for (int i = 0; i < tblists.size(); i++) {
			final Message messageToBroker = new Message(RaceConfig.MqTaobaoTradeTopic,
					RaceUtils.writeKryoObject(tblists.get(i)));
			sendMessage(messageToBroker);
		}
		sendMessage(endMsgTB);
	}

	public void sendPayList() {
		for (int i = 0; i < pyalists.size(); i++) {
			final Message messageToBroker = new Message(RaceConfig.MqPayTopic,
					RaceUtils.writeKryoObject(pyalists.get(i)));
			sendMessage(messageToBroker);
		}
		sendMessage(endMsgPay);
		// System.out.println("---"+this.countt);
	}

	private void sendMessage(final Message mess) {
		try {
			producer.send(mess, new SendCallback() {
				public void onSuccess(SendResult sendResult) {
					// System.out.println("---mess----"+mess);
					// countt++;
				}

				public void onException(Throwable throwable) {
					throwable.printStackTrace();
				}
			});
		} catch (MQClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemotingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static long getLongTime(long crateTime) {
		//
		long time = (crateTime / 1000) * 1000;
		Date d = new Date(time);
		time = time - d.getSeconds() * 1000;
		return time;
	}

	public static void main(String[] args) throws Exception {
	//	new ProducerSendSerrizableObj().readObjcetc();
		DefaultMQProducer producer = new DefaultMQProducer(RaceConfig.MetaConsumerGroup);
		producer.setNamesrvAddr("192.168.187.128:9876");
		producer.start();
		ProducerSendSerrizableObj pro = new ProducerSendSerrizableObj();
		pro.readObjcetc();
		pro.setProducer(producer);
		pro.sendTmList();
		pro.sendTbList();
		pro.sendPayList();
		producer.shutdown();

	}
}
