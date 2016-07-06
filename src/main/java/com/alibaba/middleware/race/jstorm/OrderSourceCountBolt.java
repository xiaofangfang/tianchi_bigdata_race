package com.alibaba.middleware.race.jstorm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.model.PlatFromCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class OrderSourceCountBolt implements IRichBolt {
	OutputCollector collector;
	Map<String, Double> counts = new HashMap<String, Double>();
	private static Logger LOG = LoggerFactory.getLogger(OrderSourceCountBolt.class);

	long _beginStmap;
	long _endStamp;
	int countTimes = 0;
	String _timeStampValue;

	// static Map<String, Map<Long, Double>> paysMap = new HashMap<String,
	// Map<Long, Double>>();
	/**
	 * string tm,tb long orderid
	 */

	final static Map<String, List<Long>> orderIds = new HashMap<String, List<Long>>();
	/**
	 * long timestamp string tm,tb
	 */

	/**
	 * 未处理的payment
	 */
	List<PaymentMessage> _tmpPayList = new ArrayList<PaymentMessage>();
	long lastTime;
	boolean isInitTime = false;
	final long oneMinute = 60000;

	static int count = 0;

	public void prcoessPay(PlatFromCount p, PaymentMessage pay, long timeStamp) {
		if (p.getLastStmap() == 0) {
			p.setTimeStamp(timeStamp);
			p.setLastStmap(timeStamp - oneMinute);
		}
		/*** 判断当前的时间是不是最后统计时间之前的3分钟,并且这个时刻的payment全部处理完毕 **/
		long timeSign = p.getLastStmap() + oneMinute;
		if (timeStamp > timeSign) {
			p.setLastStmap(p.getTimeStamp());
			p.setTimeStamp(timeStamp);
			p.setTotalprice(0);
		}
		p.setTotalprice(p.getTotalprice() + pay.getPayAmount());
	}

	public synchronized void doPayment(PaymentMessage p, List<Long> orders_tm, List<Long> orders_tb,
			List<PaymentMessage> noPrcelist, Tuple tuple) {
		long id = p.getOrderId();
		boolean isProcesId = false;
		if (orders_tm.contains(id)) {
			p.setOrderSource(RaceConfig.TM_flag);
			isProcesId = true;
			collector.emit(tuple, new Values(p));

		}
		if (orders_tb.contains(id)) {

			p.setOrderSource(RaceConfig.TB_flag);
			isProcesId = true;
			collector.emit(tuple, new Values(p));

		}
		if (!isProcesId) {
			noPrcelist.add(p);
		}

	}

	public static <T> List<T> deepCopy(List<T> src) throws IOException, ClassNotFoundException {
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(byteOut);
		out.writeObject(src);

		ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
		ObjectInputStream in = new ObjectInputStream(byteIn);
		@SuppressWarnings("unchecked")
		List<T> dest = (List<T>) in.readObject();
		return dest;
	}

	public PaymentMessage deepCopy(PaymentMessage o) throws IOException, ClassNotFoundException {
		// 字节数组输出流，暂存到内存中
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		// 序列化
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(o);
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bis);
		// 反序列化
		return (PaymentMessage) ois.readObject();
	}

	@Override
	public void execute(Tuple tuple) {
		String streamid = tuple.getSourceStreamId();
		Object obj = tuple.getValue(0);

		if (RaceConfig.ORDERID_STREAM_ID.equals(streamid)) {
			synchronized (orderIds) {
				PaymentMessage pay = (PaymentMessage) obj;
				count++;
				_tmpPayList.add(pay);
				List<PaymentMessage> noPrcelist = new ArrayList<PaymentMessage>();
				List<Long> orders_tm = orderIds.get(RaceConfig.TM_flag);
				List<Long> orders_tb = orderIds.get(RaceConfig.TB_flag);
				for (PaymentMessage p : _tmpPayList) {
					doPayment(p, orders_tm, orders_tb, noPrcelist, tuple);
				}
				_tmpPayList.clear();
				_tmpPayList.addAll(noPrcelist);
				if (orders_tm.size() > 500) {
					for (int i = 0; i < 100; i++) {
						orders_tm.remove(i);
					}
				}
				if (orders_tb.size() > 500) {
					for (int i = 0; i < 100; i++) {
						orders_tb.remove(i);

					}
				}
			}

		}

		else if (obj == null) {
			Utils.sleep(10);
			return;
		}

		else if (obj instanceof String) {
			String str = (String) obj;
			long id = tuple.getLong(1);
			List<Long> orderLists = orderIds.get(str);
			if (orderLists == null) {
				orderLists = new ArrayList<Long>();
			}
			orderLists.add(id);
			orderIds.put(str, orderLists);

		}
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("orderSource"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		List<Long> tmplist = orderIds.get(RaceConfig.TB_flag);
		if (tmplist == null)
			orderIds.put(RaceConfig.TB_flag, new ArrayList<Long>());
		tmplist = orderIds.get(RaceConfig.TM_flag);
		if (tmplist == null)
			orderIds.put(RaceConfig.TM_flag, new ArrayList<Long>());
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