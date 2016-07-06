package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
import backtype.storm.utils.Utils;

public class CountAllBolt implements IRichBolt {

	private static final long serialVersionUID = 17897646549784L;
	OutputCollector collector;
	Map<String, Double> counts = new HashMap<String, Double>();

	long _beginStmap;
	long _endStamp;
	int countTimes = 0;
	String _timeStampValue;
	// 如何判断上一分钟的数据已经处理完毕，可以发送到tair
	private static Logger LOG = LoggerFactory.getLogger(CountAllBolt.class);
	static PlatFromCount tm;
	static PlatFromCount tb;
	private transient TairOperatorImpl tairOperator;
	private static final String tm_flag = RaceConfig.TM_flag;
	private static final String tb_flag = RaceConfig.TB_flag;
	long lastTime;
	boolean isInitTime = false;
	long oneMinute = 60000;
	// private transient TairOperatorImpl tairOperator;
	static List<PaymentMessage> lists = new ArrayList<PaymentMessage>();
	static int count = 0;

	public void prcoessPay(PlatFromCount p, PaymentMessage pay) {
		long crateTime = pay.getCreateTime();
		// 求该时间的整分时间
		long time = (crateTime / 1000) * 1000;
		Date d = new Date(time);
		long timeStamp = time - d.getSeconds() * 1000;
		if (p.getLastStmap() == 0) {
			p.setTimeStamp(timeStamp);
			p.setMiddleStamp(timeStamp - oneMinute);
			p.setLastStmap(timeStamp - 2 * oneMinute);
			p.setLastTotalPrice(0);
			p.setMiddleTotalPrice(0);
		}

		/*** 判断当前的时间是不是最后统计时间之前的3分钟,并且这个时刻的payment全部处理完毕 **/
		// 如何判断是否处理完毕,
		long timeSign = p.getTimeStamp();
		if (timeStamp > timeSign) {
			if (p.getLastTotalPrice() != 0)
				if (!writeData(p.getLastStmap() / 1000, p.getLastTotalPrice(), p.getOrderSrc()))
					LOG.info("-------insert tm and tp totoalprice error " + p);
			p.setLastStmap(p.getTimeStamp());
			p.setLastStmap(p.getMiddleStamp());
			p.setMiddleStamp(p.getTimeStamp());
			p.setTimeStamp(timeStamp);
			p.setLastTotalPrice(p.getMiddleTotalPrice());
			p.setMiddleTotalPrice(p.getTotalprice());
			p.setTotalprice(0);
		}
		if (timeStamp < timeSign) {

			if (timeStamp == p.getMiddleStamp()) {
				p.setMiddleTotalPrice(p.getMiddleTotalPrice() + pay.getPayAmount());
			} else if (timeStamp == p.getLastStmap()) {
				p.setLastTotalPrice(p.getLastTotalPrice() + pay.getPayAmount());
			} else {
				// RaceUtils.method1_WriteText("---该时间按数据处理过期" +
				// RaceUtils.formatDate(timeStamp) + ":" + pay);
			}

		}
		p.setTotalprice(p.getTotalprice() + pay.getPayAmount());
	}

	@Override
	public void execute(Tuple tuple) {
		PaymentMessage pay = (PaymentMessage) tuple.getValue(0);
		if (pay == null)
			return;
		if (tm_flag.equals(pay.getOrderSource()))
			prcoessPay(tm, pay);
		if (tb_flag.equals(pay.getOrderSource()))
			prcoessPay(tb, pay);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields("orderSource"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		tairOperator = new TairOperatorImpl();
		this.collector = collector;
		if (tm == null) {
			tm = new PlatFromCount();
			tm.setOrderSrc(tm_flag);
		}
		if (tb == null) {
			tb = new PlatFromCount();
			tb.setOrderSrc(tb_flag);
		}
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

	public boolean writeData(long timestamp, Number str, String pt) {
		String key = null;
		if ("tm".equals(pt))
			key = RaceConfig.prex_tmall + RaceConfig.teamcode + timestamp;
		else
			key = RaceConfig.prex_taobao + RaceConfig.teamcode + timestamp;
		RaceUtils.method1_WriteText(key + ":" + str);
		return tairOperator.write(key, str);

	}

}