package com.alibaba.middleware.race.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;

public class OrderIds implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1464564564L;
	private static Map<Long, List<Long>> tm = new HashMap<Long, List<Long>>();
	private static Map<Long, List<Long>> tb = new HashMap<Long, List<Long>>();
	private static Map<Long, Double> tm_total = new HashMap<Long, Double>();
	private static Map<Long, Double> tb_total = new HashMap<Long, Double>();

	
    
	public synchronized static void tmPutTotal(Long time, Double price) {
		tm_total.put(time, price);
	}

	public synchronized static void tbPutTotal(Long time, Double price) {
		tb_total.put(time, price);
	}

	public static Map<Long, Double> getTm_total() {
		return tm_total;
	}

	public static void setTm_total(Map<Long, Double> tm_total) {
		OrderIds.tm_total = tm_total;
	}

	public static Map<Long, Double> getTb_total() {
		// RaceUtils.method1_WriteText("--tbtotal-"+tb_total);
		return tb_total;
	}

	public static void setTb_total(Map<Long, Double> tb_total) {
		OrderIds.tb_total = tb_total;
	}

	public synchronized static void add(Map<Long, List<Long>> maps, final String flag)

	{
		if (RaceConfig.TM_flag.equals(flag)) {
			for (Long l : maps.keySet()) {
				List<Long> list = maps.get(l);
				if (tm.containsKey(l)) {
					List<Long> tmp = tm.get(l);
					if (tmp == null) {
						list.addAll(tmp);
						tm.put(l, list);
						continue;
					}
					tmp.addAll(list);
					tm.put(l, tmp);
				} else {
					tm.put(l, list);
				}
			}
			// tm.putAll(maps);
		}
		if (RaceConfig.TB_flag.equals(flag)) {
			for (Long l : maps.keySet()) {
				List<Long> list = maps.get(l);
				if (tb.containsKey(l)) {
					List<Long> tmp = tb.get(l);
					if (tmp == null) {
						list.addAll(tmp);
						tb.put(l, list);
						continue;
					}
					tmp.addAll(list);
					tb.put(l, tmp);
				} else {
					tb.put(l, list);
				}
			}
		}
	}

	public synchronized static String containOrderId(long l) {
		long v = l % RaceConfig.div;
		if (tb.containsKey(v)) {
			List<Long> orderids = tb.get(v);
			if (orderids != null && orderids.contains(l))
				return RaceConfig.TB_flag;
		}
		if (tm.containsKey(v)) {
			List<Long> orderids = tm.get(v);
			if (orderids != null && orderids.contains(l))
				return RaceConfig.TM_flag;
		}

		return null;
	}

	public static Map<Long, List<Long>> getTm() {
		return tm;
	}

	public static void setTm(Map<Long, List<Long>> tm) {
		OrderIds.tm = tm;
	}

	public static Map<Long, List<Long>> getTb() {
		return tb;
	}

	public static void setTb(Map<Long, List<Long>> tb) {
		OrderIds.tb = tb;
	}

}
