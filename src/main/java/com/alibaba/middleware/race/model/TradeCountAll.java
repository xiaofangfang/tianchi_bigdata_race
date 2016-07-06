package com.alibaba.middleware.race.model;

import java.io.Serializable;

public class TradeCountAll implements Serializable {
	private long time;
	// pt1当前时间的统计结果
	private PlatFromCount pt1;
	// pt2当前时间的统计结果
	private PlatFromCount pt0;

	public TradeCountAll() {
		short p1 = 1;
		short p0 = 0;
		pt1 = new PlatFromCount();
		pt1.setPlatFrom(p1);

		pt0 = new PlatFromCount();
		pt0.setPlatFrom(p0);
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public PlatFromCount getPt1() {
		return pt1;
	}

	public void setPt1(PlatFromCount pt1) {
		this.pt1 = pt1;
	}

	public PlatFromCount getPt0() {
		return pt0;
	}

	public void setPt0(PlatFromCount pt0) {
		this.pt0 = pt0;
	}
}
