package com.alibaba.middleware.race.model;

import java.io.Serializable;

/*
 * 一个时间戳内某个平台的对象
 */
public class PlatFromCount implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 124674615646L;
	private short platFrom;
	private long currentStamp;
	private long middleStamp;
	private double lastTotalPrice;
	private double middleTotalPrice;

	public double getMiddleTotalPrice() {
		return middleTotalPrice;
	}

	public void setMiddleTotalPrice(double middleTotalPrice) {
		this.middleTotalPrice = middleTotalPrice;
	}

	public double getLastTotalPrice() {
		return lastTotalPrice;
	}

	public void setLastTotalPrice(double lastTotalPrice) {
		this.lastTotalPrice = lastTotalPrice;
	}

	public long getMiddleStamp() {
		return middleStamp;
	}

	public void setMiddleStamp(long middleStamp) {
		this.middleStamp = middleStamp;
	}

	private long lastStmap;
	private String orderSrc;

	public String getOrderSrc() {
		return orderSrc;
	}

	public void setOrderSrc(String orderSrc) {
		this.orderSrc = orderSrc;
	}

	public long getLastStmap() {
		return lastStmap;
	}

	public void setLastStmap(long lastStmap) {
		this.lastStmap = lastStmap;
	}

	private double totalprice;

	public short getPlatFrom() {
		return platFrom;
	}

	public void setPlatFrom(short platFrom) {
		this.platFrom = platFrom;
	}

	public long getTimeStamp() {
		return currentStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.currentStamp = timeStamp;
	}

	public double getTotalprice() {
		return totalprice;
	}

	public void setTotalprice(double totalprice) {
		this.totalprice = totalprice;
	}

	@Override
	public int hashCode() {
		return platFrom;
	}

	@Override
	public String toString() {
		return platFrom + "-" + currentStamp + "-" + middleStamp + "-" + lastStmap + "-" + totalprice + "-" + orderSrc+"-";
	}

}
