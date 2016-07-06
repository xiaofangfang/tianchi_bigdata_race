package com.alibaba.middleware.race.model;

import java.io.Serializable;
import java.util.List;

import clojure.main;

public class BillCount implements Serializable {
	private static final long serialVersionUID = 1465454544654654L;
	private long timeStamp;
	private List<Long> orderIds;
	private double totalPrice;

	public BillCount() {

	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public List<Long> getOrderIds() {
		return orderIds;
	}

	public void setOrderIds(List<Long> orderIds) {
		this.orderIds = orderIds;
	}

	public double getPays() {
		return totalPrice;
	}

	public void setPays(double totalPrice) {
		this.totalPrice = totalPrice;
	}

	public static void main(String[] args) {
       
	}
}
