package com.alibaba.middleware.race;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import edu.emory.mathcs.backport.java.util.Collections;

public class RaceUtils {
	/**
	 * 由于我们是将消息进行Kryo序列化后，堆积到RocketMq，所有选手需要从metaQ获取消息，
	 * 反序列出消息模型，只要消息模型的定义类似于OrderMessage和PaymentMessage即可
	 * 
	 * @param object
	 * @return
	 */
	private static String DATE_PATTERN_3 = "yyyy/MM/dd HH:mm:ss";
	public static final String file = "./src/result.txt";
	public static final String onminute = "./src/result.txt";

//	public static void method1_WriteText(String conent) {
//		BufferedWriter out = null;
//		try {
//			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));// 追加写
//			out.write(conent + "\n");
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				out.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//	}

	private static int getIndex(List<Long>list,long time)
	{
		list.add(time);
		Collections.sort(list);
		int index=list.indexOf(time);
		return index;
	}
	public static void main(String[] args) throws ParseException {
		//System.out.println(1467942320290l%5000);
		List<Long> list=new ArrayList<Long>();
		list.add(12343l);
		System.out.println(getIndex(list,12354234l));

	}

	public static String formatDate(long str) {

		SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN_3);
		return sdf.format(new Date(str));

	}

	public static byte[] writeKryoObject(Object object) {
		Output output = new Output(1024);
		Kryo kryo = new Kryo();
		kryo.writeObject(output, object);

		output.flush();
		output.close();
		byte[] ret = output.toBytes();
		output.clear();
		return ret;
	}

	public static <T> T readKryoObject(Class<T> tClass, byte[] bytes) {
		Kryo kryo = new Kryo();
		Input input = new Input(bytes);
		input.close();
		T ret = kryo.readObject(input, tClass);
		return ret;
	}

}
