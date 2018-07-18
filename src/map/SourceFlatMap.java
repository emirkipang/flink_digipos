package map;


import model.Source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import util.Constant;

public class SourceFlatMap implements
		FlatMapFunction<String, Source> {

	/**
* 
*/
	
//	33101;332000RG;Denpasar;2;S094;0750000044973700;20180301;081237777514;DO-332000RG-201803-0001;0750000044973700;0750000044974107;0812377;
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(String in, Collector<Source> out)
			throws Exception {
		// TODO Auto-generated method stub
		String[] lines = in.split("\n");

		for (String line : lines) {
			String[] items = line.split(";", -1);			
			
			String msisdn = "62"+items[7].substring(1, items[7].length());
						
			out.collect(new Source(msisdn, line));

		}

	}
}
