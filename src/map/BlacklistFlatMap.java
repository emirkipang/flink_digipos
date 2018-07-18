package map;


import model.Source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import util.Constant;



public class BlacklistFlatMap implements
		FlatMapFunction<String, Tuple1<String>> {

	/**
* 
*/
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(String in, Collector<Tuple1<String>> out)
			throws Exception {
		// TODO Auto-generated method stub
		String[] lines = in.split("\n");

		for (String line : lines) {		
			
			String msisdn = "62"+line.substring(1, line.length());
						
			out.collect(new Tuple1<String>(msisdn));

		}

	}
}
