package join;

import model.Source;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


public class DOLeftJoinBlacklist
		implements
		FlatJoinFunction<Source, Tuple1<String>, Tuple1<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void join(Source leftElem,
			Tuple1<String> rightElem, Collector<Tuple1<String>> out)
			throws Exception {
		// TODO Auto-generated method stub

		if (rightElem != null){							
			out.collect(new Tuple1<String>(leftElem.getData()));
		}			

	}

}
