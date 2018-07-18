package util;

public class Constant {
	public static String BASEDIR = "D:/Data/GIA/output";

	public static String SOURCE = BASEDIR + "/ref/digipos/source";

	public static String REF_BLACKLIST = BASEDIR + "/ref/digipos/blacklist";

	public static String OUTPUT = BASEDIR + "/ref/digipos/out/summary.csv";

	public static String joinRule(String in, int length) {
		int gap = length - in.length();

		if (gap != 0) {
			for (int i = 1; i <= gap; i++) {
				in = "0" + in;
			}
		}

		return in;
	}

}
