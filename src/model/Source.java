package model;

public class Source {
	private String msisdn;
	private String data;
	
	public Source(){
		
	}
	
	

	public Source(String msisdn, String data) {
		super();
		this.msisdn = msisdn;
		this.data = data;
	}



	public String getMsisdn() {
		return msisdn;
	}

	public void setMsisdn(String msisdn) {
		this.msisdn = msisdn;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}
	
	
	
}
