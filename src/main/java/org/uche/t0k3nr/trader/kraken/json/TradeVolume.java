package org.uche.t0k3nr.trader.kraken.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class TradeVolume {

	private Long nonce;
	private String pair;
	
	public TradeVolume(long nonce, String pair) {
		this.nonce = nonce;
		this.pair = pair;
	}


	public Long getNonce() {
		return nonce;
	}


	public void setNonce(Long nonce) {
		this.nonce = nonce;
	}


	public String getPair() {
		return pair;
	}


	public void setPair(String pair) {
		this.pair = pair;
	}

	

	
}
