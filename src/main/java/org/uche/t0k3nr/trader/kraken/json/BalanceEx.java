package org.uche.t0k3nr.trader.kraken.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class BalanceEx {

	private Long nonce;
	
	
	public BalanceEx(long nonce) {
		this.nonce = nonce;
		
	}


	public Long getNonce() {
		return nonce;
	}


	public void setNonce(Long nonce) {
		this.nonce = nonce;
	}

	

	
}
