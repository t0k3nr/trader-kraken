package org.uche.t0k3nr.trader.kraken.json;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class ExecutionsSubscribe implements Serializable {
	
	/*
	{
	    "method": "subscribe",
	    "params": {
	        "channel": "book",
	        "symbol": [
	            "ALGO/USD",
	            "MATIC/USD"
	        ]
	    }
	}
	 */
	
	private String method;
	private Map<String, Object> params = new HashMap<String, Object>();
	
	public ExecutionsSubscribe(String token) {
		method = "subscribe";
		params.put("channel", "executions");
		params.put("token", token);
		
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public Map<String, Object> getParams() {
		return params;
	}

	public void setParams(Map<String, Object> params) {
		this.params = params;
	}
	
}
