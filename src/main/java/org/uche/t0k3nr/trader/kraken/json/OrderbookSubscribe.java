package org.uche.t0k3nr.trader.kraken.json;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class OrderbookSubscribe implements Serializable {
	
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
	
	public OrderbookSubscribe(String pair, Integer depth) {
		method = "subscribe";
		params.put("channel", "book");
		params.put("depth", depth);
		params.put("symbol", List.of(pair));
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
