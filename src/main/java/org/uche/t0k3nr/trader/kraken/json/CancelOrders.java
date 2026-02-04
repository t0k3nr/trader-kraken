package org.uche.t0k3nr.trader.kraken.json;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class CancelOrders {

	private String method;
	private Map<String, Object> params = new HashMap<String, Object>();
	
	private Long req_id;
	
	
	/*
	 * {
    "method": "cancel_order",
    "params": {
        "order_id": [
            "OM5CRX-N2HAL-GFGWE9",
            "OLUMT4-UTEGU-ZYM7E9"
        ],
        "token": "zGXT1dUQQjJjy5VmGXMegdDQngXXehNo5qbMBVolwEQ"
    },
    "req_id": 123456789
},


		 */
	
	
	
	public CancelOrders(List<String> orderIds, String token) {
		method = "cancel_order";
		params.put("token", token);
		
		params.put("order_id", orderIds);
		       
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

	public Long getReq_id() {
		return req_id;
	}

	public void setReq_id(Long req_id) {
		this.req_id = req_id;
	}
}
