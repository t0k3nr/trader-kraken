package org.uche.t0k3nr.trader.kraken.json;

import java.util.HashMap;
import java.util.Map;

import org.uche.t0ken.api.gdax.MpOrder;
import org.uche.t0ken.api.gdax.OrderType;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class AddOrder {

	private String method;
	private Map<String, Object> params = new HashMap<String, Object>();
	private Long req_id;
	
	/*
	 * /*
		 * 
		 * {
    "method": "add_order",
    "params": {
        "order_type": "limit",
        "side": "buy",
        "limit_price": 26500.4,
        "order_userref": 100054,
        "order_qty": 1.2,
        "symbol": "BTC/USD",
        "time_in_force":   [gtc, gtd, ioc]
        "token": "G38a1tGFzqGiUCmnegBcm8d4nfP3tytiNQz6tkCBYXY"
        "post_only": true
        "cl_ord_id": 
        "fee_preference":  [base, quote]
        
        Market:
        "cash_order_qty": 
    },
    "req_id": 123456789
}
		 */
	
	
	
	public AddOrder(MpOrder order, String token) {
		method = "add_order";
		params.put("token", token);
		
		params.put("order_type", order.getOrderType().getName());
		params.put("side", order.getSide().getName());
		if (order.getOrderType().equals(OrderType.LIMIT)) {
			params.put("limit_price", order.getPrice());
		} else if (order.getOrderType().equals(OrderType.MARKET)) {
			params.put("cash_order_qty", order.getSpecifiedFunds());
		}
		
		params.put("order_qty", order.getSize());
		params.put("symbol", order.getProductName());
		if (order.getTimeInForce() != null) params.put("time_in_force", order.getTimeInForce().name().toLowerCase());
		if (order.getPostOnly() != null) params.put("post_only", order.getPostOnly());
		params.put("cl_ord_id", order.getMyUUID());
		if (order.getFeeInQuote() != null) {
			if (order.getFeeInQuote()) {
				params.put("fee_preference", "quote");
			} else {
				params.put("fee_preference", "base");
			}
		}
		
		if (order.getMyUUID()!=null) req_id = Long.parseLong(order.getMyUUID(), 16);
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
