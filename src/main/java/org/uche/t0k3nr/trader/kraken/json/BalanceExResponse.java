package org.uche.t0k3nr.trader.kraken.json;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class BalanceExResponse {

	
	/*
	 * 
	 * {
  "error": [],
  "result": {
    "ZUSD": {
      "balance": 25435.21,
      "hold_trade": 8249.76
    },
    "XXBT": {
      "balance": 1.2435,
      "hold_trade": 0.8423
    }
  }
}
	 */
	

	List<String> error;
	Map<String, Balance> result;
	
	
	@JsonIgnoreProperties(ignoreUnknown = true)
	@JsonInclude(Include.NON_NULL)
	public static class Balance {
		
		Double balance;
		Double hold_trade;
		public Double getHold_trade() {
			return hold_trade;
		}
		public void setHold_trade(Double hold_trade) {
			this.hold_trade = hold_trade;
		}
		public Double getBalance() {
			return balance;
		}
		public void setBalance(Double balance) {
			this.balance = balance;
		}
		
	}


	public List<String> getError() {
		return error;
	}


	public void setError(List<String> error) {
		this.error = error;
	}


	public Map<String, Balance> getResult() {
		return result;
	}


	public void setResult(Map<String, Balance> result) {
		this.result = result;
	}
	


	
}
