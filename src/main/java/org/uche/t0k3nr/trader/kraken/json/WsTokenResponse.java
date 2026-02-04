package org.uche.t0k3nr.trader.kraken.json;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class WsTokenResponse {

	/*
	 * {
  "error": [],
  "result": {
    "token": "1Dwc4lzSwNWOAwkMdqhssNNFhs1ed606d1WcF3XfEMw",
    "expires": 900
  }
}
	 */

	List<String> error;
	Result result;
	
	@JsonIgnoreProperties(ignoreUnknown = true)
	@JsonInclude(Include.NON_NULL)
	public static class Result {
		String token;
		Integer expire;
		public String getToken() {
			return token;
		}
		public void setToken(String token) {
			this.token = token;
		}
		public Integer getExpire() {
			return expire;
		}
		public void setExpire(Integer expire) {
			this.expire = expire;
		}
		
			
		
	}

	public List<String> getError() {
		return error;
	}

	public void setError(List<String> error) {
		this.error = error;
	}

	public Result getResult() {
		return result;
	}

	public void setResult(Result result) {
		this.result = result;
	}
	
	

	
}
