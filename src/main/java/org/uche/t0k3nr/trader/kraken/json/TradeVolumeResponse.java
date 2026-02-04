package org.uche.t0k3nr.trader.kraken.json;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class TradeVolumeResponse {

	/*
	 * {
  "error": [],
  "result": {
    "currency": "ZUSD",
    "volume": "200709587.4223",
    "fees": {
      "XXBTZUSD": {
        "fee": "0.1000",
        "minfee": "0.1000",
        "maxfee": "0.2600",
        "nextfee": null,
        "nextvolume": null,
        "tiervolume": "10000000.0000"
      }
    },
    "fees_maker": {
      "XXBTZUSD": {
        "fee": "0.0000",
        "minfee": "0.0000",
        "maxfee": "0.1600",
        "nextfee": null,
        "nextvolume": null,
        "tiervolume": "10000000.0000"
      }
    }
  }
}
	 */

	

	List<String> error;
	Result result;
	
	@JsonIgnoreProperties(ignoreUnknown = true)
	@JsonInclude(Include.NON_NULL)
	public static class Result {
		String currency;
		String volume;
		Map<String, Fees> fees;
		Map<String, Fees> fees_maker;
		
		@JsonIgnoreProperties(ignoreUnknown = true)
		@JsonInclude(Include.NON_NULL)
		public static class Fees {
			String fee;
			String minfee;
			String maxfee;
			String nextfee;
			String nextvolume;
			String tiervolume;
			public String getFee() {
				return fee;
			}
			public void setFee(String fee) {
				this.fee = fee;
			}
			public String getMinfee() {
				return minfee;
			}
			public void setMinfee(String minfee) {
				this.minfee = minfee;
			}
			public String getMaxfee() {
				return maxfee;
			}
			public void setMaxfee(String maxfee) {
				this.maxfee = maxfee;
			}
			public String getNextfee() {
				return nextfee;
			}
			public void setNextfee(String nextfee) {
				this.nextfee = nextfee;
			}
			public String getNextvolume() {
				return nextvolume;
			}
			public void setNextvolume(String nextvolume) {
				this.nextvolume = nextvolume;
			}
			public String getTiervolume() {
				return tiervolume;
			}
			public void setTiervolume(String tiervolume) {
				this.tiervolume = tiervolume;
			}
			
		}

		public String getCurrency() {
			return currency;
		}

		public void setCurrency(String currency) {
			this.currency = currency;
		}

		public String getVolume() {
			return volume;
		}

		public void setVolume(String volume) {
			this.volume = volume;
		}

		public Map<String, Fees> getFees() {
			return fees;
		}

		public void setFees(Map<String, Fees> fees) {
			this.fees = fees;
		}

		public Map<String, Fees> getFees_maker() {
			return fees_maker;
		}

		public void setFees_maker(Map<String, Fees> fees_maker) {
			this.fees_maker = fees_maker;
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
