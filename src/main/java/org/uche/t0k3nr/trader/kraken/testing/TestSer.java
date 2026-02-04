package org.uche.t0k3nr.trader.kraken.testing;

import org.uche.t0k3nr.trader.kraken.json.TradeVolume;
import org.uche.t0ken.api.util.JsonUtil;

import com.fasterxml.jackson.core.JsonProcessingException;

public class TestSer {

	public static void main(String[] args) throws JsonProcessingException {
		TradeVolume vol = new TradeVolume(1234, "BTCUSD");
		
		System.out.println(JsonUtil.serialize(vol, false));
		
	}

}
