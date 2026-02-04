package org.uche.t0k3nr.trader.kraken.testing;

import java.util.UUID;

import org.uche.t0k3nr.trader.kraken.json.OrderbookSubscribe;
import org.uche.t0ken.api.util.JsonUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestJson {

	public static void main(String[] args) throws JsonProcessingException {
		OrderbookSubscribe obs = new OrderbookSubscribe("BTC/USD", 10);
		
		System.out.println(JsonUtil.serialize(obs, false));
		
		
		
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree("{\"channel\":\"heartbeat\"}");
		
		
		System.out.println(JsonUtil.serialize(actualObj, false));
		
		System.out.println(UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE);
		
		
	}

}
