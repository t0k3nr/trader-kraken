package org.uche.t0ken.trader.kraken.ws;

import org.jboss.logging.Logger;
import org.uche.t0ken.api.gdax.Data;
import org.uche.t0ken.api.util.JsonUtil;

import com.fasterxml.jackson.core.JsonProcessingException;

import jakarta.websocket.EncodeException;
import jakarta.websocket.Encoder;
import jakarta.websocket.EndpointConfig;


public class DataEncoder  implements Encoder.Text<Data> {

	private static final Logger logger = Logger.getLogger(DataEncoder.class);

	@Override
	public void init(EndpointConfig config) {}

	@Override
	public void destroy() {}

	@Override
	public String encode(Data object) throws EncodeException {
		logger.info("encode: message: " + object);
		try {
			return JsonUtil.serialize(object, true);
		} catch (JsonProcessingException e) {
			logger.error("encode: error serializng " + object);
			return "{ \"error\": \"" + e.getMessage()+ "\" }";
		}
		
	}

}
