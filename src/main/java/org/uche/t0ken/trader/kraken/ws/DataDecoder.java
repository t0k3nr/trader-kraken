package org.uche.t0ken.trader.kraken.ws;

import org.jboss.logging.Logger;
import org.uche.t0ken.api.gdax.Data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.websocket.DecodeException;
import jakarta.websocket.Decoder;
import jakarta.websocket.EndpointConfig;

public class DataDecoder  implements Decoder.Text<Data> {

	private static final Logger logger = Logger.getLogger(DataDecoder.class);

	@Override
	public void init(EndpointConfig config) {}

	@Override
	public void destroy() {}

	@Override
	public Data decode(String s) throws DecodeException {
		ObjectMapper mapper = new ObjectMapper();
		Data data = null;
		try {
			data = mapper.readValue(s, Data.class);
			logger.info("decode: message: success from " + data.getClass().getCanonicalName());
		} catch (JsonMappingException e1) {
			logger.warn("decode: message: " + s + " " + e1.getMessage());
		} catch (JsonProcessingException e1) {
			logger.warn("decode: message: " + s + " " + e1.getMessage());
		} 
		return data;
	}

	@Override
	public boolean willDecode(String s) {
		return true;
	}

	

}
