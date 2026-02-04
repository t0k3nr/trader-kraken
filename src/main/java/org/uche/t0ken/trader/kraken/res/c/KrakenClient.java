package org.uche.t0ken.trader.kraken.res.c;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.uche.t0k3nr.trader.kraken.json.BalanceEx;
import org.uche.t0k3nr.trader.kraken.json.BalanceExResponse;
import org.uche.t0k3nr.trader.kraken.json.TradeVolume;
import org.uche.t0k3nr.trader.kraken.json.TradeVolumeResponse;
import org.uche.t0k3nr.trader.kraken.json.WsToken;
import org.uche.t0k3nr.trader.kraken.json.WsTokenResponse;

import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

@RegisterRestClient
public interface KrakenClient {

		
	@POST
    @Path("/0/private/BalanceEx")
	BalanceExResponse getBalanceEx(
			@HeaderParam("API-Key") String keyId, 
			@HeaderParam("API-Sign") String sign, 
			BalanceEx req
			);
	
	@POST
    @Path("/0/private/TradeVolume")
	TradeVolumeResponse getTradeVolume(
			@HeaderParam("API-Key") String keyId, 
			@HeaderParam("API-Sign") String sign, 
			TradeVolume req
			);
	
	@POST
    @Path("/0/private/GetWebSocketsToken")
	WsTokenResponse getWSToken(
			@HeaderParam("API-Key") String keyId, 
			@HeaderParam("API-Sign") String sign, 
			WsToken req
			);
	
	
	
	
}
