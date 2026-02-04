package org.uche.t0ken.trader.kraken.ws;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.uche.t0k3nr.trader.kraken.json.OrderbookSubscribe;
import org.uche.t0ken.api.gdax.HeartBeat;
import org.uche.t0ken.api.gdax.L2Change;
import org.uche.t0ken.api.gdax.L2Update;
import org.uche.t0ken.api.gdax.Order;
import org.uche.t0ken.api.gdax.Side;
import org.uche.t0ken.api.gdax.Snapshot;
import org.uche.t0ken.api.util.JsonUtil;
import org.uche.t0ken.trader.kraken.svc.KrakenService;
import org.uche.t0ken.trader.kraken.svc.TraderService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.inject.Inject;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.CloseReason;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;





@ClientEndpoint(encoders = {org.uche.t0ken.trader.kraken.ws.DataEncoder.class}, decoders = {org.uche.t0ken.trader.kraken.ws.DataDecoder.class})
public class KrakenPublicWsClient {

	@ConfigProperty(name = "org.uche.t0ken.kraken.product")
	String product;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.ws.pair")
	String wsProduct;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.book.depth")
	Integer bookDepth;
	
	
	@Inject KrakenService krakenService;
	@Inject TraderService traderService;
	@Inject KrakenWsController controller;
	
	
	private static final Logger logger = Logger.getLogger("GdaxWsClient");
	
	
	
	@OnOpen
    public void onOpen(Session session,
            EndpointConfig config) {
		
		//return managedExecutor.runAsync(threadContext.contextualRunnable(() -> {
	        
			logger.info("onOpen: connected");
			
			
			
			
			try {
				
				OrderbookSubscribe obs = new OrderbookSubscribe(wsProduct, bookDepth);
				
				String message = JsonUtil.serialize(obs, false);
				String logMessage = message.replaceAll(",\"token\":\".*\"", ",\"token\":\"*********\"");
				logger.info("onOpen: sending subscribe: " + logMessage);
				session.getAsyncRemote().sendObject(message);
				logger.info("onOpen: subscribe sent: " + logMessage);
				
				
				
				
			} catch (Exception e) {
				logger.error("onOpen: subscribe failure", e);
			}
			
		//}));
		
			logger.info("onOpen: exiting onOpen...");
		
		
    }

	@OnMessage
	 void message(String message) {
		
		//logger.warn("message: (public) " + message);
		
		
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = null;
		try {
			actualObj = mapper.readTree(message);
			
			JsonNode channelNode = actualObj.get("channel");
			
			if (channelNode != null) {
				String channel = channelNode.asText();
				if (channel.equals("heartbeat")) {
					HeartBeat hb = new HeartBeat();
					hb.setTime(Instant.now());
					traderService.queueObEvent(hb);
					traderService.notifyObQueueProcessorLockObj();
					controller.setPublicAlive();
				} else if (channel.equals("book")) {
					JsonNode typeNode = actualObj.get("type");
					if (typeNode != null) {
						String type = typeNode.asText();
						
						/*
						 * {"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":96953.3,"qty":2.57855918}],"asks":[],"checksum":670882415,"timestamp":"2025-02-10T00:22:43.211991Z"}]}
						 */
						
						if (type.equals("update")) {
							
							L2Update update = new L2Update();
							List<L2Change> changes = new ArrayList<L2Change>();
							update.setChanges(changes);
							
							
							JsonNode dataNode = actualObj.get("data");
							if (dataNode != null) {
								
								JsonNode bookNode = dataNode.get(0);
								
								if (bookNode != null) {
									
									JsonNode timestampNode = bookNode.get("timestamp");
									if (timestampNode != null) {
										update.setTime(Instant.parse(timestampNode.asText()));
									}
									
									JsonNode bidsNode = bookNode.get("bids");
									
									if (bidsNode != null) {
										for (int i=0; i<bidsNode.size(); i++) {
											JsonNode bidNode = bidsNode.get(i);
											if (bidNode != null) {
												L2Change o = new L2Change();
												o.setPrice(BigDecimal.valueOf(Double.parseDouble(bidNode.get("price").asText())));
												o.setSize(BigDecimal.valueOf(Double.parseDouble(bidNode.get("qty").asText())));
												o.setSide(Side.BUY);
												changes.add(o);
											}
										}
									}
									
									JsonNode asksNode = bookNode.get("asks");
									
									if (asksNode != null) {
										for (int i=0; i<asksNode.size(); i++) {
											JsonNode askNode = asksNode.get(i);
											if (askNode != null) {
												L2Change o = new L2Change();
												o.setPrice(BigDecimal.valueOf(Double.parseDouble(askNode.get("price").asText())));
												o.setSize(BigDecimal.valueOf(Double.parseDouble(askNode.get("qty").asText())));
												o.setSide(Side.SELL);
												changes.add(o);
											}
										}
									}
									
									JsonNode checksumNode = bookNode.get("checksum");
									
									if (checksumNode!= null) {
										String checksum = checksumNode.asText();
									}
									
								}
								
								
								
								
							}
							try {
								//logger.info("message: update " + JsonUtil.serialize(update, false));

								traderService.queueObEvent(update);
								traderService.notifyObQueueProcessorLockObj();

							} catch (Exception e) {
								logger.error("message: " + message, e);
							}
							
						} else if (type.equals("snapshot")) {
							
							Snapshot snapshot = new Snapshot();
							List<Order> bids = new ArrayList<Order>();
							List<Order> asks = new ArrayList<Order>();
							snapshot.setAsks(asks);
							snapshot.setBids(bids);
							
							JsonNode dataNode = actualObj.get("data");
							if (dataNode != null) {
								
								JsonNode bookNode = dataNode.get(0);
								
								if (bookNode != null) {
									JsonNode bidsNode = bookNode.get("bids");
									
									if (bidsNode != null) {
										for (int i=0; i<bidsNode.size(); i++) {
											JsonNode bidNode = bidsNode.get(i);
											if (bidNode != null) {
												Order o = new Order();
												o.setPrice(BigDecimal.valueOf(Double.parseDouble(bidNode.get("price").asText())));
												o.setSize(BigDecimal.valueOf(Double.parseDouble(bidNode.get("qty").asText())));
												bids.add(o);
											}
										}
									}
									
									JsonNode asksNode = bookNode.get("asks");
									
									if (asksNode != null) {
										for (int i=0; i<asksNode.size(); i++) {
											JsonNode askNode = asksNode.get(i);
											if (askNode != null) {
												Order o = new Order();
												o.setPrice(BigDecimal.valueOf(Double.parseDouble(askNode.get("price").asText())));
												o.setSize(BigDecimal.valueOf(Double.parseDouble(askNode.get("qty").asText())));
												asks.add(o);
											}
										}
									}
									
									JsonNode checksumNode = bookNode.get("checksum");
									
									if (checksumNode!= null) {
										String checksum = checksumNode.asText();
									}
									
								}
								
								
								
								
							}
							try {
								logger.info("message: snapshot: " + JsonUtil.serialize(snapshot, false));

								traderService.queueObEvent(snapshot);
								traderService.notifyObQueueProcessorLockObj();

							} catch (Exception e) {
								logger.error("message: " + message, e);
							}
							
						}
					}
					
				}
			}
			
		} catch (JsonMappingException e) {
			logger.error("", e);
		} catch (JsonProcessingException e) {
			logger.error("", e);
		}
		
		
	 }
    
	@OnClose
	void onClose(Session s,
            CloseReason closeReason) {
		
			logger.warn("onClose: closed ws: " + closeReason.getReasonPhrase());
		
	        
	        
	        controller.setPublicConnected(false);
    }
    

	@OnError
	void onError(Session s,
            Throwable thr) {
		 logger.error("onClose: error ws: ", thr);
		 
		 controller.setPublicConnected(null);
		
    }
	
	
	
	
	

	
}
