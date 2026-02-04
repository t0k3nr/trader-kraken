package org.uche.t0ken.trader.kraken.ws;

import java.math.BigDecimal;
import java.time.Instant;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.uche.t0k3nr.trader.kraken.json.ExecutionsSubscribe;
import org.uche.t0ken.api.gdax.Confirmed;
import org.uche.t0ken.api.gdax.Done;
import org.uche.t0ken.api.gdax.HeartBeat;
import org.uche.t0ken.api.gdax.Match;
import org.uche.t0ken.api.gdax.Open;
import org.uche.t0ken.api.gdax.OrderType;
import org.uche.t0ken.api.gdax.Reason;
import org.uche.t0ken.api.gdax.Received;
import org.uche.t0ken.api.gdax.Restored;
import org.uche.t0ken.api.gdax.Side;
import org.uche.t0ken.api.gdax.TimeInForce;
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
public class KrakenPrivateWsClient {

	@ConfigProperty(name = "org.uche.t0ken.kraken.product")
	String product;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.ws.pair")
	String wsProduct;
	
	
	@Inject KrakenService krakenService;
	@Inject TraderService traderService;
	@Inject KrakenWsController controller;
	
	
	
	
	private static final Logger logger = Logger.getLogger("GdaxWsClient");
	
	
	
	@OnOpen
    public void onOpen(Session session,
            EndpointConfig config) {
		
		//return managedExecutor.runAsync(threadContext.contextualRunnable(() -> {
	        
			logger.info("onOpen: connected");
			
			
			String token = krakenService.getToken(true);
			
			try {
				
				ExecutionsSubscribe exs = new ExecutionsSubscribe(token);
				
				String message = JsonUtil.serialize(exs, false);
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
		
		//logger.warn("message: (private) "  + message);
		
		
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = null;
		try {
			actualObj = mapper.readTree(message);
			
			JsonNode methodNode = actualObj.get("method");
			
			if (methodNode != null) {
				String method = methodNode.asText();
				if (method.equals("add_order")) {
					
					JsonNode successNode = actualObj.get("success");
					
					boolean success = false;
					
					if (successNode != null) {
						success = successNode.asBoolean();
					}
					String cliId = null;
					String orderId = null;
					if (success) {
						JsonNode resultNode = actualObj.get("result");
						if (resultNode != null) {
							orderId = resultNode.get("order_id").asText();
							
							JsonNode cliIdNode = resultNode.get("cl_ord_id");
							
							if (cliIdNode != null) {
								cliId =  cliIdNode.asText();
							} 
						
					} else {
						
							JsonNode reqIdNode = actualObj.get("req_id");
							if (reqIdNode != null) {
								cliId = Long.toHexString(Long.parseLong(reqIdNode.asText()));
							}
						
					}
						
					
					Confirmed confirmed = new Confirmed();
					confirmed.setOrder_id(orderId);
					confirmed.setClient_oid(cliId);
					confirmed.setSuccess(success);
					logger.info("message: add_order response: " + JsonUtil.serialize(confirmed, false));
					traderService.queueTrEvent(confirmed); 
					traderService.notifyTrQueueProcessorLockObj();
				
					}
					logger.info("message: add order response: " + message);
				} else if (method.equals("cancel_order")) {
					logger.info("message: cancel orders response:" + message);
				}
				
				
				
			} else {
				JsonNode channelNode = actualObj.get("channel");
				if (channelNode != null) {
					String channel = channelNode.asText();
					if (channel.equals("heartbeat")) {
						HeartBeat hb = new HeartBeat();
						hb.setTime(Instant.now());
						traderService.queueObEvent(hb);
						traderService.notifyObQueueProcessorLockObj();
						controller.setPrivateAlive();
					} else if (channel.equals("executions")) {
						JsonNode typeNode = actualObj.get("type");
						if (typeNode != null) {
							String type = typeNode.asText();
							if (type.equals("update") || type.equals("snapshot")) {
								JsonNode dataNode = actualObj.get("data");
								if (dataNode != null) {
									
									
									for (int i=0; i<dataNode.size(); i++) {
										
										JsonNode currentNode = dataNode.get(i);
										logger.info("i: " + i + " currentNode: " + currentNode);
										
										if (currentNode != null) {
											
											
											
											String symbol = null;
											JsonNode symbolNode = currentNode.get("symbol");
											if (symbolNode != null) {
												symbol = symbolNode.asText();
											}
											
											
												
												
												String orderId = null;
												JsonNode orderIdNode = currentNode.get("order_id");
												if (orderIdNode != null) {
													orderId = orderIdNode.asText();
												}
												String orderStatus = null;
												JsonNode orderStatusNode = currentNode.get("order_status");
												if (orderStatusNode != null) {
													orderStatus = orderStatusNode.asText();
												}
												
												String execType = null;
												JsonNode execTypeNode = currentNode.get("exec_type");
												if (execTypeNode != null) {
													execType = execTypeNode.asText();
												}
												
												
												Side side = null;
												JsonNode sideNode = currentNode.get("side");
												if (sideNode != null) {
													side =  Side.getEnum(sideNode.asText());
												}
												
												OrderType orderType = null;
												JsonNode orderTypeNode = currentNode.get("order_type");
												if (orderTypeNode != null) {
													orderType =  OrderType.getEnum(orderTypeNode.asText());
												}
												
												BigDecimal cumQty = null;
												JsonNode cumQtyNode = currentNode.get("cum_qty");
												if (cumQtyNode != null) {
													cumQty =  BigDecimal.valueOf(Double.parseDouble(cumQtyNode.asText()));
												}
												
												BigDecimal size = null;
												JsonNode sizeNode = currentNode.get("size");
												if (sizeNode != null) {
													size =  BigDecimal.valueOf(Double.parseDouble(sizeNode.asText()));
												}
												
												BigDecimal orderQty = null;
												JsonNode orderQtyNode = currentNode.get("order_qty");
												if (orderQtyNode != null) {
													orderQty =  BigDecimal.valueOf(Double.parseDouble(orderQtyNode.asText()));
												}
												
												
												BigDecimal price = null;
												JsonNode priceNode = currentNode.get("limit_price");
												if (priceNode != null) {
													price =  BigDecimal.valueOf(Double.parseDouble(priceNode.asText()));
												}
												
												BigDecimal cashOrder = null;
												JsonNode cashOrderNode = currentNode.get("cash_order_qty");
												if (cashOrderNode != null) {
													cashOrder =  BigDecimal.valueOf(Double.parseDouble(cashOrderNode.asText()));
												}
												
												TimeInForce tif = null;
												JsonNode tifNode = currentNode.get("time_in_force");
												if (tifNode != null) {
													tif =  TimeInForce.getEnum(tifNode.asText().toUpperCase());
												}
												
												
												Boolean postOnly = null;
												JsonNode postOnlyNode = currentNode.get("post_only");
												if (postOnlyNode != null) {
													postOnly = postOnlyNode.asBoolean();
												}
												
												Boolean feeInQuote = null;
												JsonNode feeInQuoteNode = currentNode.get("fee_ccy_pref");
												if (feeInQuoteNode != null) {
													if (feeInQuoteNode.asText().equals("fciq")) {
														feeInQuote = true;
													} else {
														feeInQuote = false;
													}
												}
												
												Instant ts = null;
												JsonNode tsNode = currentNode.get("timestamp");
												if (tsNode != null) {
													ts =  Instant.parse(tsNode.asText());
												}
												String cliId = null;
												
												JsonNode cliIdNode = currentNode.get("cl_ord_id");
												if (cliIdNode != null) {
													cliId =  cliIdNode.asText();
												
												}
												
												
												if ((orderStatus.equals("pending_new"))) {
													
													if (symbol.equals(wsProduct)) {
														Received received = new Received();
														received.setOrder_id(orderId);
														received.setSide(side);
														received.setOrder_type(orderType);
														received.setClient_oid(cliId);
														if (orderType.equals(OrderType.MARKET)) {
															received.setFunds(cashOrder);
															received.setSize(orderQty);
														} else {
															received.setSize(orderQty);
															received.setPrice(price);
														}
														received.setPost_only(postOnly);
														received.setFeeInQuote(feeInQuote);
														received.setTime(ts);
														received.setTimeInForce(tif);
														received.setProductName(symbol);
														
														logger.info("message: " + JsonUtil.serialize(received, false));
														traderService.queueTrEvent(received); 
														traderService.notifyTrQueueProcessorLockObj();
													}
													
												
												} else if (orderStatus.equals("new")) {
													
													if (type.equals("update")) {
														Open open = new Open();
														open.setOrder_id(orderId);
														open.setSide(side);
														
														
														logger.info("message: " + JsonUtil.serialize(open, false));
														traderService.queueTrEvent(open); 
														traderService.notifyTrQueueProcessorLockObj();
													} else {
														// snapshot
														if (symbol.equals(wsProduct)) {
															Restored restored = new Restored();
															restored.setOrder_id(orderId);
															restored.setClient_oid(cliId);
															restored.setSide(side);
															restored.setOrder_type(orderType);
															if (orderType.equals(OrderType.MARKET)) {
																restored.setFunds(cashOrder);
																restored.setSize(orderQty);
															} else {
																restored.setSize(orderQty);
																restored.setPrice(price);
															}
															restored.setPost_only(postOnly);
															restored.setFeeInQuote(feeInQuote);
															restored.setTime(ts);
															restored.setTimeInForce(tif);
															restored.setProductName(symbol);
															
															logger.info("message: " + JsonUtil.serialize(restored, false));
															traderService.queueTrEvent(restored); 
															traderService.notifyTrQueueProcessorLockObj();
														}
														
													}
													
													
												} else if (orderStatus.equals("filled")) {
													Done done = new Done();
													done.setOrder_id(orderId);
													done.setReason(Reason.FILLED);
													done.setExecuted_size(cumQty);
													
													logger.info("message: " + JsonUtil.serialize(done, false));
													traderService.queueTrEvent(done); 
													traderService.notifyTrQueueProcessorLockObj();
												} else if (orderStatus.equals("canceled")) {
													
													Done done = new Done();
													done.setOrder_id(orderId);
													done.setSide(side);
													done.setReason(Reason.CANCELED);
													done.setExecuted_size(cumQty);
													
													logger.info("message: " + JsonUtil.serialize(done, false));
													traderService.queueTrEvent(done); 
													traderService.notifyTrQueueProcessorLockObj();
												} else if (orderStatus.equals("partially_filled")) {
													
													
													if (execType.equals("trade")) {
														
														if (symbol.equals(wsProduct)) {
															Match match = new Match();
															
															BigDecimal lastSize = null;
															JsonNode lastSizeNode = currentNode.get("last_qty");
															if (lastSizeNode != null) {
																lastSize =  BigDecimal.valueOf(Double.parseDouble(lastSizeNode.asText()));
															}
															
															BigDecimal lastPrice = null;
															JsonNode lastPriceNode = currentNode.get("last_price");
															if (lastPriceNode != null) {
																lastPrice =  BigDecimal.valueOf(Double.parseDouble(lastPriceNode.asText()));
															}
															
															BigDecimal fees = null;
															JsonNode feesNode = currentNode.get("fees");
															if (feesNode != null) {
																/*
																"fees":[
															            {
															               "asset":"USD",
															               "qty":0.02000
															            }
															         ]*/
																
																JsonNode assetFeeNode = feesNode.get(0);
																if (assetFeeNode != null) {
																	fees = BigDecimal.valueOf(Double.parseDouble(assetFeeNode.get("qty").asText()));
																}
																
															}
															
															String liqIndicator = null;
															JsonNode liqIndicatorNode = currentNode.get("liquidity_ind");
															if (liqIndicatorNode != null) {
																liqIndicator =  liqIndicatorNode.asText();
																if (liqIndicator.equals("m")) {
																	match.setMaker_order_id(orderId);
																} else {
																	match.setTaker_order_id(orderId);
																}
															}
															
															
															match.setSide(side);
															match.setOrder_id(orderId);
															match.setSize(lastSize);
															match.setPrice(lastPrice);
															match.setFees(fees);
															
															logger.info("message: match " + JsonUtil.serialize(match, false));
															traderService.queueTrEvent(match); 
															traderService.notifyTrQueueProcessorLockObj();
														}
														
													}
													
												}
												
												
											
												
											
											
											
											
										}
										
									}
								}
								
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
		/*
		 * Orders:
		Received
		Done
		Open
		Change
		Match
		*/
		
		/*
		 * {"channel":"executions","type":"update","data":
		 * [{"order_id":"OF6QIS-MYM6V-XTHHZF",
		 * "symbol":"BTC/USD",
		 * "order_qty":0.00009073,
		 * "cum_cost":0.00000,"time_in_force":"GTC",
		 * "exec_type":"pending_new","side":"buy","order_type":"limit","order_userref":0,
		 * "limit_price_type":"static","limit_price":95771.3,"stop_price":0.0,"post_only":true,"order_status":"pending_new",
		 * "fee_usd_equiv":0.00000,"fee_ccy_pref":"fciq","timestamp":"2025-02-10T02:55:15.922831Z"}],"sequence":2}
			{"channel":"executions","type":"update","data":[{"timestamp":"2025-02-10T02:55:15.922831Z","order_status":"new","exec_type":"new","order_userref":0,"post_only":true,"order_id":"OF6QIS-MYM6V-XTHHZF"}],"sequence":3}
		 
		 * {"channel":"executions","type":"update","data":[{"timestamp":"2025-02-10T03:52:21.072511Z",
		 * "order_status":"canceled","exec_type":"canceled",
		 * "cum_qty":0.00000000,"cum_cost":0.00000,"fee_usd_equiv":0.00000,
		 * "avg_price":0.0,"order_userref":0,"post_only":true,"cancel_reason":"User requested",
		 * "reason":"User requested","order_id":"OXFRRV-RHDOI-SBQOPK"}],"sequence":2}
		 *
		 */
		
		/*
			try {
				Data data = (Data) JsonUtil.deserialize(message);
				if (data instanceof HeartBeat) {

					traderService.queueObEvent(data);
					traderService.notifyObQueueProcessorLockObj();
					controller.setAlive();
					
				} else if (data instanceof Subscriptions) {
					logger.warn("GdaxWsClient: subscription: " + message);
				} else if (data instanceof Snapshot) {
					try {
						//logger.info("message: " + JsonUtil.serialize(data, false));

						traderService.queueObEvent(data);
						traderService.notifyObQueueProcessorLockObj();

					} catch (Exception e) {
						logger.error("message: " + message, e);
					}
				} else if (data instanceof L2Update) {
					try {
						//logger.info("message: " + JsonUtil.serialize(data, false));

						traderService.queueObEvent(data);
						traderService.notifyObQueueProcessorLockObj();
						traderService.notifyTrQueueProcessorLockObj();

					} catch (Exception e) {
						logger.error("message: " + message, e);
					}
				} else {
					logger.info("message: " + JsonUtil.serialize(data, false));
					traderService.queueTrEvent(data); 
					traderService.notifyTrQueueProcessorLockObj();
				}
				
			} catch (Exception e) {
				logger.error("message: " + message, e);
				
			}*/
	 }
    
	@OnClose
	void onClose(Session s,
            CloseReason closeReason) {
		
	        logger.warn("onClose: closed ws: " + closeReason.getReasonPhrase());
	        controller.setPrivateConnected(false);
    }
    

	@OnError
	void onError(Session s,
            Throwable thr) {
		
		 logger.error("onClose: error ws: ", thr);
		 controller.setPrivateConnected(null);
		
    }
	
	
	
	
	

	
}
