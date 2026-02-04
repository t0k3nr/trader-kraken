package org.uche.t0ken.trader.kraken.svc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.uche.t0k3nr.trader.kraken.json.AddOrder;
import org.uche.t0k3nr.trader.kraken.json.BalanceEx;
import org.uche.t0k3nr.trader.kraken.json.BalanceExResponse;
import org.uche.t0k3nr.trader.kraken.json.BalanceExResponse.Balance;
import org.uche.t0k3nr.trader.kraken.json.CancelOrders;
import org.uche.t0k3nr.trader.kraken.json.TradeVolume;
import org.uche.t0k3nr.trader.kraken.json.TradeVolumeResponse;
import org.uche.t0k3nr.trader.kraken.json.WsToken;
import org.uche.t0k3nr.trader.kraken.json.WsTokenResponse;
import org.uche.t0k3nr.trader.kraken.util.KrakenCredentials;
import org.uche.t0ken.api.gdax.Account;
import org.uche.t0ken.api.gdax.Done;
import org.uche.t0ken.api.gdax.MpOrder;
import org.uche.t0ken.api.gdax.OrderType;
import org.uche.t0ken.api.gdax.Reason;
import org.uche.t0ken.api.gdax.Side;
import org.uche.t0ken.api.gdax.TimeInForce;
import org.uche.t0ken.api.gdax.c.Fees;
import org.uche.t0ken.api.util.JsonUtil;
import org.uche.t0ken.commons.enums.OrderStatus;
import org.uche.t0ken.trader.kraken.res.c.KrakenClient;
import org.uche.t0ken.trader.kraken.ws.KrakenWsController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class KrakenService {

	@ConfigProperty(name = "org.uche.t0ken.kraken.pubkey")
	String pubkey;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.base64SecretKey")
	String base64SecretKey;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.product")
	String product;
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.alias")
	String alias;
	
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.fee.query.pair")
	String feeQueryPair;
	
	@ConfigProperty(name = "org.uche.t0ken.trader.kraken.queues.sleep.millis")
	Long queuesSleepMillis;
	
	@ConfigProperty(name = "org.uche.t0ken.trader.kraken.res.threads")
	Integer threads;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.ws.pair")
	String wsProduct;
	
	
	@ConfigProperty(name = "org.uche.t0ken.trader.kraken.debug")
	Boolean debug;
	
	
	@Inject TraderService traderService;
	@Inject KrakenWsController controller;
	
	
	@Inject
    @RestClient
    KrakenClient krakenClient;

	
	
	ArrayDeque<MpOrder> cancelOrderQueue = null;
	ArrayDeque<MpOrder> newOrderQueue = null;
	boolean startedQueues = false;
	Object queuesLock = new Object();
	
	private static final Logger logger = Logger.getLogger("KrakenService");

	private static ExecutorService executor = Executors.newCachedThreadPool();
	
	private String token = null;
	
	public String getToken(boolean renew) {
		if ((token == null) || renew) {
			token = getWebSocketsToken();
		}
		return token;
	}
	
	public String getWebSocketsToken() {
		
		String uriPath = "/0/private/GetWebSocketsToken";
		
		WsToken req = new WsToken(System.currentTimeMillis());
		
		KrakenCredentials creds = new KrakenCredentials(pubkey, base64SecretKey);
		String sign = null;
		try {
			sign = creds.sign(uriPath, "" + req.getNonce(), JsonUtil.serialize(req, false));
		} catch (JsonProcessingException e) {
			logger.error("", e);
		}
		
		WsTokenResponse response = krakenClient.getWSToken(pubkey, sign, req);
		
		
		
		return response.getResult().getToken();
	}
	
	
	
	public Account getAccount(String accountId) {
		
		Account retval = null;
		
		
		String uriPath = "/0/private/BalanceEx";
		
		BalanceEx req = new BalanceEx(System.currentTimeMillis());
		
		KrakenCredentials creds = new KrakenCredentials(pubkey, base64SecretKey);
		String sign = null;
		try {
			sign = creds.sign(uriPath, "" + req.getNonce(), JsonUtil.serialize(req, false));
		} catch (JsonProcessingException e) {
			logger.error("", e);
		}
		
		BalanceExResponse response = krakenClient.getBalanceEx(pubkey, sign, req);
		
		
	
		BigDecimal total = BigDecimal.ZERO;
		BigDecimal available = BigDecimal.ZERO;
		BigDecimal hold = BigDecimal.ZERO;
		
		if (response != null) {
			
			
			
			if ((accountId.equals("BTC")) || (accountId.equals("XBT"))) {
				for (String s: List.of("XXBT", "XXBT.F", "XXBT.S", "XXBT.B", "XBT", "XBT.F", "XBT.S", "XBT.B")) {
					
					
					Balance current = response.getResult().get(s);
					if (current != null) {
						total = total.add(BigDecimal.valueOf(current.getBalance()));
						hold = hold.add(BigDecimal.valueOf(current.getHold_trade()));
					}
					
				}
			} else if ((accountId.equals("USD"))) {
				
				
				for (String s: List.of("USD", "USD.F", "USD.S", "USD.B", "ZUSD", "ZUSD.F", "ZUSD.S", "ZUSD.B")) {
					
					
					Balance current = response.getResult().get(s);
					if (current != null) {
						total = total.add(BigDecimal.valueOf(current.getBalance()));
						hold = hold.add(BigDecimal.valueOf(current.getHold_trade()));
					}
					
					
					
				}
			} else {
				for (String s: List.of(accountId, accountId+ ".F", accountId+ ".S", accountId+ ".B")) {
					Balance current = response.getResult().get(s);
					if (current != null) {
						total = total.add(BigDecimal.valueOf(current.getBalance()));
						hold = hold.add(BigDecimal.valueOf(current.getHold_trade()));
					}
					
				}
			}
			available = total.subtract(hold);
			retval = new Account();
			retval.setAvailable(available);
			retval.setBalance(total);
			retval.setHold(hold);
			retval.setProfile_id(accountId);
			retval.setId(accountId);
			
			if (debug) {
				try {
					logger.info("getAccount: retval: " + JsonUtil.serialize(retval, false));
				} catch (JsonProcessingException e) {
					
				}
			}
		}
		return retval;
	}
	
	
	public Fees getFees() {
		Fees retval = null;
		
		String uriPath = "/0/private/TradeVolume";
		
		TradeVolume req = new TradeVolume(System.currentTimeMillis(), feeQueryPair);
		
		KrakenCredentials creds = new KrakenCredentials(pubkey, base64SecretKey);
		String sign = null;
		try {
			sign = creds.sign(uriPath, "" + req.getNonce(), JsonUtil.serialize(req, false));
		} catch (JsonProcessingException e) {
			logger.error("", e);
		}
		
		TradeVolumeResponse response = krakenClient.getTradeVolume(pubkey, sign, req);
		
		
		try {
			logger.info(JsonUtil.serialize(response, false));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		if (response != null) {
			
			
			retval = new Fees();
			retval.setMaker_fee_rate(response.getResult().getFees_maker().get(feeQueryPair).getFee());
			retval.setTaker_fee_rate(response.getResult().getFees().get(feeQueryPair).getFee());
			retval.setUsd_volume(response.getResult().getVolume());
			
			if (debug) {
				try {
					logger.info("getFees: retval: " + JsonUtil.serialize(retval, false));
				} catch (JsonProcessingException e) {
					
				}
			}
		}
		
			
		
		return retval;
	}
	
	
/*
	public List<MpOrder> listOrders()  {
		List<MpOrder> retval = new ArrayList<MpOrder>();
		
		KrakenAPI apiSec = new KrakenAPI(pubkey, base64SecretKey);
		JsonNode openOrdersResponse = apiSec.query(KrakenAPI.Private.OPEN_ORDERS);
		
		if (debug) logger.info("listOrders: openOrdersResponse: " + openOrdersResponse);
		
		
		if (openOrdersResponse != null) {
			
			JsonNode openOrders = openOrdersResponse.get("open");
			
			if (openOrders != null) {
				Iterator<String> txids = openOrders.fieldNames();
				
				while (txids.hasNext()) {
					String txid = txids.next();
					JsonNode thisOrder = openOrders.get(txid);
					if ((thisOrder != null) && (orderFromThisProduct(thisOrder))) {
						
						MpOrder current = new MpOrder();
						current.setMpUUID(txid);
						if (debug) logger.info("listOrders: txid: " + txid + " " + thisOrder);
						this.copyFromFromResponse(thisOrder, current);
						
						retval.add(current);
					}
				}
			}
			
			
			
			
			
			
		}
		
		if (debug) logger.info("listOrders: total found orders for this product: " + retval.size());
		
		
		return retval;
	}
	
	
	private boolean orderFromThisProduct(JsonNode thisOrder) {
		
		
		JsonNode desc = thisOrder.get("descr");
		
		
		JsonNode pairNode = desc.get("pair");
		
		if (pairNode != null) {
			if ((pairNode.asText().equals(product)) || (pairNode.asText().equals(alias))) {
				return true;
			}
		}
		
		return false;
	}

*/
	/*
	public MpOrder getOrder(String txid)  {
		
		MpOrder retval = null;
		KrakenAPI apiSec = new KrakenAPI(pubkey, base64SecretKey);
		JsonNode orders = apiSec.query(KrakenAPI.Private.QUERY_ORDERS, Map.of(
				"txid", txid,
				"trades", "true" ));
		
		if (orders != null) {
			JsonNode thisOrder = orders.get(txid);
			//logger.info("getOrder: found "+ thisOrder);
			if ((thisOrder != null) && (orderFromThisProduct(thisOrder))) {
				retval = new MpOrder();
				retval.setMpUUID(txid);
				if (debug) logger.info("getOrder: " + thisOrder);
				
				this.copyFromFromResponse(thisOrder, retval);
			} else {
				if (debug) logger.info("getOrder: not considered: " + thisOrder);
				
			}
		}
		
		if (debug) {
			//try {
				if (retval != null) {
					//logger.info("getOrder: returning "+ JsonUtil.serialize(retval, false));
				} else {
					logger.info("getOrder: order not found " + txid);
				}
				
			//} catch (JsonProcessingException e) {
				
			//}
		}
		
		return retval;
	}*/
	
	/*
	public boolean cancelOrder(String txid)  {
		
		MpOrder retval = new MpOrder();
		KrakenAPI apiSec = new KrakenAPI(pubkey, base64SecretKey);
		JsonNode canceled = apiSec.query(KrakenAPI.Private.CANCEL_ORDER, Map.of(
				"txid", txid));
		
		if (canceled != null) {
			JsonNode countNode = canceled.get("count");
			if (countNode != null) {
				
				if (countNode.canConvertToInt()) {
					if (countNode.asInt()>0) {
						logger.info("cancelOrder: canceled order " + txid + " " + canceled.toString());
						return true;
					}
				}
				
				
			}
		}
		
		return false;
	}
	*/
	/*
	public List<Fill> listFills(String txid)  {
		List<Fill> retval = new ArrayList<Fill>();
		
		KrakenAPI apiSec = new KrakenAPI(pubkey, base64SecretKey);
		JsonNode orders = apiSec.query(KrakenAPI.Private.QUERY_ORDERS, Map.of(
				"txid", txid,
				"trades", "true" ));
		
		if (orders != null) {
			JsonNode thisOrder = orders.get(txid);
			if (thisOrder != null) {
				
				JsonNode tradesNode = thisOrder.get("trades");
				
				if (tradesNode != null) {
					for (int i=0; i<tradesNode.size(); i++) {
						String fillTxid = tradesNode.get(i).asText();
						
						JsonNode fills = apiSec.query(KrakenAPI.Private.QUERY_TRADES, Map.of(
								"txid", fillTxid
								));
						
						System.out.println("fills: " + fills);
						
						Iterator<String> txids = fills.fieldNames();
						
						while (txids.hasNext()) {
							String currenttx = txids.next();
							JsonNode thisFill = fills.get(currenttx);
							
							if (thisFill != null) {
								
								JsonNode feeNode = thisFill.get("fee");
								BigDecimal fee = null;
								if (feeNode != null) {
									fee = BigDecimal.valueOf(Double.parseDouble(feeNode.asText()));
								}
								
								
								JsonNode ordertxNode = thisFill.get("ordertxid");
								String ordertx = null;
								if (ordertxNode != null) {
									ordertx = ordertxNode.asText();
								}
								
								
								
								
								JsonNode typeNode = thisFill.get("type");
								Side side = null;
								if (typeNode != null) {
									side = Side.getEnum(typeNode.asText());
								}
								
								JsonNode priceNode = thisFill.get("price");
								BigDecimal price = null;
								if (priceNode != null) {
									price = BigDecimal.valueOf(Double.parseDouble(priceNode.asText()));
								}
								
								JsonNode sizeNode = thisFill.get("vol");
								BigDecimal size = null;
								if (feeNode != null) {
									size = BigDecimal.valueOf(Double.parseDouble(sizeNode.asText()));
								}
								
								JsonNode tradeIdNode = thisFill.get("trade_id");
								Long tradeId = null;
								if (tradeIdNode != null) {
									if (tradeIdNode.isLong()) {
										tradeId = tradeIdNode.asLong();
									}
								}
								
								JsonNode makerNode = thisFill.get("maker");
								Boolean maker = null;
								if (makerNode != null) {
									if (makerNode.isBoolean()) {
										maker = makerNode.asBoolean();
									}
								}
								
								JsonNode timeNode = thisFill.get("time");
								Instant time = null;
								if (timeNode != null) {
									time = this.getFromKrakenTimestamp(thisFill.get("time").asText());
								}
								
								Fill fill = new Fill();
								fill.setCreated_at(time);
								fill.setFee(fee);
								fill.setOrder_id(ordertx);
								fill.setSide(side);
								fill.setTrade_id(tradeId);
								fill.setPrice(price);
								fill.setProduct_id(null);
								fill.setSize(size);
								if (maker) {
									fill.setLiquidity("M");
								} else {
									fill.setLiquidity("T");
								}
								retval.add(fill);								
							}
						}
						
						
						
						
					}
					try {
						logger.info("listFills: returning "+ JsonUtil.serialize(retval, false));
					} catch (JsonProcessingException e) {
						
					}
				}
				
			}
		}
		
		return retval;
	}
	
	*/
	private void copyFromFromResponse(JsonNode orderNode, MpOrder mpOrder) {
		
		
		JsonNode clidNode = orderNode.get("cl_ord_id");
		String cl_ord_id = null;
		if (clidNode != null) {
			cl_ord_id = orderNode.get("cl_ord_id").asText();
			mpOrder.setMyUUID(cl_ord_id);
		}
		
		JsonNode vol = orderNode.get("vol");
		BigDecimal size = null;
		if (vol != null) {
			size = BigDecimal.valueOf(Double.parseDouble(vol.asText()));
		}
		
		JsonNode execvol = orderNode.get("vol_exec");
		BigDecimal execSize = null;
		if (execvol != null) {
			execSize = BigDecimal.valueOf(Double.parseDouble(execvol.asText()));
		}
		
		JsonNode feeNode = orderNode.get("fee");
		BigDecimal fee = null;
		if (feeNode != null) {
			fee = BigDecimal.valueOf(Double.parseDouble(feeNode.asText()));
		}
		
		JsonNode costNode = orderNode.get("cost");
		BigDecimal cost = null;
		if (costNode != null) {
			cost = BigDecimal.valueOf(Double.parseDouble(costNode.asText()));
		}
		
		JsonNode desc = orderNode.get("descr");
		
		
		JsonNode sideNode = desc.get("type");
		Side side = null;
		if (sideNode != null) {
			side = Side.getEnum(sideNode.asText());
		}
		
		
		JsonNode orderTypeNode = desc.get("ordertype");
		OrderType type = null;
		if (orderTypeNode != null) {
			type = OrderType.getEnum(orderTypeNode.asText());
		}
		
		
		JsonNode priceNode = desc.get("price");
		BigDecimal price = null;
		if (priceNode != null) {
			price = BigDecimal.valueOf(Double.parseDouble(priceNode.asText()));
		}
		// pending, open, closed, canceled, expired
		String statusStr = orderNode.get("status").asText();
		OrderStatus status = OrderStatus.ERROR_API;
		if (statusStr != null) {
			if (statusStr.equals("pending")) {
				status = OrderStatus.PENDING;
			} else if (statusStr.equals("open")) {
				status = OrderStatus.OPEN;
			} else if (statusStr.equals("closed")) {
				status = OrderStatus.DONE_FILLED;
			} else if (statusStr.equals("canceled")) {
				status = OrderStatus.DONE_CANCELED;
			} else if (statusStr.equals("expired")) {
				status = OrderStatus.CANCELED_MISSING_FILLS;
			} 
		}
		Instant createdAt = this.getFromKrakenTimestamp(orderNode.get("opentm").asText());
		
		String oflags = orderNode.get("oflags").asText();
		
		mpOrder.setCreatedAt(createdAt);
		mpOrder.setExecutedValue(cost);
		mpOrder.setFilledSize(execSize);
		mpOrder.setFillFees(fee);
		
		if (oflags != null) {
			if (oflags.contains("post")) {
				mpOrder.setPostOnly(true);
			} else {
				mpOrder.setPostOnly(false);
			}
		}
		
		
		mpOrder.setPrice(price);
		mpOrder.setSize(size);
		mpOrder.setSide(side);
		//mpOrder.setProductId(product);
		//mpOrder.setStp(or.getStp());
		mpOrder.setTimeInForce(TimeInForce.GTC);
		mpOrder.setOrderType(type);
		if (oflags != null) {
			if (oflags.contains("viqc")) {
				mpOrder.setFunds(size);
				mpOrder.setSpecifiedFunds(size);
			}
		}
		if (size != null) {
			if (execSize != null) {
				mpOrder.setRemainingSize(size.subtract(execSize));
			} else {
				mpOrder.setRemainingSize(size);
			}
		}
		
		mpOrder.setStatus(status);
		
		/*
		 * {
   "OHDUBV-WMCEV-Y4NKP5":{
      "refid":null,
      "userref":null,
      "cl_ord_id":"52ddc780-a0dc-48b4-b008-e33caf66133f",
      "status":"open",
      "opentm":1.739122628909703E9,
      "starttm":0,
      "expiretm":0,
      "descr":{
         "pair":"SOLUSD",
         "aclass":"forex",
         "type":"sell",
         "ordertype":"limit",
         "price":"300.00",
         "price2":"0",
         "leverage":"none",
         "order":"sell 0.20000000 SOLUSD @ limit 300.00",
         "close":""
      },
      "vol":"0.20000000",
      "vol_exec":"0.00000000",
      "cost":"0.00000",
      "fee":"0.00000",
      "price":"0.00000",
      "stopprice":"0.00000",
      "limitprice":"0.00000",
      "misc":"",
      "oflags":"fciq",
      "reason":null
   }
}
		 */
		
	}
	
	private Instant getFromKrakenTimestamp(String tm) {
		Long opentm = Double.valueOf(Double.parseDouble(tm)*1000000d).longValue();
		Instant opentmTs = Instant.ofEpochSecond(opentm/1000000, (opentm%1000000)*1000);
		return opentmTs;
	}
	/*
	private void copyFromOrderResponse(OrderResponse or, MpOrder mpOrder) {
		mpOrder.setCreatedAt(or.getCreated_at());
		mpOrder.setExecutedValue(or.getExecuted_value());
		mpOrder.setFilledSize(or.getFilled_size());
		mpOrder.setFillFees(or.getFill_fees());
		mpOrder.setMpUUID(or.getId());
		mpOrder.setPostOnly(or.getPost_only());
		mpOrder.setPrice(or.getPrice());
		mpOrder.setSize(or.getSize());
		mpOrder.setSide(or.getSide());
		mpOrder.setProductId(or.getProduct_id());
		mpOrder.setStp(or.getStp());
		mpOrder.setTimeInForce(or.getTime_in_force());
		mpOrder.setOrderType(or.getType());
		mpOrder.setFunds(or.getFunds());
		mpOrder.setSpecifiedFunds(or.getSpecified_funds());
		if (or.getSize() != null) {
			if (or.getFilled_size() != null) {
				mpOrder.setRemainingSize(or.getSize().subtract(or.getFilled_size()));
			} else {
				mpOrder.setRemainingSize(or.getSize());
			}
		}
		
		
		switch (or.getStatus()) {
		case OPEN: {
			mpOrder.setStatus(OrderStatus.OPEN);
			break;
		}
		case RECEIVED: {
			mpOrder.setStatus(OrderStatus.RECEIVED);
			break;
		}
		case PENDING: {
			mpOrder.setStatus(OrderStatus.PENDING);
			break;
		}
		case DONE: {
			if (or.getDone_reason().equals(Reason.CANCELED)) {
				mpOrder.setStatus(OrderStatus.DONE_CANCELED);
				
			} else {
				mpOrder.setStatus(OrderStatus.DONE_FILLED);
			}
			break;
		}
		case ACTIVE:
			default: {
				mpOrder.setStatus(OrderStatus.OPEN);
			break;
		}
		}
	}*/
	/*
	public MpOrder newOrder(MpOrder newOrder) {
		OrderType orderType = newOrder.getOrderType();
		
		KrakenAPI apiSec = new KrakenAPI(pubkey, base64SecretKey);
		
		
		
		if (orderType.equals(OrderType.MARKET)) {
			

			try {
				JsonNode response = null;
				
				if (newOrder.getSide().equals(Side.SELL)) {
					response = apiSec.query(KrakenAPI.Private.ADD_ORDER, Map.of(
							"ordertype", "market", 
							 "type", newOrder.getSide().getName(),
							"volume", newOrder.getSize().toString(),
							"pair", product,
							"cl_ord_id", newOrder.getMyUUID().toString(),
							"oflags", "fciq"
							));
				} else {
					// buy
					response = apiSec.query(KrakenAPI.Private.ADD_ORDER, Map.of(
							"ordertype", "market", 
							 "type", newOrder.getSide().getName(),
							"volume", newOrder.getFunds().toString(),
							"pair", product,
							"cl_ord_id", newOrder.getMyUUID().toString(),
							"oflags", "viqc,fciq"
							));
				}
				
				
				
				if (response != null) {
					
					
					String txid = response.get("txid").get(0).asText();
					newOrder.setMpUUID(txid);
					newOrder.setCreatedAt(Instant.now());
					newOrder.setStatus(OrderStatus.NEW);
					
				}
			} catch (KrakenException ke) {
				newOrder.setStatus(OrderStatus.ERROR_API);
				if ((ke.getMessage().contains("Insufficient funds"))) {
					newOrder.setError(OrderError.NO_FUNDS);
				} else {
					newOrder.setError(OrderError.INVALID_PARAMS);
				}
				logger.error("newOrder: response status: ", ke);
			}
			
			
		} else { //LIMIT
			
			
			try {
				
				JsonNode response = null;
				if (newOrder.getPostOnly()) {
					response = apiSec.query(KrakenAPI.Private.ADD_ORDER, Map.of(
							"ordertype", "limit", 
							 "type", newOrder.getSide().getName(),
							"volume", newOrder.getSize().toString(),
							"pair", product,
							"price", newOrder.getPrice().toString(),
							"cl_ord_id", newOrder.getMyUUID().toString(),
							"oflags", "post"//"post,fciq"
							));
				} else {
					response = apiSec.query(KrakenAPI.Private.ADD_ORDER, Map.of(
							"ordertype", "limit", 
							 "type", newOrder.getSide().getName(),
							"volume", newOrder.getSize().toString(),
							"pair", product,
							"price", newOrder.getPrice().toString(),
							"cl_ord_id", newOrder.getMyUUID().toString(),
							"oflags", "post"//"post,fciq"
							));
				}
				
				
				
				if (response != null) {
					
					
					String txid = response.get("txid").get(0).asText();
					newOrder.setMpUUID(txid);
					newOrder.setCreatedAt(Instant.now());
					newOrder.setStatus(OrderStatus.NEW);
					
					
				}
			} catch (KrakenException ke) {
				newOrder.setStatus(OrderStatus.ERROR_API);
				if ((ke.getMessage().contains("Insufficient funds"))) {
					newOrder.setError(OrderError.NO_FUNDS);
				} else {
					newOrder.setError(OrderError.INVALID_PARAMS);
				}
				logger.error("newOrder: response status: ", ke);
			}
			
			
			
		}
		
		return newOrder;
	}*/
	
	public void spoolCancelOrder(MpOrder order) {
		
		boolean res;
		synchronized(cancelOrderQueue) {
			res = cancelOrderQueue.offer(order);
		}
		
		
		logger.warn("spoolCancelOrder: " + order.getMpUUID());
		
		if (!res) {
			logger.error("spoolCancelOrder: Queue full " + order.getMpUUID());
		}
	}
	
	public void spoolNewOrder(MpOrder order) {
		boolean res;
		synchronized(newOrderQueue) {
			res = newOrderQueue.offer(order);
		}
		
		if (!res) {
			logger.error("spoolNewOrder: Queue full " + order.getMpUUID());
		}
	}
	
	//@Override
	
	public void setStarted() {
		startedQueues = true;
	}
	
	public void startQueues() {
		
		
		
		queueUUIDs = new HashSet<UUID>();
		
		cancelOrderQueue = new ArrayDeque<MpOrder>(100);
		newOrderQueue = new ArrayDeque<MpOrder>(100);
		
		
		for (int i = 0; i< threads; i++) {
			UUID u = UUID.randomUUID();
			queueUUIDs.add(u);
			logger.info("startQueues: starting thread #" + i + " " + u);
			Uni.createFrom().item(u).emitOn(executor).subscribe().with(
	                this::startQueues, Throwable::printStackTrace
	        );
			
		}
		
		
		
		
	}

	
	
	private Uni<Void>  startQueues(UUID uuid) {
		
		if (debug) logger.info("startQueues: " + uuid);
		
		synchronized (queuesLock) {
			try{
				queuesLock.wait(queuesSleepMillis);
			} catch(InterruptedException e){
				logger.error("startQueues: interrupted " + uuid);
			}

		}
		
		
		while (startedQueues) {
			try {
				
				
				
				//if (debug) logger.info("startQueues: " + uuid);
				processQueues();
				
				
				synchronized (queuesLock) {
					try{
						queuesLock.wait(queuesSleepMillis);
					} catch(InterruptedException e){
						logger.error("startQueues: interrupted " + uuid);
					}

				}
			}
				
			catch (Exception e) {
				
				logger.error("startQueues: non fatal error " + uuid, e);
				synchronized (queuesLock) {
					try{
						queuesLock.wait(queuesSleepMillis);
					} catch(InterruptedException e1){
						logger.error("startQueues: Exception " + uuid, e1);
					}

				}
			}
			
		
		}
		
		queueUUIDs.remove(uuid);
		
		if (debug) logger.info("startQueues: exiting " + uuid);
		return Uni.createFrom().voidItem();
	}

	
	

	private void processQueues() {
		/*
		 * First, cancel queue
		 */
		Object lockObj = new Object();
		MpOrder order = null;
		
		synchronized (cancelOrderQueue) {
			order = cancelOrderQueue.poll();
		}
		
		
		
		while (order != null) {
			long now = System.currentTimeMillis();
			
			boolean cancelOrder = false;
			boolean fakeCancel = false;
			
			synchronized (order) {
				
				switch (order.getStatus()) {
				
					case NEW: {
						if ((order.getCanceledAt() == null) && (!order.isMpCreateRequested())) {
							fakeCancel = true;	
							
							logger.warn("processQueues: order.isMpCreateRequested(): " + order.isMpCreateRequested() + " order.getCanceledAt(): " + order.getCanceledAt() + " order.getBuiltAt(): " + order.getBuiltAt() + " my: " + order.getMyUUID() + " setting fake to true");

						} else {
							logger.warn("processQueues: order.isMpCreateRequested(): " + order.isMpCreateRequested() + " order.getCanceledAt(): " + order.getCanceledAt() + " order.getBuiltAt(): " + order.getBuiltAt() + " cannot cancel my: " + order.getMyUUID() + " fake set to false");
						}
						break;
					}
					
					case OPEN:
					case PENDING:
					case RECEIVED:
					default: {
						
						if ((order.getCanceledAt() == null) && (order.getDoneAt() == null) && (!order.isMpCreateRequested())) {
							
							switch (order.getOrderType()) {
							
							case LIMIT:
							case STOP: {
								if (order.getRemainingSize().signum()>0) {
									cancelOrder = true;
									
								}
								break;
							}
							default: {
								
							}
							}
							
						}  else {
							logger.warn("processQueues: order.getCanceledAt() not null, cannot cancel mp: " + order.getMpUUID());
						}

						break;
					}
				}
			}
			
			if (cancelOrder) {
				try {
					order.setMpCancelRequested(true);
					cancelOrders(List.of(order.getMpUUID()));
					order.setCanceledAt(Instant.now());
				} catch (Exception e) {
					logger.warn("processQueues: exception canceling order, un-canceling... " + order.getMpUUID(), e);
					
					order.setMpCancelRequested(false);
					
				}
				/*if (!retval) {
					// not cancelled, probably order already done
					logger.warn("processQueues: error cancelling order " + order.getMpUUID());
					Done done = new Done();
					done.setOrder_id(order.getMpUUID());
					done.setReason(Reason.CANCELED);
					done.setRemaining_size(order.getRemainingSize());
					traderService.o_doneOrder(done);
				} */
				
				long duration = System.currentTimeMillis() - now;
				if (duration < 1000l) {
					synchronized (lockObj) {
						try {
							lockObj.wait(1000l - duration);
						} catch (InterruptedException e) {
							logger.error("processQueues: ", e);
						}
					}
				} else {
					logger.warn("processQueues: cancelOrder took " + duration + "ms");
				}
			} 
			
			if (fakeCancel) {
				Done done = new Done();
				done.setOrder_id(order.getMyUUID());
				done.setReason(Reason.CANCELED);
				done.setRemaining_size(order.getRemainingSize());
				done.setSide(order.getSide());
				logger.warn("processQueues: fakeCancel: " + order.getMyUUID() + " order.getCanceledAt(): " + order.getCanceledAt() + " order.getBuiltAt(): " + order.getBuiltAt());

				traderService.o_doneOrder(done);

			}
			
			synchronized (cancelOrderQueue) {
				order = cancelOrderQueue.poll();
			}
			
			
		}
		
		/*
		 * Then newOrder queue
		 */
		
		synchronized (newOrderQueue) {
			order = newOrderQueue.poll();
		}
		
		while (order != null) {
			
			long now = System.currentTimeMillis();
			MpOrder result = null;
			boolean executeOrder = false;
			
			synchronized (order) {
				switch (order.getStatus()) {
				case NEW: {
					if ( (order.getCanceledAt() == null) && (!order.isMpCancelRequested())) {
						// order not canceled, processing
						order.setMpCreateRequested(true);
						order.setCreatedAt(Instant.now());
						executeOrder = true;
						logger.warn("processQueues: order will be executed " + order.getMyUUID() + " status: " + order.getStatus());

					} else {
						logger.warn("processQueues: order cannot be executed " + order.getMyUUID() + " status: " + order.getStatus() + " canceledAt: " + order.getCanceledAt());

					}
					break;
				}
				default: {
					logger.warn("processQueues: order cannot be executed " + order.getMyUUID() + " status: " + order.getStatus() + " canceledAt: " + order.getCanceledAt());
				}
				}
			}
			
			if (executeOrder) {
				try {
					this.sendOrder(order);
					//order.setMpCreateRequested(false);
					
					/*
					if (result.getStatus().equals(OrderStatus.ERROR_API)) {
						logger.warn("processQueues: order failed: " + order.getMyUUID() + " calling o_doneOrder to release funds. Error msg: " + result.getError());
						Done done = new Done();
						done.setReason(Reason.CANCELED);
						done.setOrder_id(order.getMyUUID());
						done.setRemaining_size(order.getRemainingSize());
						synchronized (order) {
							order.setCanceledAt(Instant.now());
						}
						traderService.o_doneOrder(done);
					} else {
						if (result.getMpUUID() != null) {
							synchronized (order) {
								order.setMpCreateRequested(false); // order is now cancelable
							}
						}
					}*/
					
				} catch (Exception e) {
					logger.warn("processQueues: exception creating order " + order.getMyUUID() + ", calling o_doneOrder to release funds. ", e);
					Done done = new Done();
					done.setOrder_id(order.getMyUUID());
					done.setReason(Reason.CANCELED);
					done.setRemaining_size(order.getRemainingSize());
					done.setSide(order.getSide());
					synchronized (order) {
						order.setCanceledAt(Instant.now());
					}
					traderService.o_doneOrder(done);
				}
			}
			
			long duration = System.currentTimeMillis() - now;
			if (duration < 1000l) {
				synchronized (lockObj) {
					try {
						lockObj.wait(1000l - duration);
					} catch (InterruptedException e) {
						logger.error("processQueues: ", e);
					}
				}
			} else {
				logger.warn("processQueues: newOrder took " + duration + "ms");
			}
			
			synchronized (newOrderQueue) {
				order = newOrderQueue.poll();
			}
			
			
		}
		
		
	}



	//@Override
	
	private static Object allQueuesStoppingLockObj = new Object();
	
	private static Set<UUID> queueUUIDs = null;
	
	public void stopQueues() {
		
		startedQueues = false;
		
		int queueSize = 0;
		
		
		while(true) {
			synchronized (queueUUIDs) {
				queueSize = queueUUIDs.size();
			}
			if (queueSize == 0) {
				break;
			}
			synchronized (queuesLock) {
				queuesLock.notifyAll();
			}
			logger.info("stopQueues: waiting for all queues to finish...");
			synchronized (allQueuesStoppingLockObj) {
				try {
					allQueuesStoppingLockObj.wait(1000l);
				} catch (Exception e) {
					
				}
			}
		}
		
		
		
		
		
		logger.warn("stopQueues: notified");
		
	}

	
	public void notifyQueues() {
		synchronized (queuesLock) {
			queuesLock.notify();
		}
	}
	
	public void sendOrder(MpOrder order) throws Exception {
		
		order.setProductName(wsProduct);
		AddOrder addOrder = new AddOrder(order, getToken(false));
		controller.spoolPrivateWsMessage(addOrder);
		
	}
	
	
	public void cancelOrders(List<String> orderIds) throws Exception {
		
		CancelOrders cancelOrders = new CancelOrders(orderIds, getToken(false));
		controller.spoolPrivateWsMessage(cancelOrders);
		
	}
	
}
