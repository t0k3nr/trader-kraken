package org.uche.t0ken.trader.kraken.svc;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.graalvm.collections.Pair;
import org.jboss.logging.Logger;
import org.uche.t0ken.api.gdax.Change;
import org.uche.t0ken.api.gdax.Confirmed;
import org.uche.t0ken.api.gdax.Data;
import org.uche.t0ken.api.gdax.Done;
import org.uche.t0ken.api.gdax.L2Change;
import org.uche.t0ken.api.gdax.L2Update;
import org.uche.t0ken.api.gdax.Match;
import org.uche.t0ken.api.gdax.MpOrder;
import org.uche.t0ken.api.gdax.Open;
import org.uche.t0ken.api.gdax.Order;
import org.uche.t0ken.api.gdax.OrderType;
import org.uche.t0ken.api.gdax.Received;
import org.uche.t0ken.api.gdax.Restored;
import org.uche.t0ken.api.gdax.Side;
import org.uche.t0ken.api.gdax.Snapshot;
import org.uche.t0ken.api.gdax.TimeInForce;
import org.uche.t0ken.api.trader.Position;
import org.uche.t0ken.api.trader.Signal;
import org.uche.t0ken.api.util.JsonUtil;
import org.uche.t0ken.commons.enums.Action;
import org.uche.t0ken.commons.enums.ActionOrderType;
import org.uche.t0ken.commons.enums.OrderStatus;
import org.uche.t0ken.commons.util.Indicators;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;





@ApplicationScoped
public class TraderService {

	@ConfigProperty(name = "org.uche.t0ken.kraken.product")
	String product;
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.quote_increment")
	BigDecimal productQuoteIncrement;
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.quote_scale")
	Integer productQuoteScale;
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.base_currency")
	String productBaseCurrency;
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.base_min_size")
	BigDecimal productBaseMinSize;
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.base_max_size")
	BigDecimal productBaseMaxSize;
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.base_scale")
	Integer productBaseScale;
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.quote_currency")
	String productQuoteCurrency;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.base_account_rounding_size")
	Integer baseAccountRoundingSize;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.quote_account_rounding_size")
	Integer quoteAccountRoundingSize;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.base_account_rounding_scale")
	Integer baseAccountRoundingScale;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.quote_account_rounding_scale")
	Integer quoteAccountRoundingScale;
	
	@ConfigProperty(name = "org.uche.t0ken.trader.kraken.debug")
	Boolean debug;
	
	
	@ConfigProperty(name = "org.uche.t0ken.trading.trader.signal.maintain.after.secs")
	Integer signalMaintainAfterSeconds;
	
	@ConfigProperty(name = "org.uche.t0ken.trading.trader.order.take.obmaxpercent")
	BigDecimal orderTakeObMaxPercent;
	
	@ConfigProperty(name = "org.uche.t0ken.trading.trader.order.mode")
	ActionOrderType orderType;
	
	@ConfigProperty(name = "org.uche.t0ken.trading.trader.order.make.maker_chunks")
	Integer makerNumberOfChunks;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.book.depth")
	Integer bookDepth;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.product.ws.pair")
	String wsProduct;
	
	
	@Inject KrakenService krakenService;
	@Inject AccountService accountService;
	
	
	/*
	 * OB
	 */
	
	private volatile static NavigableMap<BigDecimal, BigDecimal> orderBookBuyOrders = new TreeMap<BigDecimal, BigDecimal>();
	private volatile static NavigableMap<BigDecimal, BigDecimal> orderBookSellOrders = new TreeMap<BigDecimal, BigDecimal>();
	
	private volatile static BigDecimal bid;
	private volatile static BigDecimal bidSize;
	
	private volatile static BigDecimal _bid;
	private volatile static BigDecimal _bidSize;
	
	private volatile static Instant lastBidTime;
	private volatile static BigDecimal ask;
	private volatile static BigDecimal askSize;
	
	private volatile static BigDecimal _ask;
	private volatile static BigDecimal _askSize;
	
	private volatile static Instant lastAskTime;
	private volatile static BigDecimal midMarketPrice;
	

	private ArrayDeque<Data> obDataQueue = new ArrayDeque<Data>(1024);
	
	private static boolean obQueueProcessorRunning = false;
	private static boolean startedObQueueProcessor = false;
	private static Object obQueueProcessorLockObj = new Object();
	
	
	/*
	 * TR
	 */
	
	private static Random random = new Random();

	
	
	protected volatile static Map<String, MpOrder> traderSellOrders = new ConcurrentHashMap<String, MpOrder>();
	protected volatile static Map<String, MpOrder> traderBuyOrders = new ConcurrentHashMap<String, MpOrder>();
	
	protected volatile static BigDecimal exchangedBase = BigDecimal.ZERO;
	protected volatile static BigDecimal exchangedQuote = BigDecimal.ZERO;
	
	protected volatile static BigDecimal profitExchangedBase = BigDecimal.ZERO;
	protected volatile static BigDecimal profitExchangedQuote = BigDecimal.ZERO;
	
	protected volatile static BigDecimal lossExchangedBase = BigDecimal.ZERO;
	protected volatile static BigDecimal lossExchangedQuote = BigDecimal.ZERO;
	
	
	
	private volatile static Object exchangedLockObj = new Object();
	
	
	private static final Logger logger = Logger.getLogger("TraderService");

	
	private static ExecutorService executor = Executors.newCachedThreadPool();
	
	
	
	private ArrayDeque<Data> trDataQueue = new ArrayDeque<Data>(1024);
	
	private static boolean trQueueProcessorRunning = false;
	private static boolean startedTrQueueProcessor = false;
	private static Object trQueueProcessorLockObj = new Object();
	
	
	
	/*
	 * OB
	 */
	
	public void queueObEvent(Data data) {
		synchronized (obDataQueue) {
			obDataQueue.add(data);
		}
	}

	@Transactional(value=TxType.NEVER)
	public void startObQueueProcessor() {
    	
		Uni.createFrom().item(UUID::randomUUID).emitOn(executor).subscribe().with(
                this::startObQueueProcessor, Throwable::printStackTrace
        );
	}
	
	
	public void setStartedObQueueProcessor() {
		startedObQueueProcessor = true;
	}
	
	public void setStartedTrader() {
		startedTrader = true;
		
	}
	
	@Transactional(value=TxType.NEVER)
	public Uni<Void> startObQueueProcessor(UUID uuid) {
		
		logger.info("startObQueueProcessor: starting... @" + Instant.now() + " startedQueueProcessor: " + startedObQueueProcessor);
		
		obQueueProcessorRunning = true;
		
		while (startedObQueueProcessor) {
			try {
				Data data = null;
				int dequeSize = 0;
				synchronized (obDataQueue) {
					try {
						data = obDataQueue.poll();
						dequeSize = obDataQueue.size();
					} catch (Exception e) {
						
					}
				}
				
				if (data != null) {
					long t = System.currentTimeMillis();
					
					obData(data);
					
					//logger.info("startQueueProcessor: processing match: done " + (System.currentTimeMillis() - t) + "ms"); 
					
				} else {
					synchronized (obQueueProcessorLockObj) {
						try {
							obQueueProcessorLockObj.wait(3600000l);
						} catch (Exception e) {
							
						}
					}
				}
			} catch (Exception e) {
				logger.error("startObQueueProcessor: ", e);
			}
			
		}
		obQueueProcessorRunning = false;
		logger.info("startObQueueProcessor: exiting... done @" + Instant.now());
		
		return Uni.createFrom().voidItem();
	}
	
	
	private void obData(Data data) {
		
		if (data instanceof Snapshot) {
			
			if (debug) {
				logger.info("obData: Snapshot");
			}
			
			Snapshot snapshot = (Snapshot) data;
			
			
			
			
			orderBookBuyOrders = new TreeMap<BigDecimal, BigDecimal>();
			orderBookSellOrders = new TreeMap<BigDecimal, BigDecimal>();
			
			List<Order> askOrders = snapshot.getAsks();
			List<Order> bidOrders = snapshot.getBids();
			
			synchronized (orderBookSellOrders) {
				for (Order order: askOrders) {
					
					orderBookSellOrders.put(order.getPrice(), order.getSize());
				}
			}
			
			synchronized(orderBookBuyOrders) {
				for (Order order: bidOrders) {
					orderBookBuyOrders.put(order.getPrice(), order.getSize());
				}
			}
			
			updateBidAsk();
			
			
		} else if (data instanceof L2Update) {
			
			L2Update l2update = (L2Update)data;
			boolean changedSpread = false;
			
			for (L2Change change: l2update.getChanges()) {
				BigDecimal changedPrice = change.getPrice();
				
				if (change.getSide().equals(Side.BUY)) {
					
					if (debug) {
						BigDecimal firstBuyKey = null;
						synchronized (orderBookBuyOrders) {
							firstBuyKey = orderBookBuyOrders.lastKey();
							if (changedPrice.compareTo(firstBuyKey) == 0) changedSpread = true;
						}
						
						
					}
					
					synchronized (orderBookBuyOrders) {
						if (change.getSize().compareTo(BigDecimal.ZERO) == 0) {
							orderBookBuyOrders.remove(changedPrice);
						} else {
							orderBookBuyOrders.put(changedPrice, change.getSize());
						}
						
						
					}
					
					//BigDecimal currentBid = thisProductBuyOrders.lastKey();
					
				} else {
					
					
					if (debug) {
						BigDecimal firstSellKey = null;
						synchronized (orderBookSellOrders) {
							firstSellKey = orderBookSellOrders.firstKey();
							if (changedPrice.compareTo(firstSellKey) == 0) changedSpread = true;
						}
						
						
					}
					
					
					synchronized (orderBookSellOrders) {
						
						if (change.getSize().compareTo(BigDecimal.ZERO) == 0) {
							orderBookSellOrders.remove(changedPrice);
						} else {
							orderBookSellOrders.put(changedPrice, change.getSize());
						}
					}
					
					
					
				}
				
				synchronized (orderBookBuyOrders) {
					while (orderBookBuyOrders.size() > bookDepth) {
						orderBookBuyOrders.remove(orderBookBuyOrders.keySet().iterator().next());
					}
				}
				synchronized (orderBookSellOrders) {
					while (orderBookSellOrders.size() > bookDepth) {
						orderBookSellOrders.remove(orderBookSellOrders.descendingKeySet().iterator().next());
					}
				}
				
				
			}
			updateBidAsk();
			/*if (debug && changedSpread) {
				this.printOrderBooks();		
			}*/
		}
	}

	
	private void u_bid() {
		
		// bid
		int boSize = 0;
		synchronized (traderBuyOrders) {
			boSize = traderBuyOrders.size();
		}
		if (boSize == 0) {
			_bid = bid;
			//logger.info("u_bid: no buy order, bid: " + bid + " _bid: " + _bid);
		} else {
			
			NavigableMap<BigDecimal, BigDecimal> orderBookSubsetBuyOrders = new TreeMap<BigDecimal, BigDecimal>();
			NavigableMap<BigDecimal, BigDecimal> myBuyOrders = new TreeMap<BigDecimal, BigDecimal>();
			
			synchronized (traderBuyOrders) {
				for (MpOrder o: traderBuyOrders.values()) {
					myBuyOrders.put(o.getPrice(), o.getRemainingSize());
				}
			}
			
			if (myBuyOrders.lastKey().compareTo(bid)<0) {
				_bid = bid;
				//logger.info("u_bid: myBuyOrders.lastKey(): " + myBuyOrders.lastKey() + ", bid: " + bid + " _bid: " + _bid);
			} else {
				synchronized (orderBookBuyOrders) {
					for (BigDecimal c: orderBookBuyOrders.descendingKeySet()) {
						
						orderBookSubsetBuyOrders.put(c, orderBookBuyOrders.get(c));
						
						if (c.compareTo(myBuyOrders.firstKey())<0) {
							break;
						}
						
					}
				}
				
				//logger.info("u_bid: orderBookSubsetBuyOrders: " + orderBookSubsetBuyOrders.toString() + " myBuyOrders: " + myBuyOrders.toString());

				
				// now orderBookSubsetBuyOrders contains what we need
				// adjusting subset
				for (BigDecimal p: myBuyOrders.keySet()) {
					BigDecimal s = myBuyOrders.get(p);
					
					if (orderBookSubsetBuyOrders.get(p) != null) {
						orderBookSubsetBuyOrders.put(p, orderBookSubsetBuyOrders.get(p).subtract(s));
					}
					
					
				}
				
				//logger.info("u_bid: adjusted orderBookSubsetBuyOrders: " + orderBookSubsetBuyOrders.toString());

				
				// now get real bid
				boolean found = false;
				for (BigDecimal p: orderBookSubsetBuyOrders.descendingKeySet()) {
					BigDecimal s = orderBookSubsetBuyOrders.get(p);
					if (s.signum()>=0) {
						found = true;
						_bid = p;
						//logger.info("u_bid: after adjusted, bid: " + bid + " _bid: " + _bid);

						break;
					}
				}
				if (!found) {
					_bid = orderBookSubsetBuyOrders.firstKey();
					//logger.info("u_bid: last chance, bid: " + bid + " _bid: " + _bid);
				}
			}
			
			
			
			
		}
		
	}
	
	
	private void u_ask() {
		
		// bid
		int soSize = 0;
		synchronized (traderSellOrders) {
			soSize = traderSellOrders.size();
		}
		if (soSize == 0) {
			_ask = ask;
			
			//logger.info("u_ask: no sell order, ask: " + ask + " _ask: " + _ask);

		} else {
			
			NavigableMap<BigDecimal, BigDecimal> orderBookSubsetSellOrders = new TreeMap<BigDecimal, BigDecimal>();
			NavigableMap<BigDecimal, BigDecimal> mySellOrders = new TreeMap<BigDecimal, BigDecimal>();
			
			synchronized (traderSellOrders) {
				for (MpOrder o: traderSellOrders.values()) {
					mySellOrders.put(o.getPrice(), o.getRemainingSize());
				}
			}
			
			if (mySellOrders.firstKey().compareTo(ask)>0) {
				_ask = ask;
				//logger.info("u_ask: mySellOrders.firstKey(): " + mySellOrders.firstKey() + ", ask: " + ask + " _ask: " + _ask);
			} else {
				synchronized (orderBookSellOrders) {
					for (BigDecimal c: orderBookSellOrders.keySet()) {
						
						orderBookSubsetSellOrders.put(c, orderBookSellOrders.get(c));
						
						if (c.compareTo(mySellOrders.lastKey())>0) {
							break;
						}
						
					}
				}
				
				//logger.info("u_ask: orderBookSubsetSellOrders: " + orderBookSubsetSellOrders.toString() + " mySellOrders: " + mySellOrders.toString());

				for (BigDecimal p: mySellOrders.keySet()) {
					BigDecimal s = mySellOrders.get(p);
					if (orderBookSubsetSellOrders.get(p) != null) {
						orderBookSubsetSellOrders.put(p, orderBookSubsetSellOrders.get(p).subtract(s));
					}
					
				}
				
				//logger.info("u_ask: adjusted orderBookSubsetSellOrders: " + orderBookSubsetSellOrders.toString());

				// now get real ask
				boolean found = false;
				for (BigDecimal p: orderBookSubsetSellOrders.keySet()) {
					BigDecimal s = orderBookSubsetSellOrders.get(p);
					if (s.signum()>=0) {
						found = true;
						_ask = p;
						//logger.info("u_ask: after adjusted, ask: " + ask + " _ask: " + _ask);

						break;
					}
				}
				if (!found) {
					_ask = orderBookSubsetSellOrders.lastKey();
					
					//logger.info("u_ask: last chance, ask: " + ask + " _ask: " + _ask);
				}
				
			}
			
			
			
		}
		
	}


	private void updateBidAsk() {
		
		
		boolean change = false;
		
		
		BigDecimal localBid = null;
		
		synchronized (orderBookBuyOrders) {
			localBid = orderBookBuyOrders.lastKey();
			if ((localBid == null) || (bid == null) || (localBid.compareTo(bid) != 0)) {
				bid = localBid.setScale(productQuoteScale, RoundingMode.HALF_EVEN);
				bidSize = orderBookBuyOrders.get(bid).setScale(productBaseScale, RoundingMode.HALF_EVEN);
				change = true;
				
				
			} else {
				BigDecimal localBidSize = orderBookBuyOrders.get(bid);
				if (localBidSize.compareTo(bidSize) != 0) {
					bidSize = localBidSize.setScale(productBaseScale, RoundingMode.HALF_EVEN);
					change = true;
					
				}
			}
		}
		
		u_bid();
		BigDecimal localAsk = null;
		
		synchronized (orderBookSellOrders) {
			localAsk = orderBookSellOrders.firstKey();
			
			if ((localAsk == null) || (ask == null) || (localAsk.compareTo(ask) != 0)) {
				ask = localAsk.setScale(productQuoteScale, RoundingMode.HALF_EVEN);
				askSize = orderBookSellOrders.get(ask).setScale(productBaseScale, RoundingMode.HALF_EVEN);
				change = true;
				
				
			} else {
				BigDecimal localAskSize = orderBookSellOrders.get(ask);
				if (localAskSize.compareTo(askSize) != 0) {
					askSize = localAskSize.setScale(productBaseScale, RoundingMode.HALF_EVEN);
					change = true;
					
				}
			} 
		}
		
		u_ask();
		
		
		if (change) {
			
			midMarketPrice = (ask.add(bid)).divide(new BigDecimal(2), Indicators.roundIndicators);
			
		
		}
		
	}

	public void notifyObQueueProcessorLockObj() {
		synchronized (obQueueProcessorLockObj) {
			try {
				obQueueProcessorLockObj.notifyAll();
			} catch (Exception e) {
				
			}
		}
	}

	public Instant getObInstantOfFirstObjectInQueue() {
		Data data = obDataQueue.peekFirst();
		if (data != null) {
			return Instant.now();
		} else {
			return null;
		}
	}

	
	public void printOrderBooks() {
		logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		logger.info("@ Product: " + product);
		if (orderBookSellOrders != null) {
			synchronized (orderBookSellOrders) {
				Set<BigDecimal> keys = orderBookSellOrders.navigableKeySet();
				BigDecimal accum = BigDecimal.ZERO;
				synchronized (keys) {
					int i = 0;
					List<String> list = new ArrayList<String>(30);
					BigDecimal firstKey = null;
					BigDecimal diff = null;
					
					for (BigDecimal key: keys) {
						if (firstKey == null) firstKey = key;
						accum = accum.add(orderBookSellOrders.get(key));
						
						diff = key.subtract(firstKey);
						list.add("@" + product + ": Sell: price: " + key + " size: " + orderBookSellOrders.get(key) + " accum: " + accum + " diff: " + diff);
						i++;
						if (i>1) break;
						//if (diff.divide(firstKey, Indicators.roundIndicators).compareTo(new BigDecimal(0.0025d)) > 0) break;
					}
					for (i =list.size()-1;i>=0; i--) {
						logger.info(list.get(i));
					}
				}
				
			}
		}
		if (orderBookBuyOrders != null) {
			synchronized (orderBookBuyOrders) {
				Set<BigDecimal> keys = orderBookBuyOrders.descendingKeySet();
				BigDecimal accum = BigDecimal.ZERO;
				synchronized (keys) {
					int i = 0;
					BigDecimal firstKey = null;
					BigDecimal diff = null;
					
					for (BigDecimal key: keys) {
						if (firstKey == null) firstKey = key;
						accum = accum.add(orderBookBuyOrders.get(key));
						diff = firstKey.subtract(key);
						logger.info("@" + product + ":  Buy: price: " + key + " size: " + orderBookBuyOrders.get(key) + " accum: " + accum + " diff: " + diff);
						i++;
						if (i>1) break;
						//if (diff.divide(firstKey, Indicators.roundIndicators).compareTo(new BigDecimal(0.0025d)) > 0) break;
					}
				}
				
			}
		}
		int size = 0;
		synchronized (obDataQueue) {
			size = obDataQueue.size();
		}
		logger.info("@" + product + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " + size);
		
	}

	
	private static Object stopObQueueProcessorLockObj = new Object();
	
	public void stopObQueueProcessor() {
		startedObQueueProcessor = false;
		notifyObQueueProcessorLockObj();
		
		while (isObQueueProcessorRunning()) {
			notifyObQueueProcessorLockObj();
			synchronized (stopObQueueProcessorLockObj) {
				try {
					logger.info("stopObQueueProcessor: waiting for queueProcessor to finish...");
					stopObQueueProcessorLockObj.wait(100l);
				} catch (Exception e) {
					
				}
			}
		}
		
		obDataQueue.clear();
	}
	
	public static boolean isObQueueProcessorRunning() {
		return obQueueProcessorRunning;
	}
	
	public BigDecimal getBid() {
		return bid;
	}
	
	public BigDecimal getAsk() {
		return ask;
	}
	
	public BigDecimal getMidMarketPrice() {
		return midMarketPrice;
	}
	
	
	/*
	 * TR
	 */
	
	public void queueTrEvent(Data data) {
		synchronized (trDataQueue) {
			try {
				logger.info("queueTrEvent: adding " + JsonUtil.serialize(data, false));
			} catch (JsonProcessingException e) {
				
			}

			trDataQueue.add(data);
		}
		
		synchronized (traderLockObj) {
			traderLockObj.notify();
		}
	}

	
	private void o_receivedOrder(Received received) {
		String myUUID = received.getClient_oid();
		String mpUUID = received.getOrder_id();
		
		MpOrder current = null;
		
		if (received.getSide().equals(Side.BUY)) {
			
			synchronized (traderBuyOrders) {
				if (myUUID != null) {
					current = traderBuyOrders.get(myUUID);
					if (current != null) {
						traderBuyOrders.remove(myUUID);
						current.setMpUUID(mpUUID);
						traderBuyOrders.put(mpUUID, current);
					}
				}
				if (current == null) {
					current = traderBuyOrders.get(mpUUID);
				}
				
				if ((current == null) ) {
					
					if (received.getProductName().equals(wsProduct) && (received.getClient_oid() == null)) {
						current = new MpOrder();
						
						current.setMpUUID(received.getOrder_id());
						current.setOrderType(received.getOrder_type());
						current.setPrice(received.getPrice());
						current.setPostOnly(received.getPost_only());
						//current.setFunds(received.getFunds());
						current.setSpecifiedFunds(received.getFunds());
						current.setSide(received.getSide());
						current.setSize(received.getSize());
						current.setStatus(OrderStatus.RECEIVED);
						current.setExecutedValue(BigDecimal.ZERO);
						current.setFilledSize(BigDecimal.ZERO);
						current.setFillFees(BigDecimal.ZERO);
						current.setRemainingSize(current.getSize());
						current.setFeeInQuote(received.getFeeInQuote());
						current.setCreatedAt(received.getTime());
						current.setTimeInForce(received.getTimeInForce());
						accountService.o_lockFundsForNewOrder(current);
						
						traderBuyOrders.put(mpUUID, current);
						
						logger.warn("o_receivedOrder: BUY: RECEIVED TP created order: " + received.getOrder_id());
					} else {
						logger.warn("o_receivedOrder: BUY: ignoring order: " + received.getOrder_id() + " for product " + received.getProductName());
					}
					
						
					
				} else {
					logger.warn("o_receivedOrder: BUY: RECEIVED: " + current.getMyUUID() + " " + received.getOrder_id());	
				}
					
					
				if (current != null) current.setMpCreateRequested(false); // now cancelable
					
			} 
			
			
		} else {
			synchronized (traderSellOrders) {
				
				if (myUUID != null) {
					current = traderSellOrders.get(myUUID);
					if (current != null) {
						traderSellOrders.remove(myUUID);
						current.setMpUUID(mpUUID);
						traderSellOrders.put(mpUUID, current);
					}
				}
				
				if (current == null) {
					current = traderSellOrders.get(mpUUID);
				}
				
				if (current == null) {
					if (received.getProductName().equals(wsProduct) && (received.getClient_oid() == null)) {
						current = new MpOrder();
						
						current.setMpUUID(received.getOrder_id());
						current.setOrderType(received.getOrder_type());
						current.setPrice(received.getPrice());
						current.setPostOnly(received.getPost_only());
						//current.setFunds(received.getFunds());
						current.setSpecifiedFunds(received.getFunds());
						current.setSide(received.getSide());
						current.setSize(received.getSize());
						current.setStatus(OrderStatus.RECEIVED);
						current.setExecutedValue(BigDecimal.ZERO);
						current.setFilledSize(BigDecimal.ZERO);
						current.setFillFees(BigDecimal.ZERO);
						current.setRemainingSize(current.getSize());
						current.setFeeInQuote(received.getFeeInQuote());
						current.setCreatedAt(received.getTime());
						current.setTimeInForce(received.getTimeInForce());
						accountService.o_lockFundsForNewOrder(current);
						traderSellOrders.put(mpUUID, current);
						logger.warn("o_receivedOrder: SELL: RECEIVED TP created order: " + received.getOrder_id());	
					} else {
						logger.warn("o_receivedOrder: SELL: ignoring order: " + received.getOrder_id() + " for product " + received.getProductName());
					}
					
				} else {
					logger.warn("o_receivedOrder: SELL: RECEIVED: " + current.getMyUUID() + " " + received.getOrder_id());	

				}
					
					
				if (current != null) current.setMpCreateRequested(false);  // now cancelable
				
			}
		
		
		}

	}
	
	
	private void o_restoredOrder(Restored received) {

		String mpUUID = received.getOrder_id();
		
		MpOrder current = null;
		if (received.getProductName().equals(wsProduct)) {
			if (received.getSide().equals(Side.BUY)) {
				
				current = new MpOrder();
				current.setMpUUID(received.getOrder_id());
				current.setOrderType(received.getOrder_type());
				current.setPrice(received.getPrice());
				current.setPostOnly(received.getPost_only());
				//current.setFunds(received.getFunds());
				current.setSpecifiedFunds(received.getFunds());
				current.setSide(received.getSide());
				current.setSize(received.getSize());
				current.setStatus(OrderStatus.OPEN);
				current.setExecutedValue(BigDecimal.ZERO);
				current.setFilledSize(BigDecimal.ZERO);
				current.setFillFees(BigDecimal.ZERO);
				current.setRemainingSize(current.getSize());
				current.setFeeInQuote(received.getFeeInQuote());
				current.setCreatedAt(received.getTime());
				current.setTimeInForce(received.getTimeInForce());	
				current.setMpCreateRequested(false); // now cancelable
				
				synchronized (traderBuyOrders) {
					traderBuyOrders.put(mpUUID, current);
					logger.info("o_restoredOrder: BUY: " + received.getClient_oid() + " " + received.getOrder_id());
				} 
				
				synchronized (traderLockObj) {
					traderLockObj.notify();
				}
				
			} else {
				
				current = new MpOrder();
				current.setMpUUID(received.getOrder_id());
				current.setOrderType(received.getOrder_type());
				current.setPrice(received.getPrice());
				current.setPostOnly(received.getPost_only());
				//current.setFunds(received.getFunds());
				current.setSpecifiedFunds(received.getFunds());
				current.setSide(received.getSide());
				current.setSize(received.getSize());
				current.setStatus(OrderStatus.OPEN);
				current.setExecutedValue(BigDecimal.ZERO);
				current.setFilledSize(BigDecimal.ZERO);
				current.setFillFees(BigDecimal.ZERO);
				current.setRemainingSize(current.getSize());
				current.setFeeInQuote(received.getFeeInQuote());
				current.setCreatedAt(received.getTime());
				current.setTimeInForce(received.getTimeInForce());
				current.setMpCreateRequested(false); // now cancelable
				
				synchronized (traderSellOrders) {
					traderSellOrders.put(mpUUID, current);
					logger.info("o_restoredOrder: SELL: " + received.getClient_oid() + " " + received.getOrder_id());	
				}
				
				synchronized (traderLockObj) {
					traderLockObj.notify();
				}
			
			
			}
				
		} else {

			logger.info("o_restoredOrder: " + received.getSide() + ": ignoring order: " + received.getClient_oid() + " " + received.getOrder_id());
		}
		
		
		
	}
	
	
	
	
	private void o_openedOrder(Open opened) {
		String mpUUID = opened.getOrder_id();
		MpOrder current = null;
		synchronized (traderBuyOrders) {
			current = traderBuyOrders.get(mpUUID);
			if (current != null) {
				current.setStatus(OrderStatus.OPEN);
			}
		}
		if (current == null) {
			synchronized (traderSellOrders) {
				current = traderSellOrders.get(mpUUID);
				if (current != null) {
					current.setStatus(OrderStatus.OPEN);
				}
			}
		}
		
		synchronized (traderLockObj) {
			traderLockObj.notify();
		}
	}

	private void o_confirmedOrder(Confirmed confirmed) {
		String mpUUID = confirmed.getOrder_id();
		String myUUID = confirmed.getClient_oid();
		
		if (myUUID != null) {
			MpOrder current = null;
			synchronized (traderBuyOrders) {
				current = traderBuyOrders.get(myUUID);
				if (current != null) {
					
					if (confirmed.getSuccess()) {
						current.setMpUUID(mpUUID);
						traderBuyOrders.remove(myUUID);
						traderBuyOrders.put(mpUUID, current);
						logger.info("o_confirmedOrder: Confirmed: " + myUUID + " " + mpUUID);
					} else {
						traderBuyOrders.remove(myUUID);
						logger.error("o_confirmedOrder: error: removing " + myUUID);
					}
					
				}
			}
			if (current == null) {
				synchronized (traderSellOrders) {
					current = traderSellOrders.get(myUUID);
					if (current != null) {
						
						if (confirmed.getSuccess()) {
							current.setMpUUID(mpUUID);
							traderSellOrders.remove(myUUID);
							traderSellOrders.put(mpUUID, current);
							
							logger.info("o_confirmedOrder: Confirmed: " + myUUID + " " + mpUUID);
							
						} else {
							
							traderSellOrders.remove(myUUID);
							logger.error("o_confirmedOrder: error: removing " + myUUID);
						}
						
					}
				}
			}
			
			
		}
		
		
	}
	
	
	
	private void o_changedOrder(Change change) {
		
		
		try {
			logger.error("o_changedOrder: not implemented " + JsonUtil.serialize(change, false));
		} catch (JsonProcessingException e) {
			logger.error("o_changedOrder: ", e);
		}
		
	}

	private void o_processMatchForOrder(Match match, MpOrder order, Boolean taker) {
		// mine
		
		boolean profit = false;
		
		
		BigDecimal price = match.getPrice();
		BigDecimal size = match.getSize();
		BigDecimal quoteAmount = price.multiply(size, accountService.getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
		BigDecimal matchFees = match.getFees().setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
		
		//BigDecimal originalLockedFees = originalQuoteAmount
		
		
		
		/*
		if (taker) {
			matchFees = quoteAmount.multiply(accountService.getTakerFees(), accountService.getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
		} else {
			matchFees = quoteAmount.multiply(accountService.getMakerFees(), accountService.getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
		}
		*/
			
		switch (order.getOrderType()) {
		case LIMIT:
		case MARKET:
		{
			
			synchronized(order) {
				if (order.getFilledSize() == null) order.setFilledSize(BigDecimal.ZERO);
				order.setFilledSize(order.getFilledSize().add(size));
				if (order.getFillFees() == null) order.setFillFees(BigDecimal.ZERO);
				order.setFillFees(order.getFillFees().add(matchFees));
				if (order.getRemainingSize() == null) order.setRemainingSize(BigDecimal.ZERO);
				order.setRemainingSize(order.getRemainingSize().subtract(size));
				if (order.getExecutedValue() == null) order.setExecutedValue(BigDecimal.ZERO);
				order.setExecutedValue(order.getExecutedValue().add(quoteAmount));
				
			}
			
			break;
		}
		
		case STOP: {
			
			break;
		}
			
			
		}
		
		
		
		
		if (
				(match.getSide().equals(Side.BUY))
				/*&& (!taker))
				||
				(match.getSide().equals(Side.SELL) && (taker))*/
				
		) { // BUY
			
			profit = accountService._buy_update_for_match(match, order, taker, quoteAmount, matchFees);
			
		} else {
			// SELL
			//(match.getSide().equals(Side.SELL) && (!taker))
			//	||
			//	(match.getSide().equals(Side.BUY) && (taker))
			
			
			profit = accountService._sell_update_for_match(match, order, taker, quoteAmount, matchFees);
		} 
		
		synchronized(exchangedLockObj) {
			exchangedBase = exchangedBase.add(size);
			exchangedQuote = exchangedQuote.add(quoteAmount);
			
			if (profit) {
				profitExchangedBase = profitExchangedBase.add(size);
				profitExchangedQuote = profitExchangedQuote.add(quoteAmount);
				
			} else {
				lossExchangedBase = lossExchangedBase.add(size);
				lossExchangedQuote = lossExchangedQuote.add(quoteAmount);
			}
		}
		
	}

	
	
	
	public void o_doneOrder(Done done) {
		
		
		String mpUUID = done.getOrder_id();
		MpOrder current = null;
		
		synchronized (traderBuyOrders) {
			current = traderBuyOrders.get(mpUUID);
		}
		if (current == null) {
			synchronized (traderSellOrders) {
				current = traderSellOrders.get(mpUUID);
			}
		}
		
		
		
		if (current != null) {
			
			
			synchronized (current) {
				
				switch (current.getStatus()) {
				
				case NEW:
				case PENDING:
				case RECEIVED:
				case OPEN: {
					
					if (debug) logger.info("o_doneOrder: " + mpUUID + " " + current.getStatus() + "=>DONE "+ this.printTraderOrder(current) );
					
					current.setDoneAt(done.getTime());
					BigDecimal doneRemainingSize = null;
					
					if (current.getOrderType().equals(OrderType.LIMIT) 
							|| ((current.getOrderType().equals(OrderType.MARKET) ) && current.getSide().equals(Side.SELL)) 
							
							) {
						doneRemainingSize = done.getRemaining_size();
						if (doneRemainingSize == null) {
							doneRemainingSize = current.getSize().subtract(done.getExecuted_size());
						}
						
						if (doneRemainingSize != null) {
							if (current.getRemainingSize().compareTo(doneRemainingSize) != 0) {
								// we did not received all matches
								logger.warn("doneOrder: " + mpUUID + " NOT all matches received. ");
								//o_listFillsAndProcessMissing(current);
							} else {
								if (debug) logger.info("doneOrder: " + mpUUID + " all matches received. ");
							}
						} else {
							// MARKET BUY
							logger.warn("doneOrder: " + mpUUID + " NOT all matches received. ");
							
							//o_listFillsAndProcessMissing(current);
						}
					} else {
						
					}
					
					
					 //  we received all matches.
					 // we can release remaining locked funds and remove order from list
					 
					
					if (current.getSide().equals(Side.BUY)) {
						
						//accountService._buy_release_remaining_funds(current, doneRemainingSize);
						if (doneRemainingSize!=null) accountService._buy_release_remaining_funds(current, doneRemainingSize);
						
						
						synchronized (traderBuyOrders) {
							if (current.getMpUUID() != null) {
								traderBuyOrders.remove(current.getMpUUID());
							} else {
								traderBuyOrders.remove(current.getMyUUID());
							}
							
						}
					} else { // SELL
						//accountService._sell_release_remaining_funds(current, doneRemainingSize);
						if (doneRemainingSize!=null) accountService._sell_release_remaining_funds(current, doneRemainingSize);
						
						synchronized (traderSellOrders) {
							if (current.getMpUUID() != null) {
								traderSellOrders.remove(current.getMpUUID());
							} else {
								traderSellOrders.remove(current.getMyUUID());
							}
							
						}
					}
					
					break;
				}
				default: {
					logger.error("doneOrder: " + mpUUID + " " + current.getStatus() + "=>DONE status for existing order " + this.printTraderOrder(current));
				}
		}
			}
			
			
			
			
		}
		
		
		
	}


	private void o_matchOrder(Match match) {
		
		Boolean taker = null;
		if (match.getMaker_order_id() != null) {
			taker = false;
		} else if (match.getTaker_order_id() != null) {
			taker = true;
		}
		
		//logger.info("o_matchOrder: taker: " + taker);
		
		if (taker != null) {
			
			
			if (debug) {
				if (taker) {
					try {
						logger.warn("o_matchOrder: TAKER match received " + match.getTaker_order_id() + " " + JsonUtil.serialize(match, false));
					} catch (JsonProcessingException e) {
						logger.error("o_matchOrder", e);
					}
				} else {
					try {
						logger.warn("o_matchOrder: MAKER match received " + match.getMaker_order_id() + " " + JsonUtil.serialize(match, false));
					} catch (JsonProcessingException e) {
						logger.error("o_matchOrder", e);
					}
				}
			}
			
			
			MpOrder order = null;
			
			if (match.getSide().equals(Side.BUY)) {
				
				
				synchronized (traderBuyOrders) {
					
					order = traderBuyOrders.get(
							match.getOrder_id());
					if (debug) logger.warn("o_matchOrder: BUY match received " + match.getMaker_order_id() + " found order: " + order);

					
				} 
				
				/*
				if (taker) {
					synchronized (traderSellOrders) {
						
						order = traderSellOrders.get(
								match.getTaker_order_id());
						if (debug) logger.warn("o_matchOrder: TAKER BUY (we SELL) match received " + match.getTaker_order_id() + " found order: " + order);
				
						
					} 
						} else {
					synchronized (traderBuyOrders) {
						
						order = traderBuyOrders.get(
								match.getMaker_order_id());
						if (debug) logger.warn("o_matchOrder: MAKER BUY (we BUY) match received " + match.getMaker_order_id() + " found order: " + order);

						
					} 
					
				}*/
				
				
			} else { // SELL
				
				synchronized (traderSellOrders) {
					order = traderSellOrders.get(
							match.getOrder_id());
					if (debug) logger.warn("o_matchOrder: SELL match received " + match.getMaker_order_id() + " found order: " + order);

					
				} 
				/*
				if (taker) {
					synchronized (traderBuyOrders) {
						order = traderBuyOrders.get(
								match.getTaker_order_id());
						if (debug) logger.warn("o_matchOrder: TAKER SELL (we BUY) match received " + match.getTaker_order_id() + " found order: " + order);

					}
					
				} else {
					synchronized (traderSellOrders) {
						order = traderSellOrders.get(
								match.getMaker_order_id());
						if (debug) logger.warn("o_matchOrder: MAKER SELL (we SELL) match received " + match.getMaker_order_id() + " found order: " + order);

						
					} 
					
					
				}*/
			}
				
			if (order != null) {
				
				this.o_processMatchForOrder(match, order, taker);
			} else {
				// order not found, ignoring and not saving
				if (debug) logger.warn("o_matchOrder: ignoring match for nonexistant order takerOrderId: " + match.getTaker_order_id() + " makerOrderId: " + match.getMaker_order_id());
			}
			
			
			
			
		}
	}
	

	
	public MpOrder t_buildLimitOrder(Side side, BigDecimal price, BigDecimal size, boolean maker, TimeInForce tif, Instant now) {
		
		size = size.setScale(productBaseScale, RoundingMode.DOWN);
		price = price.setScale(productQuoteScale, RoundingMode.DOWN);
		
		if (size.compareTo(productBaseMinSize) < 0) return null;
		
		MpOrder order = new MpOrder();
		order.setStatus(OrderStatus.NEW);
		order.setMyUUID(Long.toHexString(UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE));
		order.setOrderType(OrderType.LIMIT);
		order.setPostOnly(maker);
		order.setPrice(price);
		order.setSize(size);
		order.setRemainingSize(size);
		order.setSide(side);
		order.setBuiltAt(now);
		order.setTimeInForce(tif);
		order.setMpCreateRequested(true);
		return order;
	}
	
	/*private void o_listFillsAndProcessMissing(MpOrder order) {
		
		if (debug) logger.info("o_listFillsAndProcessMissing: " + order.getMpUUID() + " NOT all matches received, getting fills... ");
		
		List<Fill> fills = krakenService.listFills(order.getMpUUID());
		
		int i = 0;
		
		for (Fill fill:fills) {
			if (debug) logger.info("o_listFillsAndProcessMissing: " + order.getMpUUID() + " NOT all matches received, getting fills... #" + i);
			
			//MatchB mb = em.find(MatchB.class, fill.getTrade_id());
			//if (mb == null) {
				// match has not been received
				if (debug) logger.info("o_listFillsAndProcessMissing: " + order.getMpUUID() + " NOT all matches received, getting fills... #" + i + " has not been received, processing");
				
				Match match = new Match();
				match.setOrder_id(order.getMpUUID());
				match.setPrice(fill.getPrice());
				match.setSide(fill.getSide());
				match.setSize(fill.getSize());
				match.setTime(fill.getCreated_at());
				match.setTrade_id(fill.getTrade_id());
				Boolean taker =  fill.getLiquidity().equals("T");
				//a15TradingService.saveMatch(match, taker);
				this.o_processMatchForOrder(match, order, taker);
			
		}
	}*/

	protected String printTraderOrder(MpOrder order) {
		return (order.getSide() + " p/s/t/po: " + order.getPrice() + "/" + order.getSize() + "/" + order.getOrderType() + "/" + order.getPostOnly() + " rs:" + order.getRemainingSize() + " ev: " + order.getExecutedValue() + " " + order.getStatus() + " " + order.getMpUUID() );

	}



	
	@Transactional(value=TxType.NEVER)
	public void startTrQueueProcessor() {
    	
		Uni.createFrom().item(UUID::randomUUID).emitOn(executor).subscribe().with(
                this::startTrQueueProcessor, Throwable::printStackTrace
        );
	}
	
	
	public void setTrStartedQueueProcessor() {
		startedTrQueueProcessor = true;
	}
	
	
	@Transactional(value=TxType.NEVER)
	public void startTrQueueProcessor(UUID uuid) {
		
		logger.info("startTrQueueProcessor: starting... @" + Instant.now() + " startedQueueProcessor: " + startedTrQueueProcessor);
		
		trQueueProcessorRunning = true;
		
		while (startedTrQueueProcessor) {
			
			try {
				Data data = null;
				int dequeSize = 0;
				synchronized (trDataQueue) {
					try {
						data = trDataQueue.poll();
						dequeSize = trDataQueue.size();
					} catch (Exception e) {
						
					}
				}
				
				if (data != null) {
					long t = System.currentTimeMillis();
					logger.info("startTrQueueProcessor: " + uuid + " processing " + JsonUtil.serialize(data, false));
					trData(data);
					
					//logger.info("startQueueProcessor: processing match: done " + (System.currentTimeMillis() - t) + "ms"); 
					
				} else {
					synchronized (trQueueProcessorLockObj) {
						try {
							trQueueProcessorLockObj.wait(3600000l);
						} catch (Exception e) {
							
						}
					}
				}
			} catch (Exception e) {
				logger.error("startTrQueueProcessor: ", e);
			}
			
			
		}
		trQueueProcessorRunning = false;
		logger.info("startTrQueueProcessor: exiting... done @" + Instant.now());
			
	}
	
	
	
	private void trData(Data data) {
		
		if (data instanceof Received) {
			
			Received received = (Received) data;
			if (debug) {
				logger.info("trData: Received: myUUID: " + received.getClient_oid() + " mpUUID: " + received.getOrder_id());
			}
			o_receivedOrder(received);
			
	} else if (data instanceof Restored) {
		
		Restored restored = (Restored) data;
		if (debug) {
			logger.info("trData: Done: mpUUID: " + restored.getOrder_id());
		}
		
		this.o_restoredOrder(restored);
		
		
	} else if (data instanceof Confirmed) {
		
		Confirmed confirmed = (Confirmed) data;
		if (debug) {
			logger.info("trData: Confirmed: mpUUID: " + confirmed.getOrder_id());
		}
		
		this.o_confirmedOrder(confirmed);
		
		
	} else if (data instanceof Done) {
		
		Done done = (Done) data;
		if (debug) {
			logger.info("trData: Done: reason: " + done.getReason() + " mpUUID: " + done.getOrder_id());
		}
		
		this.o_doneOrder(done);
		
		
	} else if (data instanceof Open) {
		
		Open open = (Open) data;
		if (debug) {
			logger.info("trData: Open: remainingSize: " + open.getRemaining_size() + " side: " + open.getSide() + " mpUUID: " + open.getOrder_id());
		}
		
		this.o_openedOrder(open);
		
	} else if (data instanceof Change) {
		
		Change change = (Change) data;
		if (debug) {
			logger.info("trData: Change: oldSize: " + change.getOld_size() + " newSize: " + change.getNew_size() + " oldFunds: " + change.getOld_funds() + " newFunds: " + change.getNew_funds() + " side: " + change.getSide() + " mpUUID: " + change.getOrder_id());
		}
		this.o_changedOrder(change);
		
	}
	else if (data instanceof Match) {
		Match match = (Match) data;
		
		o_matchOrder(match);
	} else {
		
			try {
				logger.warn("trData: ignored: " + JsonUtil.serialize(data, false));
			} catch (JsonProcessingException e) {
				logger.error("", e);
			}
	}
	}

	public void notifyTrQueueProcessorLockObj() {
		synchronized (trQueueProcessorLockObj) {
			try {
				trQueueProcessorLockObj.notifyAll();
			} catch (Exception e) {
				
			}
		}
	}

	public Instant getTrInstantOfFirstObjectInQueue() {
		Data data = trDataQueue.peekFirst();
		if (data != null) {
			return Instant.now();
		} else {
			return null;
		}
	}
	
	private static Object stopTrQueueProcessorLockObj = new Object();
	
	public void stopTrQueueProcessor() {
		startedTrQueueProcessor = false;
		notifyTrQueueProcessorLockObj();
		
		while (isTrQueueProcessorRunning()) {
			notifyTrQueueProcessorLockObj();
			synchronized (stopTrQueueProcessorLockObj) {
				try {
					logger.info("stopTrQueueProcessor: waiting for queueProcessor to finish...");
					stopTrQueueProcessorLockObj.wait(100l);
				} catch (Exception e) {
					
				}
			}
		}
		
		trDataQueue.clear();
	}
	
	public static boolean isTrQueueProcessorRunning() {
		return trQueueProcessorRunning;
	}
	
	public void syncOrders() {
		// get orders
		
			//List<MpOrder> orders = krakenService.listOrders();
			
			traderBuyOrders = new ConcurrentHashMap<String, MpOrder>();
			traderSellOrders = new ConcurrentHashMap<String, MpOrder>();
			
			/*
			for (MpOrder order: orders) {
				if (order.getSide().equals(Side.BUY)) {
					synchronized(traderBuyOrders) {
						traderBuyOrders.put(order.getMpUUID(), order);
					}
					
				} else {
					synchronized (traderSellOrders) {
						traderSellOrders.put(order.getMpUUID(), order);
					}
					
				}
			}
			
			printTraderSellOrders();
			printTraderBuyOrders();
			*/
			
	}
	
	public void printTraderSellOrders() {
		if (traderSellOrders.size() > 0) {
			
			synchronized(traderSellOrders) {
				for(String uuid: traderSellOrders.keySet()) {
					MpOrder order = traderSellOrders.get(uuid);
					logger.info("printTraderSellOrders: " + this.printTraderOrder(order));
				}
			}
			
		} else {
			//logger.info("printTraderSellOrders: no SELL orders");
		}
		
	}
	
	public void printTraderBuyOrders() {
		
		if (traderBuyOrders.size() > 0) {
			synchronized(traderBuyOrders) {
				for(String uuid: traderBuyOrders.keySet()) {
					MpOrder order = traderBuyOrders.get(uuid);
					logger.info("printTraderBuyOrders: " +  this.printTraderOrder(order));
			}
			}
			
		} else {
			//logger.info("printTraderBuyOrders: no BUY orders");
		}
		
	}
	
	
protected MpOrder t_buildMarketOrder(BigDecimal funds, BigDecimal size, Instant now) {
		
		if (size.compareTo(productBaseMinSize) < 0) return null;
		
		MpOrder order = new MpOrder();
		order.setStatus(OrderStatus.NEW);
		order.setMyUUID(Long.toHexString(UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE));
		order.setOrderType(OrderType.MARKET);
		order.setBuiltAt(now);
		order.setMpCreateRequested(true);
		if (funds != null) {
			order.setFunds(funds);
			order.setExecutedValue(BigDecimal.ZERO);
			order.setSide(Side.BUY);
		} else if (size != null) {
			order.setSize(size);
			order.setRemainingSize(size);
			order.setSide(Side.SELL);
		} else {
			return null;
		}
		
		return order;
	}
	
	protected boolean t_placeMakerOrder(Side side, BigDecimal price, BigDecimal size, Instant now) {
		boolean retval = false;
		MpOrder limitOrder = this.t_buildLimitOrder(side, price, size, true, null, now);
		if (limitOrder != null) {
			retval = this.t_placeOrder(limitOrder);
		}
		return retval;
	}
	protected boolean t_placeTakerOrder(Side side, BigDecimal price, BigDecimal size, TimeInForce tif, Instant now) {
		boolean retval = false;
		MpOrder limitOrder = this.t_buildLimitOrder(side, price, size, false, tif, now);
		if (limitOrder != null) {
			retval = this.t_placeOrder(limitOrder);
		}
		return retval;
	}

	protected boolean t_placeOrder(MpOrder order) {
		
		
		boolean locked = accountService.o_lockFundsForNewOrder(order);
		if (!locked) return false;
		
		if (order.getSide().equals(Side.BUY)) {
			synchronized (traderBuyOrders) {
				traderBuyOrders.put(order.getMyUUID(), order);
			}
		} else {
			synchronized (traderSellOrders) {
				traderSellOrders.put(order.getMyUUID(), order);
			}
		}
		
		krakenService.spoolNewOrder(order);
		krakenService.notifyQueues();
		
		logger.info("t_placeOrder: spooled " + order.getSide() + " price: " + order.getPrice() + " b/a: " + bid + "/" + ask);
		return true;
	}

	
	
	


	
	protected void t_purgeUnwantedOrders(Position p, Instant now) {
		
		if ((p == null) || (p.getAction() == null) || (p.getAction().equals(Action.WAIT))) {
			//logger.info("t_purgeUnwantedOrders: null position: purging all orders");
			t_purgeSellOrders(null, null, now);
			t_purgeBuyOrders(null, null, now);
		} else {
			//logger.info("t_purgeUnwantedOrders: purging SELL orders not satisfying minSell: " + p.getMinSell()+ " maxSell: " + p.getMaxSell() + " action: " + p.getAction());
			
			t_purgeSellOrders(p.getMinSell(), p.getMaxSell(), now);
			
			//logger.info("t_purgeUnwantedOrders: purging BUY orders not satisfying minBuy: " + p.getMinBuy()+ " maxBuy: " + p.getMaxBuy());
			t_purgeBuyOrders(p.getMinBuy(), p.getMaxBuy(), now);
		}
		
	}

	private void t_purgeBuyOrders(BigDecimal minPrice, BigDecimal maxPrice, Instant now) {
		
		
		Set<String> keySet = null;
		synchronized (traderBuyOrders) {
			keySet = Set.copyOf(traderBuyOrders.keySet());
		}
		
		for (String key: keySet) {
			
			if (debug) logger.info("t_purgeBuyOrders: checking " + key);
			
			MpOrder order = traderBuyOrders.get(key);
			boolean purge = false;
			if (order != null) {
				synchronized (order) {
					
					if ((!order.isMpCancelRequested()) && (!order.isMpCreateRequested() )) {

						if ((minPrice == null) && (maxPrice == null)) {
							
							if (debug) logger.info("t_purgeBuyOrders: checking " + key + " minPrice null and maxPrice null, purging ");
							
							if (debug) {
								if (order.getMpUUID() != null) {
									logger.info("purgeBuyOrders: purging MP " + order.getMpUUID() + " as minPrice and maxPrice are null @" + now);
								} else {
									logger.info("purgeBuyOrders: purging MY " + order.getMpUUID() + " as minPrice and maxPrice are null @" + now);

								}
							}
							purge = true;
						} else {

							switch (order.getOrderType()) {

							case LIMIT:
							case STOP: {
								if (minPrice != null) {
									if (order.getPrice().compareTo(minPrice) < 0) {
										// order price < minPrice
										
										if (debug) logger.info("t_purgeBuyOrders: checking " + key + " order price < minPrice, purging ");
										
										
										if (debug) {
											
											if (order.getMpUUID() != null) {
												logger.info("purgeBuyOrders: purging MP " + order.getMpUUID() + " order.getPrice(): " + order.getPrice() + " < minPrice: " + minPrice + " @" + now);
											} else {
												logger.info("purgeBuyOrders: purging MY " + order.getMyUUID() + " order.getPrice(): " + order.getPrice() + " < minPrice: " + minPrice + " @" + now);

											}
										}
										purge = true;
									}
								}
								if (maxPrice != null) {
									if (order.getPrice().compareTo(maxPrice) > 0) {
										
										if (debug) logger.info("t_purgeBuyOrders: checking " + key + " order price > maxPrice, purging ");
										
										// order price > maxPrice
										if (debug) {
											
											if (order.getMpUUID() != null) {
												logger.info("purgeBuyOrders: purging MP " + order.getMpUUID() + " order.getPrice(): " + order.getPrice() + " > maxPrice: " + maxPrice + " @" + now);

											} else {
												logger.info("purgeBuyOrders: purging MY " + order.getMyUUID() + " order.getPrice(): " + order.getPrice() + " > maxPrice: " + maxPrice + " @" + now);

											}
										}
										purge = true;
									}
								}
								break;
							}
							case MARKET: {

								break;
							}
							}
						}
						if (purge) {
							order.setMpCancelRequested(true);
						}

					} 
				}
			}
			
			if (purge) {
				
				logger.info("t_purgeBuyOrders: purging... ");
				krakenService.spoolCancelOrder(order);
				krakenService.notifyQueues();
				
			} else {
				logger.info("t_purgeBuyOrders: not purging ");
				
			}
		}
	}

	private void t_purgeSellOrders(BigDecimal minPrice, BigDecimal maxPrice, Instant now) {
		
		// purge sell orders not in this range
		//logger.info("purgeSellOrders: entering, minPrice: " + minPrice + " maxPrice: " + maxPrice);
		
		Set<String> keySet = null;
		synchronized (traderSellOrders) {
			keySet = Set.copyOf(traderSellOrders.keySet());
		}
			
		for (String key: keySet) {
			MpOrder order = traderSellOrders.get(key);
			boolean purge = false;
			if (order != null) {
				synchronized (order) {
					if ((!order.isMpCancelRequested()) && (!order.isMpCreateRequested())) {
						
						if ((minPrice == null) && (maxPrice == null)) {
							if (debug) {
								if (order.getMpUUID() != null) {
									logger.info("purgeSellOrders: purging MP " + order.getMpUUID() + " as minPrice and maxPrice are null @" + now);
								} else {
									logger.info("purgeSellOrders: purging MY " + order.getMpUUID() + " as minPrice and maxPrice are null @" + now);

								}
							}
							purge = true;
						} else {
							
							switch (order.getOrderType()) {
							
							case LIMIT:
							case STOP: {
								if (minPrice != null) {
									if (order.getPrice().compareTo(minPrice) < 0) {
										// order price < minPrice
										purge = true;
										if (debug) {
											
											if (order.getMpUUID() != null) {
												logger.info("purgeSellOrders: purging MP " + order.getMpUUID() + " order.getPrice(): " + order.getPrice() + " < minPrice: " + minPrice + " @" + now);
											} else {
												logger.info("purgeSellOrders: purging MY " + order.getMyUUID() + " order.getPrice(): " + order.getPrice() + " < minPrice: " + minPrice + " @" + now);

											}
										}

									}
								}
								if (maxPrice != null) {
									if (order.getPrice().compareTo(maxPrice) > 0) {
										// order price > maxPrice
										purge = true;
										if (debug) {
											
											if (order.getMpUUID() != null) {
												logger.info("purgeSellOrders: purging MP " + order.getMpUUID() + " order.getPrice(): " + order.getPrice() + " > maxPrice: " + maxPrice + " @" + now);

											} else {
												logger.info("purgeSellOrders: purging MY " + order.getMyUUID() + " order.getPrice(): " + order.getPrice() + " > maxPrice: " + maxPrice + " @" + now);

											}
										}

									}
								}
								break;
							}
							case MARKET: {
								
								break;
							}
							}
							
						}
								
						/*} else {
							logger.warn("purgeSellOrders: only OPEN orders can be canceled");
						}*/
						
						
						
						
						if (purge) {
							//logger.info("purgeSellOrders: purging " + order.getMpUUID());
							order.setMpCancelRequested(true);
							
							
							
						} else {
							//logger.info("purgeSellOrders: not purging " + order.getMpUUID());
						}
						
					}
					
				}
			}
			if (purge) {
				
				krakenService.spoolCancelOrder(order);
				krakenService.notifyQueues();	
			}
		}
	}


	
	
	
	private BigDecimal t_getMakerBestPrice(Side side) {
		
		
		if (side.equals(Side.SELL)) {
			
			// get order book
			Set<BigDecimal> asks = orderBookSellOrders.keySet();
			BigDecimal bestAsk = null;
			BigDecimal oldBestAsk = null;
			
			for (BigDecimal currentAsk: asks) {
				
				BigDecimal ourSizeForCurrentAsk = BigDecimal.ZERO;
				bestAsk = currentAsk;
				
				synchronized (traderSellOrders) {
					for (String myUUID: traderSellOrders.keySet()) {
						MpOrder order = traderSellOrders.get(myUUID);
						
						if (order != null) {
							if ((!order.getStatus().equals(OrderStatus.NEW)) && (order.getPrice().compareTo(currentAsk) == 0)) {
								if (debug && (ourSizeForCurrentAsk.signum() != 0)) {
									logger.info("t_getMakerBestPrice: SELL found order " + order.getMyUUID() + " with price " + order.getPrice() +  " size " + order.getSize() + " currentAsk " + currentAsk + " currentSize " +  orderBookSellOrders.get(currentAsk));
								}
								ourSizeForCurrentAsk = ourSizeForCurrentAsk.add(order.getSize());
								if (debug && (ourSizeForCurrentAsk.signum() != 0)) {
									logger.info("t_getMakerBestPrice: SELL ourSizeForCurrentAsk: " + ourSizeForCurrentAsk + " ob: " + orderBookSellOrders.get(currentAsk));
								}
							}
							if (oldBestAsk == null) {
								oldBestAsk = order.getPrice();
							} else {
								if (order.getPrice().compareTo(oldBestAsk) < 0) {
									oldBestAsk = order.getPrice();
								}
							}
						}
						
						
					}
				}
				
				BigDecimal sizeCurrentAsk = orderBookSellOrders.get(currentAsk);
				if ((sizeCurrentAsk == null) || ourSizeForCurrentAsk.compareTo(sizeCurrentAsk) < 0) {
					if (debug && (ourSizeForCurrentAsk.signum() != 0)) {
						logger.info("t_getMakerBestPrice: SELL ourSizeForCurrentAsk: " + ourSizeForCurrentAsk + " ob: " + orderBookSellOrders.get(currentAsk) + " not all volume is from us, setting bestAsk to currentAsk - trader.getProductQuoteIncrement() or to currentAsk");
					}
					if (bestAsk.subtract(bid).compareTo(productQuoteIncrement) >0) {
						bestAsk = bestAsk.subtract(productQuoteIncrement);
					}
					break;
				}
			}
			
			if (debug && (traderSellOrders.size() >0 )) {
				logger.info("t_getMakerBestPrice: SELL best Ask: " + bestAsk + " oldBestAsk: " + oldBestAsk);
			}
			return bestAsk;
			
		} else {
			// BUY
			
			// get order book
			Set<BigDecimal> bids = orderBookBuyOrders.descendingKeySet();
			BigDecimal bestBid = null;
			BigDecimal oldBestBid = null;
			
			for (BigDecimal currentBid: bids) {
				
				BigDecimal ourSizeForCurrentBid = BigDecimal.ZERO;
				bestBid = currentBid;
				synchronized (traderBuyOrders) {
					for (String myUUID: traderBuyOrders.keySet()) {
						MpOrder order = traderBuyOrders.get(myUUID);
						if ((order != null) && (!order.getStatus().equals(OrderStatus.NEW)) && (order.getPrice().compareTo(currentBid) == 0)) {
							if (debug && (ourSizeForCurrentBid.signum() != 0)) {
								logger.info("t_getMakerBestPrice: BUY found order " + order.getMyUUID() + " with price " + order.getPrice() +  " size " + order.getSize() + " currentBid " + currentBid + " currentSize " +  orderBookBuyOrders.get(currentBid));
							}
							ourSizeForCurrentBid = ourSizeForCurrentBid.add(order.getSize());
							if (debug  && (ourSizeForCurrentBid.signum() != 0)) {
								logger.info("t_getMakerBestPrice: BUY ourSizeForCurrentBid: " + ourSizeForCurrentBid + " ob: " + orderBookBuyOrders.get(currentBid));
							}
						}
						if (oldBestBid == null) {
							oldBestBid = order.getPrice();
						} else {
							if (order.getPrice().compareTo(oldBestBid) > 0) {
								oldBestBid = order.getPrice();
							}
						}
					}
				}
				
				BigDecimal sizeCurrentBid =  orderBookBuyOrders.get(currentBid);
				
				if ((sizeCurrentBid == null) || ourSizeForCurrentBid.compareTo(sizeCurrentBid) < 0) {
					if (debug  && (ourSizeForCurrentBid.signum() != 0)) {
						logger.info("t_getMakerBestPrice: BUY ourSizeForCurrentBuy: " + ourSizeForCurrentBid + " ob: " + orderBookBuyOrders.get(currentBid) + " not all volume is from us, setting bestBid to currentBid + trader.getProductQuoteIncrement() or to currentBid");
					}
					if (ask.subtract(bestBid).compareTo(productQuoteIncrement) >0) {
						bestBid = bestBid.add(productQuoteIncrement);
					}
					break;
				}
				
			}
			
			if (debug && (traderBuyOrders.size() >0)) {
				logger.info("t_getMakerBestPrice: BUY best Bid: " + bestBid + " oldBestBid: " + oldBestBid);
			}
			return bestBid;
			
			
		}
		
		

	}
	
	
	protected List<MpOrder> t_optimalDisperseBuyOrderWithinSpread(BigDecimal quoteToSpend,
			BigDecimal minPrice, BigDecimal maxPrice,
			int numberOfChunks, 
			TimeInForce tif, Instant now) {
		
		List<MpOrder> limitOrders = new ArrayList<MpOrder>();
		
		
		
		
		if (quoteToSpend != null) {
			
			// BUY
			
			// 1. define price
			
			BigDecimal range = maxPrice.subtract(minPrice);
			Integer rangeInt = (range.multiply(BigDecimal.TEN.pow(productQuoteScale))).intValue();
			
			if (rangeInt <0) {
				//System.out.println("t_buildDispersedOrders: minPrice: " + minPrice + " maxPrice: " + maxPrice + " side: " + Side.BUY + " numberOfChunks: " + numberOfChunks + " quote: " + quoteToSpend + "  requiresAtMarketPriceOrder: " + requiresAtMaxPriceOrder + " attempt to use negative rangeInt: " + rangeInt + " setting to 0");
				rangeInt = 0;
			}
			
			
			BigDecimal higherPrice = maxPrice;
			
				// minSize for a chunk (from higherPrice)
				BigDecimal minSize = quoteToSpend.divide(higherPrice, accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
				BigDecimal chunckSize = minSize.divide(new BigDecimal(numberOfChunks), accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
				if (minSize.compareTo(productBaseMinSize) > 0) {
					
					while (
							
							(chunckSize.compareTo(productBaseMinSize)<0)
							|| (numberOfChunks<1)
							
							) {
						numberOfChunks = numberOfChunks -1;
						chunckSize = minSize.divide(new BigDecimal(numberOfChunks), accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
					
					
					}
				}
				
				//BigDecimal size = null;
				List<BigDecimal> prices = new ArrayList<BigDecimal>(numberOfChunks);
				
				for (int j=0;j<numberOfChunks;j++) {
					Integer delta = null;
					if ((j == 0)) {
						delta = 0;
					} else {
						if (rangeInt == 0) {
							delta = 0;
						} else {
							delta = random.nextInt(rangeInt);
							
							
							
						}
					}
					
					BigDecimal price = null;
					if (delta == 0) {
						price = minPrice;
						
					} else {
						price = minPrice.add(new BigDecimal(delta).divide(BigDecimal.TEN.pow(productQuoteScale)));
					}
					prices.add(price); 
					if (higherPrice == null) {
						higherPrice = price;
						
					} else {
						if (price.compareTo(higherPrice) > 0) {
							higherPrice = price;
						}
					}
				}
				
				Collections.sort(prices, Comparator.reverseOrder());
				
				/*for (BigDecimal p: prices) {
					System.out.println("price: " + p);
				}*/
				
				
				
				
				BigDecimal price = null;
				BigDecimal size = null;
				BigDecimal leftQuote = quoteToSpend;
				
				BigDecimal totalSpentQuote = BigDecimal.ZERO;
				
				for (int p=0; p<numberOfChunks; p++) {
					price = prices.get(p);
					
					//System.out.println("quoteToSpend: " + quoteToSpend + " leftQuote: " + leftQuote + " p: " + p + " numberOfChunks: " + numberOfChunks);
					
					if (p+1<numberOfChunks) {
						BigDecimal quoteValue = price.multiply(chunckSize, accountService.getQuoteAccountMathContext()); //.setScale(productQuoteScale, RoundingMode.HALF_EVEN);
						quoteValue = quoteValue.add(quoteValue.multiply(accountService.getMakerFees()), accountService.getQuoteAccountMathContext()); //.setScale(productQuoteScale, RoundingMode.HALF_EVEN);
						leftQuote = leftQuote.subtract(quoteValue);
						totalSpentQuote = totalSpentQuote.add(quoteValue);
						size = chunckSize;
					} else {
						//System.out.println("last: leftQuote: " + leftQuote);
						
						leftQuote = leftQuote.divide(
								BigDecimal.ONE.add(accountService.getMakerFees()), accountService.getQuoteAccountMathContext())
						.setScale(productQuoteScale, RoundingMode.DOWN)
								;
						//logger.warn("t_disperseBuyOrder_: leftQuote: " + leftQuote);
						
								
						//System.out.println("last: leftQuote: " + leftQuote);
						size = leftQuote.divide(price, accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
						//logger.warn("t_disperseBuyOrder_: size: " + size);

						totalSpentQuote = totalSpentQuote.add(leftQuote);
					}
					
					//System.out.println("price: " + price + " size: " + size);
					
					MpOrder order = t_buildLimitOrder(Side.BUY, price, size, true, tif, now);
					
					//System.out.println("totalSpentQuote: " + totalSpentQuote + " order: " + order);
					
					
					if (order!= null) limitOrders.add(order);
				}
				
				
			}
			
		
			
			return limitOrders;
		
	}
	
	
	protected List<MpOrder> t_disperseBuyOrder(BigDecimal quoteToSpend,
			BigDecimal minPrice, BigDecimal maxPrice,
			int numberOfChunks, boolean requiresAtMaxPriceOrder, TimeInForce tif, Instant now) {
		
		List<MpOrder> limitOrders = new ArrayList<MpOrder>();
		
		
		if (quoteToSpend != null) {
			
			// BUY
			
			// 1. define price
			
			BigDecimal range = maxPrice.subtract(minPrice);
			Integer rangeInt = (range.multiply(BigDecimal.TEN.pow(productQuoteScale))).intValue();
			
			if (rangeInt <0) {
				//System.out.println("t_buildDispersedOrders: minPrice: " + minPrice + " maxPrice: " + maxPrice + " side: " + Side.BUY + " numberOfChunks: " + numberOfChunks + " quote: " + quoteToSpend + "  requiresAtMarketPriceOrder: " + requiresAtMaxPriceOrder + " attempt to use negative rangeInt: " + rangeInt + " setting to 0");
				rangeInt = 0;
			}
			
			
			BigDecimal higherPrice = bid;
			
				// minSize for a chunk (from higherPrice)
				BigDecimal minSize = quoteToSpend.divide(higherPrice, accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
				BigDecimal chunckSize = minSize.divide(new BigDecimal(numberOfChunks), accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
				if (minSize.compareTo(productBaseMinSize) > 0) {
					
					while (
							
							(chunckSize.compareTo(productBaseMinSize)<0)
							|| (numberOfChunks<1)
							
							) {
						numberOfChunks = numberOfChunks -1;
						chunckSize = minSize.divide(new BigDecimal(numberOfChunks), accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
					
					
					}
				}
				
				//BigDecimal size = null;
				List<BigDecimal> prices = new ArrayList<BigDecimal>(numberOfChunks);
				
				for (int j=0;j<numberOfChunks;j++) {
					Integer delta = null;
					if ((j == 0) && requiresAtMaxPriceOrder) {
						delta = 0;
					} else {
						if (rangeInt == 0) {
							delta = 0;
						} else {
							delta = random.nextInt(rangeInt);
							
							
							
						}
					}
					
					BigDecimal price = null;
					if (delta == 0) {
						price = maxPrice;
						
					} else {
						price = minPrice.add(new BigDecimal(delta).divide(BigDecimal.TEN.pow(productQuoteScale)));
					}
					prices.add(price); 
					if (higherPrice == null) {
						higherPrice = price;
						
					} else {
						if (price.compareTo(higherPrice) > 0) {
							higherPrice = price;
						}
					}
				}
				
				Collections.sort(prices, Comparator.reverseOrder());
				
				/*for (BigDecimal p: prices) {
					System.out.println("price: " + p);
				}*/
				
				
				
				
				BigDecimal price = null;
				BigDecimal size = null;
				BigDecimal leftQuote = quoteToSpend;
				
				BigDecimal totalSpentQuote = BigDecimal.ZERO;
				
				for (int p=0; p<numberOfChunks; p++) {
					price = prices.get(p);
					
					//System.out.println("quoteToSpend: " + quoteToSpend + " leftQuote: " + leftQuote + " p: " + p + " numberOfChunks: " + numberOfChunks);
					
					if (p+1<numberOfChunks) {
						BigDecimal quoteValue = price.multiply(chunckSize, accountService.getQuoteAccountMathContext()); //.setScale(productQuoteScale, RoundingMode.HALF_EVEN);
						quoteValue = quoteValue.add(quoteValue.multiply(accountService.getMakerFees()), accountService.getQuoteAccountMathContext()); //.setScale(productQuoteScale, RoundingMode.HALF_EVEN);
						leftQuote = leftQuote.subtract(quoteValue);
						totalSpentQuote = totalSpentQuote.add(quoteValue);
						size = chunckSize;
					} else {
						//System.out.println("last: leftQuote: " + leftQuote);
						
						leftQuote = leftQuote.divide(
								BigDecimal.ONE.add(accountService.getMakerFees()), accountService.getQuoteAccountMathContext())
						.setScale(productQuoteScale, RoundingMode.DOWN)
								;
						//logger.warn("t_disperseBuyOrder_: leftQuote: " + leftQuote);
						
								
						//System.out.println("last: leftQuote: " + leftQuote);
						size = leftQuote.divide(price, accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
						//logger.warn("t_disperseBuyOrder_: size: " + size);

						totalSpentQuote = totalSpentQuote.add(leftQuote);
					}
					
					//System.out.println("price: " + price + " size: " + size);
					
					MpOrder order = t_buildLimitOrder(Side.BUY, price, size, true, tif, now);
					
					//System.out.println("totalSpentQuote: " + totalSpentQuote + " order: " + order);
					
					
					if (order!= null) limitOrders.add(order);
				}
				
				
			}
			
		/*BigDecimal total = BigDecimal.ZERO;
			
			for (MpOrder order: limitOrders) {
				
				total = total.add(calcOrderQuoteValue(order, true));
				System.out.println(printTraderOrder(order));
			}
			
			logger.warn("t_disperseBuyOrder total: " + total + " chunks: " + numberOfChunks);
		*/
			
			
			return limitOrders;
		
	}
	
	protected List<MpOrder> t_disperseSellOrder(BigDecimal sizeToSpend,
			BigDecimal minPrice, BigDecimal maxPrice,
			int numberOfChunks, boolean requiresAtMinPriceOrder, TimeInForce tif, Instant now) {
		
		
		List<MpOrder> limitOrders = new ArrayList<MpOrder>();
		
		
		
		if (sizeToSpend != null) {
			
			// SELL
			
			// 1. define price
			
			BigDecimal range = maxPrice.subtract(minPrice);
			Integer rangeInt = (range.multiply(BigDecimal.TEN.pow(productQuoteScale))).intValue();
			
			if (rangeInt <0) {
				//System.out.println("t_buildDispersedOrders: minPrice: " + minPrice + " maxPrice: " + maxPrice + " side: " + Side.BUY + " numberOfChunks: " + numberOfChunks + " size: " + sizeToSpend + "  requiresAtMinPriceOrder: " + requiresAtMinPriceOrder + " attempt to use negative rangeInt: " + rangeInt + " setting to 0");
				rangeInt = 0;
			}
			
					//BigDecimal size = null;
				List<BigDecimal> prices = new ArrayList<BigDecimal>(numberOfChunks);
				BigDecimal higherPrice = null;
				
				for (int j=0;j<numberOfChunks;j++) {
					Integer delta = null;
					if ((j == 0) && requiresAtMinPriceOrder) {
						delta = 0;
					} else {
						if (rangeInt == 0) {
							delta = 0;
						} else {
							
							delta = random.nextInt(rangeInt);
							
							
							
						}
					}
					
					BigDecimal price = null;
					if (delta == 0) {
						price = minPrice;
						
					} else {
						price = minPrice.add(new BigDecimal(delta).divide(BigDecimal.TEN.pow(productQuoteScale)));
					}
					prices.add(price); 
					if (higherPrice == null) {
						higherPrice = price;
						
					} else {
						if (price.compareTo(higherPrice) > 0) {
							higherPrice = price;
						}
					}
				}
				
				Collections.sort(prices);
				
				for (BigDecimal p: prices) {
					//System.out.println("price: " + p);
				}
				
				
				
				if (sizeToSpend.compareTo(productBaseMinSize) > 0) {
					BigDecimal chunckSize = sizeToSpend.divide(new BigDecimal(numberOfChunks), accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
					
					while (
							
							(chunckSize.compareTo(productBaseMinSize)<0)
							|| (numberOfChunks<1)
							
							) {
						numberOfChunks = numberOfChunks -1;
						chunckSize = sizeToSpend.divide(new BigDecimal(numberOfChunks), accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
					
					
					}
					
					BigDecimal price = null;
					BigDecimal size = null;
					BigDecimal leftSize = sizeToSpend;
					
					BigDecimal totalSpentSize = BigDecimal.ZERO;
					
					for (int p=0; p<numberOfChunks; p++) {
						price = prices.get(p);
						
						//System.out.println("baseToSpend: " + sizeToSpend + " leftSize: " + leftSize + " p: " + p + " numberOfChunks: " + numberOfChunks);
						
						if (p+1<numberOfChunks) {
							leftSize = leftSize.subtract(chunckSize);
							totalSpentSize = totalSpentSize.add(chunckSize);
							size = chunckSize;
						} else {
							
							size = leftSize;
							leftSize = leftSize.subtract(chunckSize);
							totalSpentSize = totalSpentSize.add(leftSize);
						}
						
						//System.out.println("price: " + price + " size: " + size);
						
						MpOrder order = t_buildLimitOrder(Side.SELL, price, size, true, tif, now);
						
						//System.out.println("totalSpentSize: " + totalSpentSize );
						
						
						if (order!= null) limitOrders.add(order);
					}
					
				}
				
				
			}
					
			
			
			return limitOrders; 
		
	}
	
	
	
	protected List<MpOrder> t_optimalDisperseSellOrderWithinSpread(BigDecimal sizeToSpend,
			BigDecimal minPrice, BigDecimal maxPrice,
			int numberOfChunks, TimeInForce tif, Instant now) {
		
		
		List<MpOrder> limitOrders = new ArrayList<MpOrder>();
		
		
		
		if (sizeToSpend != null) {
			
			// SELL
			
			// 1. define price
			
			BigDecimal range = maxPrice.subtract(minPrice);
			Integer rangeInt = (range.multiply(BigDecimal.TEN.pow(productQuoteScale))).intValue();
			
			if (rangeInt <0) {
				//System.out.println("t_buildDispersedOrders: minPrice: " + minPrice + " maxPrice: " + maxPrice + " side: " + Side.BUY + " numberOfChunks: " + numberOfChunks + " size: " + sizeToSpend + "  requiresAtMinPriceOrder: " + requiresAtMinPriceOrder + " attempt to use negative rangeInt: " + rangeInt + " setting to 0");
				rangeInt = 0;
			}
			
					//BigDecimal size = null;
				List<BigDecimal> prices = new ArrayList<BigDecimal>(numberOfChunks);
				//BigDecimal higherPrice = null;
				
				for (int j=0;j<numberOfChunks;j++) {
					Integer delta = null;
					if ((j == 0)) {
						delta = 0;
					} else {
						if (rangeInt == 0) {
							delta = 0;
						} else {
							
							delta = random.nextInt(rangeInt);
							
							
							
						}
					}
					
					BigDecimal price = null;
					if (delta == 0) {
						price = maxPrice;
						
					} else {
						price = maxPrice.subtract(new BigDecimal(delta).divide(BigDecimal.TEN.pow(productQuoteScale)));
					}
					prices.add(price); 
					
				}
				
				Collections.sort(prices);
				
				
				if (sizeToSpend.compareTo(productBaseMinSize) > 0) {
					BigDecimal chunckSize = sizeToSpend.divide(new BigDecimal(numberOfChunks), accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
					
					while (
							
							(chunckSize.compareTo(productBaseMinSize)<0)
							|| (numberOfChunks<1)
							
							) {
						numberOfChunks = numberOfChunks -1;
						chunckSize = sizeToSpend.divide(new BigDecimal(numberOfChunks), accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
					
					
					}
					
					BigDecimal price = null;
					BigDecimal size = null;
					BigDecimal leftSize = sizeToSpend;
					
					BigDecimal totalSpentSize = BigDecimal.ZERO;
					
					for (int p=0; p<numberOfChunks; p++) {
						price = prices.get(p);
						
						//System.out.println("baseToSpend: " + sizeToSpend + " leftSize: " + leftSize + " p: " + p + " numberOfChunks: " + numberOfChunks);
						
						if (p+1<numberOfChunks) {
							leftSize = leftSize.subtract(chunckSize);
							totalSpentSize = totalSpentSize.add(chunckSize);
							size = chunckSize;
						} else {
							
							size = leftSize;
							leftSize = leftSize.subtract(chunckSize);
							totalSpentSize = totalSpentSize.add(leftSize);
						}
						
						//System.out.println("price: " + price + " size: " + size);
						
						MpOrder order = t_buildLimitOrder(Side.SELL, price, size, true, tif, now);
						
						//System.out.println("totalSpentSize: " + totalSpentSize );
						
						
						if (order!= null) limitOrders.add(order);
					}
					
				}
				
				
			}
					
			
			
			return limitOrders; 
		
	}
	
	
	
	
	public Position position(Instant now) {
		
		
		Position p = new Position();
		p.setAction(Action.WAIT);
		
		
		boolean activeBuySignal = false;
		boolean activeSellSignal = false;
		
		if ( (_s != null) && (_s.getProduct() != null) && (_s.getProduct().equals(product))) {
			
			if (_s.getSide() != null) {
				
				switch (_s.getSide()) {
				case BUY: {
					
					activeBuySignal = _s.getTs().plus(Duration.ofSeconds(signalMaintainAfterSeconds)).compareTo(now)>0;
					
					if (activeBuySignal) {
						
						p.setAction(Action.BUY);
						p.setAccountPercentToSpend(Indicators.HUNDRED_PERCENT);
						if (_s.getTake()) {
							p.setBuyActiontype(ActionOrderType.TAKE);
						} else {
							p.setBuyActiontype(ActionOrderType.MAKE);
						}
						
					} else {
						// clear signal
						_s = null;
					}
					
					break;
				}
				case SELL: {
					
					activeSellSignal = _s.getTs().plus(Duration.ofSeconds(signalMaintainAfterSeconds)).compareTo(now)>0;
					
					if (activeSellSignal) {
						
						p.setAction(Action.SELL);
						p.setAccountPercentToSpend(Indicators.HUNDRED_PERCENT);
						if (_s.getTake()) {
							p.setSellActiontype(ActionOrderType.TAKE);
						} else {
							p.setSellActiontype(ActionOrderType.MAKE);
						}
						
					} else {
						// clear signal
						_s = null;
					}
					break;
				}
				}
				
			}
			
		}
		
		
		/*
		 * Define acceptable trade price
		 */
		
		switch (p.getAction()) {
		
		case WAIT: {
			
			break;
		}
		case BUY: {
			/*
			 * Defines buy price.
			 * 
			 * if M1 decreases, calcLow -> bid
			 * 
			 * if M1 increases, enforce bid
			 */
			//this.printAccounts();
	
			switch (p.getBuyActiontype()) {
			case MAKE: {
				
				
				Pair<BigDecimal, BigDecimal> bestRange = this.t_getMakerRange(false); 
				p.setMinBuy(bestRange.getLeft());
				p.setMaxBuy(bestRange.getRight());
				break;
			}
			
			
			case TAKE: {
				
				p.setMinBuy(_ask);
				p.setMaxBuy(_ask);
				break;
			}
			
			case MARKET: {
				
				break;
			}
			}
			
			
			break;
		}
		case SELL: {
			//logger.warn("t_WT20210319_calcPosition: WT suggests: " + action + " " + now);
			// calculate for MAKE only
			//this.printAccounts();
			switch (p.getSellActiontype()) {
			
			case MAKE: {
				
				Pair<BigDecimal, BigDecimal> bestRange = this.t_getMakerRange(true); 
				p.setMinSell(bestRange.getLeft());
				p.setMaxSell(bestRange.getRight());
				
				
				break;
			}
			
			case TAKE: {
				
				p.setMinSell(_bid);
				p.setMaxSell(_bid);
				break;
			}
			
			case MARKET: {
				
				break;
			}
			}
			
			break;
		}
		}
		
		
		//t_wtStatPersistIfNeeded(wtKpis, now);
		return p;
	}


	
	public void startTrader() {
		Uni.createFrom().item(UUID::randomUUID).emitOn(executor).subscribe().with(
                this::startTrader, Throwable::printStackTrace
        );
	}
	private static boolean startedTrader = true;
	private static Object traderLockObj = new Object();
	
	
	private void startTrader(UUID uuid) {
		logger.info("startTrader: " + uuid);
		runningTrader = true;
		
		while (startedTrader) {
			
			try {
				trade();
			} catch (Exception e) {
				logger.info("startTrader: ", e);
			}
			
			synchronized (traderLockObj) {
				try {
					
					traderLockObj.wait(1000l);
					
				} catch (Exception e) {
					logger.error("", e);
				}
			}
			
		}
		
		logger.info("startTrader: exiting cleanly");
		runningTrader = false;
	}
	
	private void trade() {
		
		/*if (_s != null)
			logger.info("trade: signal: " + _s.getSide() + " " + _s.getTake() + " from @" + _s.getTs() + " ask: " + ask + " bid: " + bid);
		*/	
		
		if ((ask != null) && (bid != null)) {
			
			
			Instant now = Instant.now();
			
			/*
			 * Calc position
			 * 
			 */
			
			Position p = this.position(now);
			
			if (p != null) {
				
				switch (p.getAction()) {
				case BUY: {
					logger.info("trade: position: buy: " + p.getBuyActiontype() + " minBuy: " + p.getMinBuy() + " maxBuy: " + p.getMaxBuy()  + " " + p.getAccountPercentToSpend());
					break;
					
				}
				case SELL: {
					
					logger.info("trade: position: sell: " + p.getSellActiontype() + " maxSell: " + p.getMaxSell() + " minSell: " + p.getMinSell()  + " " + p.getAccountPercentToSpend());
					break;
					
				}
				default: {
					break;
				}
				}
				
				
			}
			
			/*
			 * 2. kill unwanted orders
			 * 
			 */
	
			t_purgeUnwantedOrders(p, now);
	
	
			if ((p != null) && (accountService.isReady())) {
				// calc orders
	
				
				
				switch (p.getAction()) {
				
				case WAIT: {
					
					break;
				}
				case BUY: {
					
					BigDecimal toSpendQuote = accountService.t_getSpendableQuote(p.getAccountPercentToSpend(), bid);
					if ((toSpendQuote!= null) && (toSpendQuote.signum() > 0)) {
						
						
						switch (p.getBuyActiontype()) {
						case MAKE: {
							
							
							List<MpOrder> buyOrders = this.t_optimalDisperseBuyOrderWithinSpread(
									toSpendQuote,
									p.getMinBuy(),
									p.getMaxBuy(),
									makerNumberOfChunks, TimeInForce.GTC, now);
							
							if (buyOrders != null) {
								for (MpOrder order: buyOrders) {
									if (debug) logger.info("a15: placing orders: " + this.printTraderOrder(order) + " @" + now);
									this.t_placeOrder(order);
								}
							}
							break;
						}
						
						
						case TAKE: {
							
							BigDecimal leftQuote = toSpendQuote.divide(
									BigDecimal.ONE.add(accountService.getTakerFees()), accountService.getQuoteAccountMathContext())
							.setScale(productQuoteScale, RoundingMode.DOWN);
							
							BigDecimal leftSize = leftQuote.divide(p.getMaxBuy(), 
									accountService.getBaseAccountMathContext()).setScale(productBaseScale, RoundingMode.DOWN);
							
							if (leftSize.compareTo(productBaseMinSize) >= 0) {
								MpOrder order = this.t_buildLimitOrder(Side.BUY, _ask, leftSize, false, TimeInForce.IOC, now);
								if (order != null) {
									if (debug) logger.info("a15: placing TAKE order: " + this.printTraderOrder(order) + " @" + now);
									this.t_placeOrder(order);
								}
								
							} else {
								//if (debug) logger.info("a15: cannot place order, leftQuote: " + leftQuote + " @" + now);
								
							}
							
							
							break;
						}
						case MARKET: {
							
							MpOrder order = this.t_buildMarketOrder(toSpendQuote, null, now);
							if (order != null) {
								if (debug) logger.info("a15: placing MARKET order: " + this.printTraderOrder(order) + " @" + now);
								this.t_placeOrder(order);
							}
							
							
							break;
						}
						
						
						
						}
						
						
						
						
					}
					
					
					break;
				}
				
				
				case SELL: {
					
					BigDecimal toSpendBase = null;
					
					toSpendBase = accountService.t_getSpendableBase(p.getAccountPercentToSpend(), ask);
					if ((toSpendBase!= null) && (toSpendBase.compareTo(productBaseMinSize) >= 0)) {
						
						switch (p.getSellActiontype()) {
						
						case MAKE: {
							List<MpOrder> sellOrders = this.t_optimalDisperseSellOrderWithinSpread(
									toSpendBase,
									p.getMinSell(),
									p.getMaxSell(),
									makerNumberOfChunks, TimeInForce.GTC, now);
	
							if (sellOrders != null) {
								for (MpOrder order: sellOrders) {
									if (debug) logger.info("a15: placing MAKE order: " + this.printTraderOrder(order) + " @" + now);
									this.t_placeOrder(order);
								}
							}
							break;
						}
						case TAKE: {
							
							MpOrder order = this.t_buildLimitOrder(Side.SELL, _bid, toSpendBase, false, TimeInForce.IOC, now);
							if (order != null) {
								if (debug) logger.info("a15: placing TAKE order: " + this.printTraderOrder(order) + " @" + now);
								this.t_placeOrder(order);
							}
							
							break;
						}
						case MARKET: {
							
							MpOrder order = this.t_buildMarketOrder(null, toSpendBase, now);
							if (order != null) {
								if (debug) logger.info("a15: placing MARKET order: " + this.printTraderOrder(order) + " @" + now);
								this.t_placeOrder(order);
							}
							
							
							break;
						}
						}
					} else {
						//if (debug) logger.info("a15: cannot place order, leftBase: " + toSpendBase + " @" + now);
						
					}
					
					
					break;
				}
				}
				
				
			}
		}
	}

	private static boolean runningTrader = true;
	private static Object exitLockObj = new Object();
	
	public void stopTrader() {
		
		while (runningTrader) {
			startedTrader = false;
			synchronized (traderLockObj) {
				traderLockObj.notifyAll();
			}
			
			synchronized (exitLockObj) {
				try {
					
					logger.info("stop: waiting for trader thread to finish...");
					exitLockObj.wait(1000l);
					
				} catch (Exception e) {
					
				}
			}
		}
		
		
	}
	
	void onStart(@Observes StartupEvent ev) {
    	//this.startObQueueProcessor();
    	//this.startTrQueueProcessor();
		//this.startTrader();
    	
    }

    void onStop(@Observes ShutdownEvent ev) {
    	//this.stopTrader();
    	//this.stopTrQueueProcessor();
    	//this.stopObQueueProcessor();
    }

    private static Signal _s = null;
    
	public static void _s(Signal signal) {
		logger.info("_s: set signal " + signal.getSide() + " " + signal.getTake() + " " + signal.getTs());
		_s = signal;
	}
	

	
	private Pair<BigDecimal, BigDecimal> t_getMakerRange(boolean sell) {
		if (sell) {
			BigDecimal obdiff = _ask.subtract(_bid);
			if (obdiff.equals(productQuoteIncrement)) {
				return Pair.create(_ask, _ask);
			} else {
				BigDecimal ourMinAsk = _ask.subtract(obdiff.divide(Indicators.TEN, Indicators.roundIndicators)).setScale(productQuoteScale, RoundingMode.HALF_EVEN);
				return Pair.create(ourMinAsk, _ask);
			}
		} else {
			BigDecimal obdiff = _ask.subtract(_bid);
			if (obdiff.equals(productQuoteIncrement)) {
				return Pair.create(_bid, _bid);
			} else {
				BigDecimal ourMaxBid = _bid.add(obdiff.divide(Indicators.TEN, Indicators.roundIndicators)).setScale(productQuoteScale, RoundingMode.HALF_EVEN);
				return Pair.create(_bid, ourMaxBid);
			}
		}
	}
	
}
