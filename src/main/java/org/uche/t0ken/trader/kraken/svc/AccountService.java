package org.uche.t0ken.trader.kraken.svc;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.uche.t0ken.api.gdax.Account;
import org.uche.t0ken.api.gdax.Match;
import org.uche.t0ken.api.gdax.MpOrder;
import org.uche.t0ken.api.gdax.OrderType;
import org.uche.t0ken.api.gdax.Side;
import org.uche.t0ken.api.gdax.c.Fees;
import org.uche.t0ken.api.util.JsonUtil;
import org.uche.t0ken.commons.util.Indicators;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;





@ApplicationScoped
public class AccountService {

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
	
	
	@Inject TraderService traderService;
	@Inject KrakenService krakenService;
	
	@ConfigProperty(name = "org.uche.t0ken.kraken.account.update.interval")
	Long accountUpdateInterval;
	

	
	
	private static final Logger logger = Logger.getLogger("AccountService");

	private static ExecutorService executor = Executors.newCachedThreadPool();
	
	protected volatile static String baseAccountId = null;
	protected volatile static String quoteAccountId = null;
	
	
	protected static MathContext baseAccountMathContext = null;
	protected static MathContext quoteAccountMathContext = null;
	
	protected volatile static Long accountInconsistency = null;
	private static Object mpAccountLock = new Object();

	protected BigDecimal makerFees = null;
	protected BigDecimal takerFees = null;
	
	protected volatile Account baseAccount = null;
	protected volatile Account quoteAccount = null;
	
	protected volatile Account mpBaseAccount = null;
	protected volatile Account mpQuoteAccount = null;
	
	protected volatile Object lockAccounts = new Object();
	private static boolean startedAccountUpdater = false;
	private static boolean accountUpdaterRunning = false;

	private void getFees() {
		
		synchronized (feesLokObj) {
			Fees fees = krakenService.getFees();
			
			makerFees = new BigDecimal(fees.getMaker_fee_rate());
			takerFees = new BigDecimal(fees.getTaker_fee_rate());
			
		}
		
		//if (debug) logger.info("getFees: " + makerFees + "/" + takerFees);
		
	}
	
	
	public void printAccounts(Instant now) {
		BigDecimal quoteValue = BigDecimal.ZERO;
		BigDecimal baseValue = BigDecimal.ZERO;
		
		if (quoteAccount != null) {
			logger.info("quoteAccount: " + now) ;
			logger.info("quoteAccount: " + quoteAccount.getCurrency() + " " + quoteAccount.getBalance() + "/" + quoteAccount.getAvailable() + "/"  + quoteAccount.getHold() );
		
			if (quoteAccount.getTradePrice() != null) {
				logger.info("quoteAccount: lastTrade: " + quoteAccount.getLastTradeSize() + "@" + quoteAccount.getLastTradePrice() + " " + quoteAccount.getLastTradeTs()  + " mean: " + quoteAccount.getTradeSize() + "@" + quoteAccount.getTradePrice() );

			}
		
		}
		if (baseAccount != null) {
			logger.info("baseAccount: " + now) ;
			logger.info("baseAccount: " + baseAccount.getCurrency() + " " + baseAccount.getBalance() + "/" + baseAccount.getAvailable() + "/"  + baseAccount.getHold() );
			if (quoteAccount.getTradePrice() != null) {
				logger.info("baseAccount: lastTrade: " + baseAccount.getLastTradeSize() + "@" + baseAccount.getLastTradePrice() + " " + baseAccount.getLastTradeTs() + " mean: " + baseAccount.getTradeSize() + "@" + baseAccount.getTradePrice() );

			}
		}
		
		if ((quoteAccount != null) && (baseAccount != null)) {
			quoteValue = quoteValue.add(quoteAccount.getBalance());
			baseValue = baseValue.add(baseAccount.getBalance());
			BigDecimal bid = traderService.getBid();
			BigDecimal ask = traderService.getAsk();
			if ((bid != null) && (ask!=null)) {
				quoteValue = quoteValue.add(baseAccount.getBalance().multiply(bid.add(ask)).divide(Indicators.TWO, Indicators.roundIndicators)).setScale(productQuoteScale, RoundingMode.DOWN);;
				baseValue = baseValue.add(quoteAccount.getBalance().divide(bid.add(ask).divide(Indicators.TWO, Indicators.roundIndicators), Indicators.roundIndicators)).setScale(productBaseScale, RoundingMode.DOWN);
			}
			
			
			
			logger.info("value@" + now + ": base: " + baseValue + " quote: " + quoteValue + " bid/ask " + bid + "/" + ask);
		}
	}
	
	private String printAccount(Account account) {
		try {
			return JsonUtil.serialize(account, false);
		} catch (JsonProcessingException e) {
			logger.error("printAccount: " + account.getId(), e);
		}
		return null;
	}
	
	public MathContext getBaseAccountMathContext() {
		if (baseAccountMathContext == null) {
			baseAccountMathContext = new MathContext(baseAccountRoundingSize, RoundingMode.HALF_EVEN);
		}
		return baseAccountMathContext;
	}
	
	public MathContext getQuoteAccountMathContext() {
		if (quoteAccountMathContext == null) {
			quoteAccountMathContext = new MathContext(quoteAccountRoundingSize, RoundingMode.HALF_EVEN);
		}
		return quoteAccountMathContext;
	}
	
	public void refreshMpAccounts() {
		
		if (baseAccountId != null) mpBaseAccount = krakenService.getAccount(baseAccountId);
		if (quoteAccountId != null) mpQuoteAccount = krakenService.getAccount(quoteAccountId);
		
		
	}
	
	
	public void startMpAccountUpdater() {
		Uni.createFrom().item(UUID::randomUUID).emitOn(Infrastructure.getDefaultWorkerPool()).subscribe().with(
                this::startMpAccountUpdater, Throwable::printStackTrace
        );
	}

	
	public void setAccountInconsistencyNull() {
		//logger.warn("setAccountInconsistencyNull");
		accountInconsistency = null;
	}
	
	
	public void setAccountUpdaterStarted() {
		startedAccountUpdater = true;
	}
	
	private Uni<Void>  startMpAccountUpdater(UUID uuid) {
		accountUpdaterRunning = true;


		logger.info("startMpAccountUpdater: " + uuid);

		
		
		
		while (startedAccountUpdater) {
			try {
				getFees();
				refreshMpAccounts();
				
				
				if (inconsistency()) {
					
					clearAccounts();
					initAccounts();
					setAccountInconsistencyNull();
				}

				
				synchronized (mpAccountLock) {
					try{
						mpAccountLock.wait(accountUpdateInterval);
					} catch(InterruptedException e){
						logger.error("startMpAccountUpdater: interrupted");
					}

				}
				
				
			}

			catch (Exception e) {

				logger.error("startMpAccountUpdater: non fatal error", e);
				synchronized (mpAccountLock) {
					try{
						mpAccountLock.wait(accountUpdateInterval);
					} catch(InterruptedException e1){
						logger.error("startMpAccountUpdater: Exception ", e1);
					}

				}
			}

		}
		logger.info("startMpAccountUpdater: exiting " + uuid);
		accountUpdaterRunning = false;
		return Uni.createFrom().voidItem();

	}

	private void clearAccounts() {
		baseAccount = null;
		quoteAccount = null;
	}

	private static Object stopMpAccountUpdaterLockObj = new Object();
	
	
	public void stopMpAccountUpdater() {
		startedAccountUpdater = false;
		
		
		while (accountUpdaterRunning) {
			synchronized (mpAccountLock) {
				mpAccountLock.notifyAll();
			}
			synchronized (stopMpAccountUpdaterLockObj) {
				logger.info("stopMpAccountUpdater: waiting for updater to finish...");
				try {
					stopMpAccountUpdaterLockObj.wait(1000l);
				} catch (Exception e) {
					
				}
			}
		}
		logger.warn("stopMpAccountUpdater: terminated");
	}
	
	public void checkAccountsForInconsistencies() {
		boolean inconsistencyBase = false;
		boolean inconsistencyQuote = false;
		
		if (!compareAccounts(mpBaseAccount, baseAccount)) {
			inconsistencyBase = true;
			try {
				logger.warn("checkAccountsForInconsistencies: inconsistency: mpBaseAccount: " + JsonUtil.serialize(mpBaseAccount, false));
				logger.warn("checkAccountsForInconsistencies: inconsistency: baseAccount: " + JsonUtil.serialize(baseAccount, false));
				
			} catch (JsonProcessingException e) {
				logger.error("refreshMpAccounts: inconsistency", e);
			}
			
		} else {
			
			/*if (debug) {
			
				try {
					logger.info("checkAccountsForInconsistencies: mpBaseAccount: " + JsonUtil.serialize(mpBaseAccount, false));
					logger.info("checkAccountsForInconsistencies: baseAccount: " + JsonUtil.serialize(baseAccount, false));
					
				} catch (JsonProcessingException e) {
					logger.error("refreshMpAccounts", e);
				}
			}*/
		}
		
		
		if (!compareAccounts(mpQuoteAccount, quoteAccount)) {
			inconsistencyQuote = true;
			try {
				logger.warn("checkAccountsForInconsistencies: inconsistency: mpQuoteAccount: " + JsonUtil.serialize(mpQuoteAccount, false));
				logger.warn("checkAccountsForInconsistencies: inconsistency: quoteAccount: " + JsonUtil.serialize(quoteAccount, false));
				
			} catch (JsonProcessingException e) {
				logger.error("checkAccountsForInconsistencies: inconsistency", e);
			}
			
		} else {
			/*if (debug) {
			
				try {
					
					logger.info("checkAccountsForInconsistencies: mpQuoteAccount: " + JsonUtil.serialize(mpQuoteAccount, false));
					logger.info("checkAccountsForInconsistencies: quoteAccount: " + JsonUtil.serialize(quoteAccount, false));
				} catch (JsonProcessingException e) {
					logger.error("checkAccountsForInconsistencies", e);
				}
			}*/
		}
		if (inconsistencyBase || inconsistencyQuote) {
			if (accountInconsistency == null) accountInconsistency = System.currentTimeMillis();
		} else {
			accountInconsistency = null;
		}
		
		
	}
	
	private boolean compareAccounts(Account mpBaseAccount, Account baseAccount) {
		//logger.warn("compareAccounts: mpAccount: " + mpBaseAccount + " account: " + baseAccount);
		if (mpBaseAccount.getAvailable().compareTo(baseAccount.getAvailable()) != 0) return false;
		if (mpBaseAccount.getBalance().compareTo(baseAccount.getBalance()) != 0) return false;
		if (mpBaseAccount.getHold().compareTo(baseAccount.getHold()) != 0) return false;
		return true;
	}
	
	private void initAccounts() {
	
		baseAccount = new Account();
		baseAccount.setAvailable(mpBaseAccount.getAvailable());
		baseAccount.setBalance(mpBaseAccount.getBalance());
		baseAccount.setCurrency(mpBaseAccount.getCurrency());
		baseAccount.setHold(mpBaseAccount.getHold());
		baseAccount.setId(mpBaseAccount.getId());
		baseAccount.setProfile_id(mpBaseAccount.getProfile_id());
		
		
		quoteAccount = new Account();
		quoteAccount.setAvailable(mpQuoteAccount.getAvailable());
		quoteAccount.setBalance(mpQuoteAccount.getBalance());
		quoteAccount.setCurrency(mpQuoteAccount.getCurrency());
		quoteAccount.setHold(mpQuoteAccount.getHold());
		quoteAccount.setId(mpQuoteAccount.getId());
		quoteAccount.setProfile_id(mpQuoteAccount.getProfile_id());
		
		
		try {
			logger.warn("getAccounts: baseAccount: " + JsonUtil.serialize(baseAccount, false));
			logger.warn("getAccounts: quoteAccount: " + JsonUtil.serialize(quoteAccount, false));
		} catch (JsonProcessingException e) {
			logger.error("getAccount", e);
		}
		
			
	}
	
	public boolean inconsistency() {
		
		boolean inconsistency = false;
		
		if (baseAccount == null) return true;
		if (quoteAccount == null) return true;
		
		this.checkAccountsForInconsistencies();
		if (accountInconsistency != null) {
			if ((System.currentTimeMillis() - accountInconsistency) > (5 * accountUpdateInterval)) {
				logger.error("*********** inconsistency: Accounts ***********");
				inconsistency = true;
			}
			
		}
		
		
		return inconsistency;
	}
	
	public void gatherAccountIds() {
		
		baseAccountId = productBaseCurrency;
		quoteAccountId = productQuoteCurrency;
		
		
	}
	
	public boolean o_lockFundsForNewOrder(MpOrder order) {
		// true if funds available, else false
		
		BigDecimal specifiedFunds = order.getSpecifiedFunds();
		BigDecimal price = order.getPrice();
		BigDecimal remainingSize = order.getRemainingSize();
		BigDecimal size = order.getSize();
		Side side = order.getSide();
		OrderType type = order.getOrderType();

		switch (type) {
		
			case LIMIT: {
				
				if (side.equals(Side.SELL)) {
					if (debug) {
						logger.info("o_lockFundsForNewOrder: LIMIT SELL");
						logger.info("o_lockFundsForNewOrder: LIMIT SELL before baseAccount: " + printAccount(baseAccount));
						logger.info("o_lockFundsForNewOrder: LIMIT SELL before quoteAccount: " + printAccount(quoteAccount));
						
						
					}
					synchronized (lockAccounts) {
						
						BigDecimal newAvailable = baseAccount.getAvailable().subtract(size);
						BigDecimal newHold = baseAccount.getHold().add(remainingSize);
						
						if ((newAvailable.signum() >=0) && (newHold.compareTo(baseAccount.getBalance()) <= 0)) {
							baseAccount.setAvailable(newAvailable);
							baseAccount.setHold(newHold);
						} else {
							logger.error("o_lockFundsForNewOrder: LIMIT SELL: attempt to use negative balance for baseAccount");
							return false;
						}
						
					
					}
					if (debug) {
						logger.info("o_lockFundsForNewOrder: LIMIT SELL after baseAccount: " + printAccount(baseAccount));
						logger.info("o_lockFundsForNewOrder: LIMIT SELL after quoteAccount: " + printAccount(quoteAccount));
						
					}
					
				} else { // BUY
					if (debug) {
						logger.info("o_lockFundsForNewOrder: LIMIT BUY");
						logger.info("o_lockFundsForNewOrder: LIMIT BUY before baseAccount: " + printAccount(baseAccount));
						logger.info("o_lockFundsForNewOrder: LIMIT BUY before quoteAccount: " + printAccount(quoteAccount));
						
					}
					
					synchronized (lockAccounts) {	
						BigDecimal lockAmount = null;
						BigDecimal remainingAmount = null;
						if ((order.getPostOnly() != null) && (order.getPostOnly())) {
							logger.info("o_lockFundsForNewOrder: post only");
							//lockAmount = price.multiply(size, getQuoteAccountMathContext()).multiply(BigDecimal.ONE.add(makerFees), getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
							//remainingAmount = price.multiply(remainingSize, getQuoteAccountMathContext()).multiply(BigDecimal.ONE.add(makerFees), getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
							lockAmount = price.multiply(size, getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
							remainingAmount = price.multiply(remainingSize, getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
						} else {
							logger.info("o_lockFundsForNewOrder: not post only");
							lockAmount = price.multiply(size, getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
							remainingAmount = price.multiply(remainingSize, getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
						}
						
						BigDecimal newAvailable = quoteAccount.getAvailable().subtract(lockAmount);
						BigDecimal newHold = quoteAccount.getHold().add(remainingAmount);
						
						if ((newAvailable.signum() >=0) && (newHold.compareTo(quoteAccount.getBalance()) <= 0)) {
							quoteAccount.setAvailable(newAvailable);
							quoteAccount.setHold(newHold);
						} else {
							logger.error("o_lockFundsForNewOrder: LIMIT BUY: attempt to use negative balance for quoteAccount, newHold: " + newHold + " newAvailable: " + newAvailable);
							return false;
						}
						
						
					}
					if (debug) {
						logger.info("o_lockFundsForNewOrder: LIMIT BUY after baseAccount: " + printAccount(baseAccount));
						logger.info("o_lockFundsForNewOrder: LIMIT BUY after quoteAccount: " + printAccount(quoteAccount));
					}
					
				}
				break;
			}
			case MARKET: {
				if (debug) {
					logger.info("o_lockFundsForNewOrder: MARKET SELL");

				}
				if (side.equals(Side.SELL)) {
					if (debug) {
						logger.info("o_lockFundsForNewOrder: MARKET SELL");
						logger.info("o_lockFundsForNewOrder: MARKET SELL before baseAccount: " + printAccount(baseAccount));
						logger.info("o_lockFundsForNewOrder: MARKET SELL before quoteAccount: " + printAccount(quoteAccount));
						
					}
					
					synchronized (lockAccounts) {
						BigDecimal newAvailable = baseAccount.getAvailable().subtract(size);
						BigDecimal newHold = baseAccount.getHold().add(remainingSize);
						
						if ((newAvailable.signum() >=0) && (newHold.compareTo(baseAccount.getBalance()) <= 0)) {
							baseAccount.setAvailable(newAvailable);
							baseAccount.setHold(newHold);
						} else {
							logger.error("o_lockFundsForNewOrder: MARKET SELL: attempt to use negative balance for baseAccount");
							return false;
						}
						
					}
					if (debug) {
						logger.info("o_lockFundsForNewOrder: MARKET SELL after baseAccount: " + printAccount(baseAccount));
						logger.info("o_lockFundsForNewOrder: MARKET SELL after quoteAccount: " + printAccount(quoteAccount));
						
					}
					
					
				} else {
					if (debug) {
						logger.info("o_lockFundsForNewOrder: MARKET BUY");
						logger.info("o_lockFundsForNewOrder: MARKET BUY before baseAccount: " + printAccount(baseAccount));
						logger.info("o_lockFundsForNewOrder: MARKET BUY before quoteAccount: " + printAccount(quoteAccount));
						
					}
					synchronized (lockAccounts) {
						
						BigDecimal newAvailable = quoteAccount.getAvailable().subtract(specifiedFunds);
						BigDecimal newHold = quoteAccount.getHold().add(specifiedFunds);
						
						if ((newAvailable.signum() >=0) && (newHold.compareTo(quoteAccount.getBalance()) <= 0)) {
							quoteAccount.setAvailable(newAvailable);
							quoteAccount.setHold(newHold);
						} else {
							logger.error("o_lockFundsForNewOrder: MARKET BUY: attempt to use negative balance for quoteAccount");
							return false;
						}
						
						
						
						
						
					}
					if (debug) {
						logger.info("o_lockFundsForNewOrder: MARKET BUY after baseAccount: " + printAccount(baseAccount));
						logger.info("o_lockFundsForNewOrder: MARKET BUY after quoteAccount: " + printAccount(quoteAccount));
						
					}
					
				}
				
				break;
			}
			case STOP: {
				return false;
				
			}
		}
		
		return true;
	}
	public boolean _sell_update_for_match(Match match, MpOrder order, Boolean taker, BigDecimal quoteAmount, BigDecimal matchFees) {
		
		boolean profit = false;
		
		if (debug) {
			logger.info("_sell_update_for_match: p: " + match.getPrice() + " s: " + match.getSize() + " " + match.getSide() + " taker: " + taker + " quoteAmount: " + quoteAmount + " matchFees: " +  matchFees);
		}
		synchronized(lockAccounts) {
			
			
			// add size to base balances
			
			if (quoteAccount.getTradePrice() != null) {
				if (quoteAccount.getTradePrice().compareTo(match.getPrice()) < 0) {
					profit = true;
				}
			}
			
			quoteAccount.setAvailable(quoteAccount.getAvailable().add(quoteAmount).subtract(matchFees));
			quoteAccount.setBalance(quoteAccount.getBalance().add(quoteAmount).subtract(matchFees));
			// unlock amount from base hold
			baseAccount.setBalance(baseAccount.getBalance().subtract(match.getSize()));
			baseAccount.setHold(baseAccount.getHold().subtract(match.getSize()));
			
			
			
			 // Calculate mean prices for SELL
			 
			
			baseAccount.setLastTradePrice(match.getPrice());
			baseAccount.setLastTradeSize(match.getSize());
			baseAccount.setLastTradeTs(match.getTime());
			
			if (baseAccount.getTradePrice() == null) {
				baseAccount.setTradePrice(baseAccount.getLastTradePrice());
				baseAccount.setTradeSize(baseAccount.getLastTradeSize());
			} else {
				BigDecimal totalSize = baseAccount.getTradeSize().add(match.getSize());
				BigDecimal totalPrice = ((baseAccount.getTradeSize().multiply(baseAccount.getTradePrice(), this.getQuoteAccountMathContext())
						.add(match.getSize().multiply(match.getPrice(), this.getQuoteAccountMathContext()))
						)
						.divide(totalSize, this.getQuoteAccountMathContext())) .setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
				baseAccount.setTradePrice(totalPrice);
				baseAccount.setTradeSize(totalSize);
			}
			
			
			
			
			
			if (quoteAccount.getTradeSize() != null) {
				if (quoteAccount.getTradeSize().compareTo(match.getSize()) > 0) {
					
					if (match.getPrice().compareTo(quoteAccount.getTradePrice()) >0) {
						if (debug) logger.info("_sell_update_for_match: SELL PROFIT: match: " + match.getSize() + "@" + match.getPrice() + " traded " + quoteAccount.getTradeSize() + "@" + quoteAccount.getTradePrice());
					} else {
						if (debug) logger.info("_sell_update_for_match: SELL LOSS: match: " + match.getSize() + "@" + match.getPrice() + " traded " + quoteAccount.getTradeSize() + "@" + quoteAccount.getTradePrice());

					}
					
					quoteAccount.setTradeSize(quoteAccount.getTradeSize().subtract(match.getSize()));
					
				} else {
					quoteAccount.setTradeSize(BigDecimal.ZERO);
					//quoteAccount.setTradePrice(BigDecimal.ZERO);
				}
			} else {
				quoteAccount.setTradeSize(BigDecimal.ZERO);
				quoteAccount.setTradePrice(BigDecimal.ZERO);
			}
			
			
		}
		if (debug) {
			logger.info("_sell_update_for_match: p: " + match.getPrice() + " s: " + match.getSize() + " " + match.getSide() + " taker: " + taker + " quoteAmount: " + quoteAmount + " matchFees: " +  matchFees + " profit: " + profit);
		}
		return profit;
	}
	
	public boolean _buy_update_for_match(Match match, MpOrder order, Boolean taker, BigDecimal quoteAmount, BigDecimal matchFees) {
		
		if (debug) {
			logger.info("_buy_update_for_match: p: " + match.getPrice() + " s: " + match.getSize() + " " + match.getSide() + " taker: " + taker + " quoteAmount: " + quoteAmount + " matchFees: " +  matchFees);
		}
		
		
		boolean profit = false;
		
		BigDecimal lockedFees = null;
		
		BigDecimal originalQuoteAmount = null;
		
		if (order.getPrice() != null) {
			originalQuoteAmount = order.getPrice().multiply(match.getSize(), getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
		} else {
			// MARKET
			originalQuoteAmount = match.getPrice().multiply(match.getSize(), getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);

		}
		
		if ((order.getPostOnly() != null) && (order.getPostOnly())) {
			// maker fees
			lockedFees = BigDecimal.ZERO; // originalQuoteAmount.multiply(makerFees, getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
		} else {
			// taker fees
			
			lockedFees = BigDecimal.ZERO;// originalQuoteAmount.multiply(takerFees, getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
			
		}
		if (debug) logger.info("lockedFees: " + lockedFees + " matchFees: " + matchFees);
		
		synchronized(lockAccounts) {
			
			// if limit take order and match of a lower price
			//BigDecimal requestedPrice = order.getPrice();
			//BigDecimal requestedPriceQuoteAmount = requestedPrice.multiply(size, getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
			
			
			
			if (baseAccount.getTradePrice() != null) {
				if (baseAccount.getTradePrice().compareTo(match.getPrice()) > 0) {
					profit = true;
				}
			}
			// add size to base balances
			baseAccount.setAvailable(baseAccount.getAvailable().add(match.getSize()));
			baseAccount.setBalance(baseAccount.getBalance().add(match.getSize()));
			
			// unlock equivalent amount from quote hold
			if (taker) {
				quoteAccount.setHold(quoteAccount.getHold().subtract(originalQuoteAmount)); //.subtract(lockedFees)
				quoteAccount.setAvailable(quoteAccount.getAvailable().add(originalQuoteAmount).subtract(quoteAmount).subtract(matchFees).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN)); // .add(lockedFees);

			} else {
				quoteAccount.setHold(quoteAccount.getHold().subtract(quoteAmount)); //.subtract(lockedFees)
				quoteAccount.setAvailable(quoteAccount.getAvailable().add(originalQuoteAmount).subtract(quoteAmount).subtract(matchFees).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN)); //.add(lockedFees)
					
			}
			quoteAccount.setBalance(quoteAccount.getBalance().subtract(quoteAmount).subtract(matchFees).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN));
			
			
			
			 // Calculate mean prices for BUY
			
			
			quoteAccount.setLastTradePrice(match.getPrice());
			quoteAccount.setLastTradeSize(match.getSize());
			quoteAccount.setLastTradeTs(match.getTime());
			
			if (quoteAccount.getTradePrice() == null) {
				quoteAccount.setTradePrice(quoteAccount.getLastTradePrice());
				quoteAccount.setTradeSize(quoteAccount.getLastTradeSize());
			} else {
				BigDecimal totalSize = quoteAccount.getTradeSize().add(match.getSize());
				BigDecimal totalPrice = ((quoteAccount.getTradeSize().multiply(quoteAccount.getTradePrice(), this.getQuoteAccountMathContext())
						.add(match.getSize().multiply(match.getPrice(), this.getQuoteAccountMathContext()))
						)
						.divide(totalSize, this.getQuoteAccountMathContext())).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
				quoteAccount.setTradePrice(totalPrice);
				quoteAccount.setTradeSize(totalSize);
			}
			
			
			if (baseAccount.getTradeSize() != null) {
				if (baseAccount.getTradeSize().compareTo(match.getSize()) > 0) {
					if (match.getPrice().compareTo(baseAccount.getTradePrice()) <0) {
						if (debug) logger.info("_buy_update_for_match: BUY PROFIT: match: " + match.getSize() + "@" + match.getPrice() + " traded " + baseAccount.getTradeSize() + "@" + baseAccount.getTradePrice());
					} else {
						if (debug) logger.info("_buy_update_for_match: BUY LOSS: match: " + match.getSize() + "@" + match.getPrice() + " traded " + baseAccount.getTradeSize() + "@" + baseAccount.getTradePrice());

					}
					
					baseAccount.setTradeSize(baseAccount.getTradeSize().subtract(match.getSize()));
				} else {
					baseAccount.setTradeSize(BigDecimal.ZERO);
					//baseAccount.setTradePrice(BigDecimal.ZERO);
				}
			} else {
				baseAccount.setTradeSize(BigDecimal.ZERO);
				baseAccount.setTradePrice(BigDecimal.ZERO);
			}
			
		}
		
		if (debug) {
			logger.info("_buy_update_for_match: p: " + match.getPrice() + " s: " + match.getSize() + " " + match.getSide() + " taker: " + taker + " quoteAmount: " + quoteAmount + " matchFees: " +  matchFees + " profit: " + profit);
		}
		
		
		return profit;
	}
	
	
	public void _buy_release_remaining_funds(MpOrder current, BigDecimal doneRemainingSize) {
		
		if (debug) {
			logger.info("_buy_release_remaining_funds: p: " + current.getPrice() + " s: " + current.getSize() + " " + current.getSide() + " maker: " + current.getPostOnly() + " doneRemainingSize: " + doneRemainingSize);
		}
		
		
		synchronized (lockAccounts) {
			
			if ((current.getPostOnly() != null) && (current.getPostOnly())) {
				BigDecimal toUnHold = doneRemainingSize.multiply(current.getPrice(), getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
				quoteAccount.setAvailable(quoteAccount.getAvailable().add(toUnHold));
				quoteAccount.setHold(quoteAccount.getHold().subtract(toUnHold));
			} else {
				switch (current.getOrderType()) {
				case LIMIT: {
					BigDecimal toUnHold = doneRemainingSize.multiply(current.getPrice(), getQuoteAccountMathContext()).setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
					quoteAccount.setAvailable(quoteAccount.getAvailable().add(toUnHold));
					quoteAccount.setHold(quoteAccount.getHold().subtract(toUnHold));
					break;
				} case MARKET: {
					
					if (debug) {
						logger.warn("funds: " + current.getFunds() + " specifiedFunds " + current.getSpecifiedFunds());
						logger.warn("executedValue: " + current.getExecutedValue() + " filledSize " + current.getFilledSize());
						logger.warn("fillFees: " + current.getFillFees() + " filledSize ");
						
					}
					
					
					BigDecimal fundAndFees = current.getExecutedValue().setScale(quoteAccountRoundingScale, RoundingMode.HALF_EVEN);
					
					BigDecimal toUnHold = current.getSpecifiedFunds().subtract(fundAndFees);
					quoteAccount.setAvailable(quoteAccount.getAvailable().add(toUnHold));
					quoteAccount.setHold(quoteAccount.getHold().subtract(toUnHold));
					break;
				}
				case STOP: {
					break;
				}
				}
			}
			
		}
		if (debug) {
			logger.info("_buy_release_remaining_funds: p: " + current.getPrice() + " s: " + current.getSize() + " " + current.getSide() + " maker: " + current.getPostOnly() + " doneRemainingSize: " + doneRemainingSize + ", E");
		}
	}
	
	public void _sell_release_remaining_funds(MpOrder current, BigDecimal doneRemainingSize) {
		
		if (debug) {
			logger.info("_sell_release_remaining_funds: p: " + current.getPrice() + " s: " + current.getSize() + " " + current.getSide() + " maker: " + current.getPostOnly() + " doneRemainingSize: " + doneRemainingSize);
		}
		
		
		synchronized (lockAccounts) {
			baseAccount.setAvailable(baseAccount.getAvailable().add(doneRemainingSize));
			baseAccount.setHold(baseAccount.getHold().subtract(doneRemainingSize));
		}
		
		if (debug) {
			logger.info("_sell_release_remaining_funds: p: " + current.getPrice() + " s: " + current.getSize() + " " + current.getSide() + " maker: " + current.getPostOnly() + " doneRemainingSize: " + doneRemainingSize + ", E");
		}
	}


	private Object feesLokObj = new Object();
	
	public BigDecimal getMakerFees() {
		
		
		if (makerFees == null) {
			this.getFees();
		}	
		
		
		return makerFees;
	}


	public BigDecimal getTakerFees() {
		if (takerFees == null) {
			this.getFees();
		}
		return takerFees;
	}
	
	

	public BigDecimal t_getSpendableQuote(BigDecimal accountPercentToSpend, BigDecimal bid) {
		if (quoteAccount == null)  return null;
		if (baseAccount == null)  return null;
		if (bid == null) return null;
		BigDecimal baseQuote = baseAccount.getBalance().multiply(bid, this.getQuoteAccountMathContext());
		BigDecimal totalQuoteValue = quoteAccount.getBalance().add(baseQuote);
		BigDecimal wishedSpentQuote = totalQuoteValue.multiply(accountPercentToSpend, this.getQuoteAccountMathContext());
		BigDecimal spendableQuote = null;
		
		if (baseQuote.compareTo(wishedSpentQuote) >= 0) {
			// baseQuote already greater than wishedSpentQuote
			spendableQuote = BigDecimal.ZERO;
		} else {
			BigDecimal leftToSpend = wishedSpentQuote.subtract(baseQuote);
			if (leftToSpend.compareTo(quoteAccount.getHold()) <= 0) {
				spendableQuote = BigDecimal.ZERO;
			}
			else {
				spendableQuote = (leftToSpend.subtract(quoteAccount.getHold()).setScale(quoteAccountRoundingScale, RoundingMode.DOWN));
			}
			
		}
		
		return spendableQuote;
	}
	
	
	public BigDecimal t_getSpendableBase(BigDecimal accountPercentToSpend, BigDecimal ask) {
		if (quoteAccount == null)  return null;
		if (baseAccount == null)  return null;
		if (ask == null) return null;
		
		BigDecimal quoteBase = quoteAccount.getBalance().divide(ask, this.getBaseAccountMathContext());
		BigDecimal totalBaseValue = baseAccount.getBalance().add(quoteBase);
		BigDecimal wishedSpentBase = totalBaseValue.multiply(accountPercentToSpend, this.getBaseAccountMathContext());
		BigDecimal spendableBase = null;
		
		if (quoteBase.compareTo(wishedSpentBase) >= 0) {
			// quoteBase already greater than wishedSpentBase
			spendableBase = BigDecimal.ZERO;
		} else {
			// 
			BigDecimal leftToSpend = wishedSpentBase.subtract(quoteBase);
			if (leftToSpend.compareTo(baseAccount.getHold()) <= 0) {
				spendableBase = BigDecimal.ZERO;
			}
			else {
				spendableBase = (leftToSpend.subtract(baseAccount.getHold()).setScale(baseAccountRoundingScale, RoundingMode.DOWN));
			}
		}
		
		return spendableBase;
		
		
	}


	public boolean isReady() {
		if (quoteAccount == null)  return false;
		if (baseAccount == null)  return false;
		return true;
	}


	

}
