package org.uche.t0ken.trader.kraken.ws;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.uche.t0ken.api.util.JsonUtil;
import org.uche.t0ken.trader.kraken.svc.AccountService;
import org.uche.t0ken.trader.kraken.svc.KrakenService;
import org.uche.t0ken.trader.kraken.svc.TraderService;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.DeploymentException;
import jakarta.websocket.Session;

 
@ApplicationScoped
public class KrakenWsController {

	@ConfigProperty(name = "org.uche.t0ken.trader.kraken.ws.check_interval")
	Long checkInterval;
	
	@ConfigProperty(name = "org.uche.t0ken.trader.kraken.ws.timeout_interval")
	Long timeoutInterval;
	
	@ConfigProperty(name = "org.uche.t0ken.trader.kraken.ws.max_alive_delay")
	Long aliveDelay;
	
	@ConfigProperty(name = "org.uche.t0ken.trader.kraken.ws.private_uri_string")
	String privateUriString;
	
	@ConfigProperty(name = "org.uche.t0ken.trader.kraken.ws.public_uri_string")
	String publicUriString;
	
	@ConfigProperty(name = "org.uche.t0ken.trader.kraken.debug")
	Boolean debug;
	

	
	private static ExecutorService executor = Executors.newCachedThreadPool();
	
	
	private static Boolean publicConnected = null;
	private static Boolean privateConnected = null;
	
	
	@Inject
	TraderService traderService;
	@Inject
	KrakenService krakenService;
	
	@Inject
	AccountService accountService;
	
	
	Object lockObj = new Object();
	private static final Logger logger = Logger.getLogger("KrakenWsController");

	boolean started = false;
	boolean startedSimulator = false;

	private static Instant privateLastHeartbeatTs;
	private static Instant publicLastHeartbeatTs;
	
	private Session privateWsSession = null;
	private Session publicWsSession = null;
	
	
	
	void onStart(@Observes StartupEvent ev) {
		startKrakenWsManager();
		
    }
	
	void onStop(@Observes ShutdownEvent ev) {
		stopKrakenWsManager();
		
		
		logger.info("onStop: exiting cleanly");
	}
	
	public void startKrakenWsManager() {
		Uni.createFrom().item(UUID::randomUUID).emitOn(Infrastructure.getDefaultWorkerPool()).subscribe().with(
                this::krakenWsManager, Throwable::printStackTrace
        );
	}

	
	
	Object object = new Object();
	
	public Uni<Void> krakenWsManager(UUID uuid) {
		started = true;
		
		logger.info("krakenWsManager: starting: " + uuid);
		
		while (started) {
			try {
				
				boolean start = false;
				
				boolean heartbeatOk = true;
				
				if (privateLastHeartbeatTs != null) {
					if (privateLastHeartbeatTs.plusMillis(aliveDelay).compareTo(Instant.now())<0) {
						heartbeatOk = false;
						logger.info("krakenWsManager: lastPrivateLastHeartbeatTs: " + privateLastHeartbeatTs + " heartbeatOk: " + heartbeatOk);
						
					}
					
				}
				if (heartbeatOk) {
					if (publicLastHeartbeatTs != null) {
						if (publicLastHeartbeatTs.plusMillis(aliveDelay).compareTo(Instant.now())<0) {
							heartbeatOk = false;
							logger.info("krakenWsManager: lastPublicLastHeartbeatTs: " + publicLastHeartbeatTs + " heartbeatOk: " + heartbeatOk);
							
						}
						
					}
				}
				
				if (((privateWsSession == null) || (!privateWsSession.isOpen())) && ((publicWsSession == null) || (!publicWsSession.isOpen())))  {
					start = true;
				} else if ((!heartbeatOk)) {
					logger.info("krakenWsManager: lastPrivateLastHeartbeatTs: " + privateLastHeartbeatTs +  ", stopping " + uuid);
					logger.info("krakenWsManager: lastPublicLastHeartbeatTs: " + publicLastHeartbeatTs +  ", stopping " + uuid);
					// stop and start
					
					
					krakenService.stopQueues();
					traderService.stopTrader();
					
					stopPublicWs();
					stopPrivateWs();
					
					traderService.stopTrQueueProcessor();
					traderService.stopObQueueProcessor();
					accountService.stopMpAccountUpdater();
					start = true;
				}
				
				// accounts
				if (start) {
					accountService.gatherAccountIds();
					accountService.setAccountUpdaterStarted();
					accountService.startMpAccountUpdater();
					
					while (!accountService.isReady()) {
						synchronized (object) {
							try {
								logger.info("krakenWsManager: waiting for account to be loaded...");
								object.wait(1000l);
							} catch (Exception e) {
								
							}
						}
					}
					
				}
				
				// websocket
				if (start) {	
					
					// create order maps only, order snapshot received in websocket
					traderService.syncOrders();
					
					
					logger.info("krakenWsManager: starting gdax websocket...");
					publicConnected = null;
					privateConnected = null;
					
					startPrivateWs();
					startPublicWs();
					
					
					while ((publicConnected == null) || (privateConnected == null)) {
						synchronized (object) {
							try {
								logger.info("krakenWsManager: waiting for all WS to connect...");
								object.wait(1000l);
							} catch (Exception e) {
								
							}
						}
					}
					
					privateLastHeartbeatTs = Instant.now();
					publicLastHeartbeatTs = Instant.now();
					
					
					Instant ts = null;
					while ((((ts = traderService.getObInstantOfFirstObjectInQueue()) == null)) 
						&& (privateLastHeartbeatTs.plusMillis(aliveDelay).compareTo(Instant.now())>0)
						
						&& (publicLastHeartbeatTs.plusMillis(aliveDelay).compareTo(Instant.now())>0)) 
					
					{
						
						if ((publicConnected == null) ||(!publicConnected) || (privateConnected == null) ||(!privateConnected))  {
							throw new Exception("Connexion Error");
						}
						
						
						synchronized (object) {
							try {
								logger.info("krakenWsManager: waiting for first object in orderbook queue...");
								object.wait(1000l);
							} catch (Exception e) {
								
							}
						}
					}
					
					logger.info("krakenWsManager: waiting for first object in orderbook queue: found object, ts: " + ts);
					
					
					
				}
				
				
				
				
				// ob processing
				if (start) {
					traderService.setStartedObQueueProcessor();
					traderService.startObQueueProcessor();
				}	
				
				
				// trading queues
				
				if (start) {
					traderService.setTrStartedQueueProcessor();
					traderService.startTrQueueProcessor();
				}
				
				if (start) {
					traderService.setStartedTrader();
					traderService.startTrader();
				}
				
				if (start) {
					krakenService.setStarted();
					krakenService.startQueues();
				}
				traderService.printTraderSellOrders();
				traderService.printTraderBuyOrders();
				
				synchronized (lockObj) {
					try{
						if (debug) {
							//logger.info("krakenWsManager: " + uuid + " sleeping " + checkInterval + " (checkInterval)");
						}
						
						lockObj.wait(checkInterval);
					} catch(InterruptedException e2){
						logger.error("krakenWsManager: interrupted");
					}

				}
				
				
			} catch (Exception e) {
				logger.error("krakenWsManager: ", e);
				synchronized (lockObj) {
					try{
						lockObj.wait(checkInterval);
					} catch(InterruptedException e2){
						logger.error("krakenWsChecker: interrupted");
					}

				}
			}
			
		
		}
		
		traderService.stopTrader();
		
		stopPrivateWs();
		stopPublicWs();
		traderService.stopTrQueueProcessor();
		traderService.stopObQueueProcessor();
		
		logger.info("krakenWsChecker: exiting " + uuid);
		
		return Uni.createFrom().voidItem();
    
	}
	

	public void setPublicConnected(Boolean c) {
		publicConnected = c;
	}

	public void setPrivateConnected(Boolean c) {
		privateConnected = c;
	}

	public void setPrivateAlive() {
		
		privateLastHeartbeatTs = Instant.now();
	}
	
	public void setPublicAlive() {
		
		publicLastHeartbeatTs = Instant.now();
	}
	
	
	public void startPrivateWs() {
		Uni.createFrom().item(UUID::randomUUID).emitOn(executor).subscribe().with(
                this::startPrivateWs, Throwable::printStackTrace
        );
	}

	public void startPublicWs() {
		Uni.createFrom().item(UUID::randomUUID).emitOn(executor).subscribe().with(
                this::startPublicWs, Throwable::printStackTrace
        );
	}

	
	private void startPublicWs(UUID uuid) {
		
		logger.info("startPublicWs: entering " + uuid);
		
		
        URI uri;
		try {
			uri = new URI(publicUriString);
			try {

				jakarta.websocket.WebSocketContainer container = ContainerProvider.getWebSocketContainer();
				
				container.setAsyncSendTimeout(2000l);
				container.setDefaultMaxSessionIdleTimeout(2000l);
				
				logger.info("startPublicWs: Starting ws client... : getDefaultAsyncSendTimeout: " + container.getDefaultAsyncSendTimeout() + " " + uuid);
				logger.info("startPublicWs: Starting ws client... : getDefaultMaxSessionIdleTimeout: " + container.getDefaultMaxSessionIdleTimeout() + " " + uuid);
				
				
				publicWsSession = container.connectToServer(KrakenPublicWsClient.class, uri);
				
				
			} catch (DeploymentException e) {
				publicConnected = false;
				logger.error("startPublicWs: Starting ws client... DeploymentException " + uuid, e );
			} catch (IOException e) {
				publicConnected = false;
				logger.error("startPublicWs: Starting ws client... IOError " + uuid, e );
			}

	        
		} catch (URISyntaxException e) {
			publicConnected = false;
			logger.error("startPublicWs: Starting ws client... : invalid uri: " + publicUriString + " " + uuid);
		}
		logger.info("startPublicWs: started " + uuid);
		if (publicConnected == null) publicConnected = true;
		synchronized (lockObj) {
			lockObj.notifyAll();
		}
		
	}
	
	
	private void startPrivateWs(UUID uuid) {
		
		logger.info("startPrivateWs: entering " + uuid);
		
		
        URI uri;
		try {
			uri = new URI(privateUriString);
			try {

				jakarta.websocket.WebSocketContainer container = ContainerProvider.getWebSocketContainer();
				
				container.setAsyncSendTimeout(2000l);
				container.setDefaultMaxSessionIdleTimeout(2000l);
				
				logger.info("startPrivateWs: Starting ws client... : getDefaultAsyncSendTimeout: " + container.getDefaultAsyncSendTimeout() + " " + uuid);
				logger.info("startPrivateWs: Starting ws client... : getDefaultMaxSessionIdleTimeout: " + container.getDefaultMaxSessionIdleTimeout() + " " + uuid);
				
				
				privateWsSession = container.connectToServer(KrakenPrivateWsClient.class, uri);
				
				
			} catch (DeploymentException e) {
				privateConnected = false;
				logger.error("startPrivateWs: Starting ws client... DeploymentException " + uuid, e );
			} catch (IOException e) {
				privateConnected = false;
				logger.error("startPrivateWs: Starting ws client... IOError " + uuid, e );
			}

	        
		} catch (URISyntaxException e) {
			privateConnected = false;
			logger.error("startPrivateWs: Starting ws client... : invalid uri: " + privateUriString + " " + uuid);
		}
		logger.info("startPrivateWs: started " + uuid);
		if (privateConnected == null) privateConnected = true;
		synchronized (lockObj) {
			lockObj.notifyAll();
		}
		
	}
	
	private void stopPrivateWs() {
		logger.info("stopPrivateWs: entering");
		//Session session = KrakenPrivateWsClient.getSession();
		if (privateWsSession != null) {
			try {
				
				privateWsSession.close();
			} catch (IOException e) {
				logger.error("stopPrivateWs: Stopping ws client... IOError", e );
			}
		} else {
			logger.info("stopPrivateWs: null session");
		}
		logger.info("stopPrivateWs: exiting");
	
	}
	
	private void stopPublicWs() {
		logger.info("stopPublicWs: entering");
		//Session session = KrakenPublicWsClient.getSession();
		if (publicWsSession != null) {
			try {
				
				publicWsSession.close();
			} catch (IOException e) {
				logger.error("stopPublicWs: Stopping ws client... IOError", e );
			}
		} else {
			logger.info("stopPublicWs: null session");
		}
		logger.info("stopPublicWs: exiting");
	
	}
	
	private static Object flushStatLockObj = new Object();
	public void stopKrakenWsManager() {
		
		started = false;
		synchronized (lockObj) {
			lockObj.notifyAll();
		}
		
		logger.info("stopKrakenWsChecker: notified");
		
	}

	public void spoolPrivateWsMessage(Object obj) throws Exception {
		String message = JsonUtil.serialize(obj, false);
		String logMessage = message.replaceAll(",\"token\":\".*\"", ",\"token\":\"*********\"");
		logger.info("spoolPrivateWsMessage: sending: " + logMessage);
		privateWsSession.getAsyncRemote().sendObject(message);
		logger.info("sendOrder: sent: " + logMessage);
	}
	

}
