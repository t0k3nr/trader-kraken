package org.uche.t0ken.trader.kraken.res.s;

import java.time.Instant;

import org.jboss.logging.Logger;
import org.uche.t0ken.api.trader.Signal;
import org.uche.t0ken.trader.kraken.svc.TraderService;

import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;




@Path("")
public class TraderResource {

	
	
	@Inject TraderService traderService;
	
	private static Logger logger = Logger.getLogger(TraderResource.class);

	
	@POST
	@Path("/signal")
	@Produces("application/json")
	public Response position(Signal signal
			
			) {
		if (signal == null) return Response.status(Status.BAD_REQUEST).build();
		//signal.setTs(Instant.now());
		traderService._s(signal);
		return Response.status(Status.OK).build();
		
		
	}
	
	



}