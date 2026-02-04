package org.uche.t0k3nr.trader.kraken.json;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ChannelName implements Serializable {
	EXECUTIONS("executions"),
	HEARTBEAT("heartbeat"),
	BOOK("book"),
	
	
	;
	
	
	private ChannelName(String index){
		this.index = index;
	}

	private String index;

	public String getIndex(){
		return this.index;
	}

	public static ChannelName getEnum(String index){
		if (index == null)
	return null;

		if (index.equals("executions")) return EXECUTIONS;
		else if (index.equals("heartbeat")) return HEARTBEAT;
		else if (index.equals("book")) return BOOK;
		
		else return null;
		
	}
	
	@JsonValue 
	public String getValue() {
		return index;
	}
}
