package com.swisscom.api.model;

public class ElasticCredentials {
	private String elasticSearchHost ;
	public int elasticSearchPort ;
	public String elasticSearchPassword ;
	public String elasticSearchUsername ;
	
	public String getElasticSearchHost() {
		return elasticSearchHost;
	}
	public void setElasticSearchHost(String elasticSearchHost) {
		this.elasticSearchHost = elasticSearchHost;
	}
	public String getElasticSearchPassword() {
		return elasticSearchPassword;
	}
	public int getElasticSearchPort() {
		return elasticSearchPort;
	}
	public void setElasticSearchPort(int elasticSearchPort) {
		this.elasticSearchPort = elasticSearchPort;
	}
	public void setElasticSearchPassword(String elasticSearchPassword) {
		this.elasticSearchPassword = elasticSearchPassword;
	}
	public String getElasticSearchUsername() {
		return elasticSearchUsername;
	}
	public void setElasticSearchUsername(String elasticSearchUsername) {
		this.elasticSearchUsername = elasticSearchUsername;
	}

}
