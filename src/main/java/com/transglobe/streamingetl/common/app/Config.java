package com.transglobe.streamingetl.common.app;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.builder.ToStringBuilder;


public class Config {
	
	public String sourceTableStreamingEtlHealthCdc;

	public String sourceDbDriver;
	public String sourceDbUrl;
	public String sourceDbUsername;
	public String sourceDbPassword;
	
	public String logminerDbDriver;
	public String logminerDbUrl;
	public String logminerDbUsername;
	public String logminerDbPassword;
	
	public String logminerTableStreamingEtlHealthCdc;
	public String logminerTableSupplLogSync;


	public static Config getConfig(String fileName) throws Exception {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();

		Properties prop = new Properties();
		try (InputStream input = loader.getResourceAsStream(fileName)) {

			// load a properties file
			prop.load(input);


			Config config = new Config();
			
			config.logminerDbDriver = prop.getProperty("logminer.db.driver");
			config.logminerDbUrl = prop.getProperty("logminer.db.url");
			config.logminerDbUsername = prop.getProperty("logminer.db.username");
			config.logminerDbPassword = prop.getProperty("logminer.db.password");
			
			config.logminerTableStreamingEtlHealthCdc = prop.getProperty("logminer.table.streaming_etl_health_cdc");
			config.logminerTableSupplLogSync = prop.getProperty("logminer.table.suppl_log_sync");
			
			
			/*
			config.sourceTableStreamingEtlHealthCdc = prop.getProperty("source.table.streaming_etl_health_cdc");

			config.sourceDbDriver = prop.getProperty("source.db.driver");
			config.sourceDbUrl = prop.getProperty("source.db.url");
			config.sourceDbUsername = prop.getProperty("source.db.username");
			config.sourceDbPassword = prop.getProperty("source.db.password");
			*/
			return config;
		} catch (Exception e) {
			throw e;
		} 
	}
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
