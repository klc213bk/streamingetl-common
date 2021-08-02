package com.transglobe.streamingetl.common.app;

import java.io.Console;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthCheckApp {
	private static final Logger logger = LoggerFactory.getLogger(HealthCheckApp.class);
	
	private static final String CONFIG_FILE_NAME = "config.properties";

	private Config config;

	public HealthCheckApp(String configFile) throws Exception {
		config = Config.getConfig(configFile);
	}

	private void run() throws Exception {
		Connection sourceConn = null;
		PreparedStatement pstmt = null;
		String sql = "";
		Console console = null;
		while (true) {
			try {
				Class.forName(config.logminerDbDriver);
				
				sourceConn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);

				sourceConn.setAutoCommit(false);

				long t = System.currentTimeMillis();
				sql = "update " + config.logminerTableLogminerScn
						+ " set health_time=?";
				pstmt = sourceConn.prepareStatement(sql);
				pstmt.setTimestamp(1, new Timestamp(t));
				pstmt.executeUpdate();
				sourceConn.commit();
				
				pstmt.close();
				
				console = System.console();
				console.printf(" %d ", t);
				console.flush();

				
				Thread.sleep(60000);
				

			} catch (Exception e) {
				logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				throw e;
			} finally {
				if (sourceConn != null) {
					try {
						sourceConn.close();
					} catch (SQLException e) {
						throw e;
					}
				}

			}
		}
	}

	public static void main(String[] args) {
		String profileActive = System.getProperty("profile.active", "");

		HealthCheckApp app;
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

			app = new HealthCheckApp(configFile);

			app.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
