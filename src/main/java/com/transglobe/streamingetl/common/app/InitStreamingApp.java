package com.transglobe.streamingetl.common.app;

import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitStreamingApp {
	private static final Logger logger = LoggerFactory.getLogger(InitStreamingApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

	private static final String LOGMINER_SCN_TABLE_FILE_NAME = "logminertable-T_LOGMINER_SCN.sql";
	
	private Config config;

	public InitStreamingApp(String configFile) throws Exception {
		config = Config.getConfig(configFile);
	}
	private void createTable() throws Exception {
		String tableName = config.logminerTableLogminerScn;
		logger.info(">>> check if table exists:{}", tableName);
		if (!checkTableExists(tableName)) {
			logger.info(">>> table:{} does not exist. Create table!!!", tableName);
			// create table
			String tableFileName = LOGMINER_SCN_TABLE_FILE_NAME;
			createTable(tableFileName);
			logger.info(">>> table:{} is created from file={}.", tableName, tableFileName );
			
			//  add supplemental log
			logger.info(">>> add supplemental log");
			addSupplementalLog(tableName);
		} else {
			logger.info(">>> table:{} is already existed.", tableName);
		}
		
		
	}
	private void addSupplementalLog(String tableName) throws Exception {
		Connection conn = null;
		Statement stmt = null;
		String sql = null;
		try {
			Class.forName(config.logminerDbDriver);

			conn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);
			stmt = conn.createStatement();
			sql = "ALTER TABLE TGLMINER.T_LOGMINER_SCN ADD SUPPLEMENTAL LOG DATA(ALL) COLUMNS";
			stmt.execute(sql);
			
			
		} catch (Exception e) {
			
			throw e;
		} finally {
			if (stmt != null) stmt.close();
			if (conn != null) conn.close();
		}


	}
	private void createTable(String createTableFile) throws Exception {
		Connection sourceConn = null;
		Statement stmt = null;
		InputStream inputStream = null;
		String sql = null;
		try {
			Class.forName(config.logminerDbDriver);

			sourceConn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);

			ClassLoader loader = Thread.currentThread().getContextClassLoader();	
			inputStream = loader.getResourceAsStream(createTableFile);
			sql = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
			
			stmt = sourceConn.createStatement();
			
			stmt.execute(sql);
		} catch (Exception e) {
			
			throw e;
		} finally {
			if (inputStream != null) inputStream.close();
			if (stmt != null) stmt.close();
			if (sourceConn != null) sourceConn.close();
		}


	}
	private boolean checkTableExists(String tableName) throws Exception {
		Connection sourceConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		Console console = null;
		boolean exists = false;
		try {
			Class.forName(config.logminerDbDriver);

			sourceConn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);

			sql = "select count(*) from " + tableName;

			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				exists = true;
			}
			rs.close();
			pstmt.close();


			//			console = System.console();
			//			console.printf(" time:%d, currentScn:%d", time, currentScn);
			//			console.flush();

		} catch (Exception e) {
			if (e instanceof SQLException) {
				logger.error(">>> sqlstate:{}", ((SQLException) e).getSQLState());
				logger.error(">>> errorCode:{}", ((SQLException) e).getErrorCode());
				int errorCode = ((SQLException) e).getErrorCode();
				if (942 == errorCode) {
					// table does not exists
				}
			} else {
				throw e;
			}
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					throw e;
				}
			}

		}
		return exists;
	}
	

	public static void main(String[] args) {
		String profileActive = System.getProperty("profile.active", "");

		InitStreamingApp app;
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

			app = new InitStreamingApp(configFile);

			app.createTable();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
