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

import com.transglobe.streamingetl.common.util.OracleUtils;

public class InitStreamingApp {
	private static final Logger logger = LoggerFactory.getLogger(InitStreamingApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

	private static final String LOGMINER_SCN_TABLE_FILE_NAME = "logminertable-T_LOGMINER_SCN.sql";
//	private static final String LOGMINR_CONTENTS_LOG_TABLE_FILE_NAME = "logminertable-LOGMNR_CONTENTS_LOG.sql";
//	private static final String LOGMINR_CONTENTS_LOG_INDEX_FILE_NAME = "logminerindex-LOGMNR_CONTENTS_LOG.sql";

	private Config config;

	public InitStreamingApp(String configFile) throws Exception {
		config = Config.getConfig(configFile);
	}
	private void createLogminerScnTable() throws Exception {
		Connection conn = null;
		try {
			Class.forName(config.logminerDbDriver);

			conn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);

			String tableName = config.logminerTableLogminerScn;
			logger.info(">>> check if table exists:{}", tableName);
			if (checkTableExists(tableName)) {
				logger.info(">>> table:{} already exists.", tableName);
			} else {
				logger.info(">>> table:{} does not exist.", tableName);


				logger.info(">>> Create table!!!", tableName);
				// create table
				String tableFileName = LOGMINER_SCN_TABLE_FILE_NAME;
				OracleUtils.executeScriptFromFile(tableFileName, conn);
				logger.info(">>> table:{} is created from file={}.", tableName, tableFileName );

				//  add supplemental log
				logger.info(">>> add supplemental log");
				addSupplementalLog(tableName, conn);
			}
		} catch (Exception e) {

			throw e;
		} finally {
			if (conn != null) conn.close();
		}

	}
//	private void createLogminrContentsLogTable() throws Exception {
//		Connection conn = null;
//		try {
//			Class.forName(config.logminerDbDriver);
//
//			conn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);
//
//			String tableName = config.logminerTableLogmnrContentsLog;
//			logger.info(">>> check if table exists:{}", tableName);
//			if (checkTableExists(tableName)) {
//				logger.info(">>> table:{} already exists.", tableName);
//			} else {
//				logger.info(">>> table:{} does not exist.", tableName);
//
//
//				logger.info(">>> Create table!!!", tableName);
//				// create table
//				String tableFileName = LOGMINR_CONTENTS_LOG_TABLE_FILE_NAME;
//				OracleUtils.executeScriptFromFile(tableFileName, conn);
//				logger.info(">>> table:{} is created from file={}.", tableName, tableFileName );
//
//				// create index
//				String indexFileName = LOGMINR_CONTENTS_LOG_INDEX_FILE_NAME;
//				OracleUtils.executeScriptFromFile(indexFileName, conn);
//				logger.info(">>> indexes is created from file={}.", indexFileName );
//
//				
//			}
//		} catch (Exception e) {
//
//			throw e;
//		} finally {
//			if (conn != null) conn.close();
//		}
//
//	}

	private void addSupplementalLog(String tableName, Connection conn) throws Exception {
		Statement stmt = null;
		String sql = null;
		try {
			stmt = conn.createStatement();
			sql = "ALTER TABLE TGLMINER.T_LOGMINER_SCN ADD SUPPLEMENTAL LOG DATA(ALL) COLUMNS";
			stmt.execute(sql);


		} catch (Exception e) {

			throw e;
		} finally {
			if (stmt != null) stmt.close();
		}


	}
	
	//	private void dropTable(String tableName) throws Exception {
	//		Connection conn = null;
	//		Statement stmt = null;
	//		String sql = null;
	//		try {
	//			Class.forName(config.logminerDbDriver);
	//
	//			conn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);
	//			stmt = conn.createStatement();
	//			sql = "DROP TABLE " + tableName;
	//			stmt.execute(sql);
	//
	//		} catch (Exception e) {
	//
	//			throw e;
	//		} finally {
	//			if (stmt != null) stmt.close();
	//			if (conn != null) conn.close();
	//		}
	//
	//
	//	}
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

			app.createLogminerScnTable();

//			app.createLogminrContentsLogTable();

		} catch (Exception e) {
			logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
	}

}
