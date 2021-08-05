package com.transglobe.streamingetl.common.util;

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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OracleUtils {
	private static final Logger logger = LoggerFactory.getLogger(OracleUtils.class);
	
	public static boolean checkTableExists(String tableName, Connection sourceConn) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		Console console = null;
		boolean exists = false;
		try {

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
			
		}
		return exists;
	}
	public static void executeScriptFromFile(String createTableFile, Connection conn) throws Exception {
		
		Statement stmt = null;
		ClassLoader loader = Thread.currentThread().getContextClassLoader();	
		try (InputStream inputStream = loader.getResourceAsStream(createTableFile)) {
			String createTableScript = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
			//	logger.info(">>>  createTableScript={}", createTableScript);
			stmt = conn.createStatement();
			stmt.execute(createTableScript);
		} catch (SQLException | IOException e) {
			throw e;
		} finally {
			if (stmt != null) stmt.close();
		}

	}
	
	public static void dropTable(String tableName, Connection conn) throws SQLException {

		Statement stmt = null;
		try {
			boolean exists = OracleUtils.checkTableExists(tableName, conn);

			if (exists) {
				stmt = conn.createStatement();
				stmt.executeUpdate("DROP TABLE " + tableName);
				stmt.close();
			}
		} catch (Exception e) {
			logger.error(">>>drop table:{}, error={}", tableName, ExceptionUtils.getStackTrace(e));
		} finally {
			if (stmt != null) { 
				stmt.close();
			}
		}
	}
}
