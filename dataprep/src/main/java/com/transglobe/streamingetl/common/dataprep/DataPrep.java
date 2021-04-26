package com.transglobe.streamingetl.common.dataprep;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.common.dataprep.util.DbConnectionUtil;

public class DataPrep {

	private static final Logger logger = LoggerFactory.getLogger(DataPrep.class);


	private DbConfig dbConfig;;

	public DataPrep(String configFileName) throws Exception {
		dbConfig = DbConnectionUtil.getDbConnConfig(configFileName);
	}

	public void truncateSourceTable(String owner, String tableName) throws Exception  {

		Connection sourceConn = null;
		try {
			Class.forName(dbConfig.sourceDbDriver);
			sourceConn = DriverManager.getConnection(dbConfig.sourceDbUrl, dbConfig.sourceDbUsername, dbConfig.sourceDbPassword);
			
			// truncate table
			Statement stmt = sourceConn.createStatement();
			stmt.executeUpdate(String.format("truncate table %s.%s", owner, tableName));
			
			stmt.close();
		}  catch (Exception e) {
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
	public void insertDataIntoSource(String insertSqlFileName, Integer fromRows, Integer toRows) throws Exception  {


		logger.info("  dbConfig = {}", dbConfig);

		Connection sourceConn = null;
		try {
			Class.forName(dbConfig.sourceDbDriver);
			sourceConn = DriverManager.getConnection(dbConfig.sourceDbUrl, dbConfig.sourceDbUsername, dbConfig.sourceDbPassword);

			List<String> dataSql = getDataSql(insertSqlFileName, fromRows, toRows);

			// insert 
			Statement stmt = sourceConn.createStatement();
			int i = 0; // total count
			int j = 0; // failure count
			for (String sql : dataSql) {
				i++;
				try {
					stmt.execute(sql);
				} catch (java.sql.SQLIntegrityConstraintViolationException e) {
					j++;
					logger.info("    insert count={}, failure count={}, ex={}",i, j);
				} catch (SQLException e) {
					j++;
					logger.info("    insert count={}, failure count={}, ex=unique constraint () violated",i, j, ExceptionUtils.getStackTrace(e));
				}
				//stmt.addBatch(sql);

				if ( i % 100 == 0 || i == dataSql.size()) {
					//stmt.executeBatch();
					logger.info("    insert count:" + i);
				}
			}
			stmt.close();
			logger.info("    total count={}, failure count={}", i, j);
		} catch (Exception e) {
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
	
	/**
	 * 
	 * @param fileName
	 * @param fromRows inclusive
	 * @param toRows inclusive
	 * @return
	 * @throws Exception
	 */
	public List<String> getDataSql(String fileName, Integer fromRows, Integer toRows) throws Exception {
		List<String> sqlList = new ArrayList<>();

		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
			int i = 0;
			inputStream = new FileInputStream(fileName);
			sc = new Scanner(inputStream, "UTF-8");
			System.out.println("scanner:" + sc);
			
			// skip first 2 lines
			sc.nextLine();
			sc.nextLine();
			while (sc.hasNextLine()) {
				i++;
				if (i < fromRows) {
					sc.nextLine();
					continue;
				}
				if (toRows != null && i > toRows) break;

				String line = sc.nextLine();
				line = StringUtils.substring(line, 0, line.length()-1);
				sqlList.add(line);
			}

			// note that Scanner suppresses exceptions
			if (sc.ioException() != null) {
				throw sc.ioException();
			}

			return sqlList;
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
			if (sc != null) {
				sc.close();
			}

		}
	}
	
}
