package com.transglobe.streamingetl.common.util;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;

public class DataUtils {
	
	public static void truncateTable(Connection conn, String owner, String tableName) throws Exception  {

		try {
			
			// truncate table
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(String.format("truncate table %s.%s", owner, tableName));
			
			stmt.close();
		}  catch (Exception e) {
			throw e;
		} 
	}
	
	public static void insertDataIntoSource(Connection sourceConn, String insertSqlFileName, Integer fromRows, Integer toRows) throws Exception  {

		try {
		
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
				} catch (SQLException e) {
					j++;
				}
				//stmt.addBatch(sql);

			}
			stmt.close();
		} catch (Exception e) {
			throw e;
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
	public static List<String> getDataSql(String fileName, Integer fromRows, Integer toRows) throws Exception {
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
