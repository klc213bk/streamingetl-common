package com.transglobe.streamingetl.common.dataprep.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.common.dataprep.DbConfig;

public class DbConnectionUtil {
	private static final Logger logger = LoggerFactory.getLogger(DbConnectionUtil.class);
	
	private static String fullTableNameT = "pmuser.t_production_detail";
	private static String fullTableNameK = "pmuser.k_production_detail";
	
	public static DbConfig getDbConnConfig(String fileName) throws Exception {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();

		Properties prop = new Properties();
		try (InputStream input = loader.getResourceAsStream(fileName)) {

			// load a properties file
			prop.load(input);


			DbConfig dbConfig = new DbConfig();
			dbConfig.sourceDbDriver = prop.getProperty("source.db.driver");
			dbConfig.sourceDbUrl = prop.getProperty("source.db.url");
			dbConfig.sourceDbUsername = prop.getProperty("source.db.username");
			dbConfig.sourceDbPassword = prop.getProperty("source.db.password");
			String sourceTableStr = prop.getProperty("source.tables");
			dbConfig.sourceTables = sourceTableStr.split(",");

			dbConfig.sinkDbDriver = prop.getProperty("sink.db.driver");
			dbConfig.sinkDbUrl = prop.getProperty("sink.db.url");
			dbConfig.sinkDbUsername = prop.getProperty("sink.db.username");
			dbConfig.sinkDbPassword = prop.getProperty("sink.db.password");
			String destTableStr = prop.getProperty("sink.tables");
			dbConfig.sinkTables = destTableStr.split(",");

			return dbConfig;
		} catch (Exception e) {
			throw e;
		} 
	}
//	public static void writeTProductionDetailToDb(Connection conn, List<ProductionDetail> list) throws Exception {
//
//		PreparedStatement pstmt = conn.prepareStatement("INSERT INTO T_PRODUCTION_DETAIL (DETAIL_ID) VALUES (?)");
//		int i = 0;
//		for (ProductionDetail detail : list) {
//			i ++;
//			pstmt.setLong(1, detail.getDetailId());
//			pstmt.addBatch();
//			
//			if (i % 10 == 0) {
//				pstmt.executeBatch();
//				System.out.println("count:" + i);
//			}
//		}
//		
//		pstmt.executeBatch();
//		
//		pstmt.close();
//	
//
//	}
//	public static void writeOriginalTProductionDetailAsJson(Connection conn, String initilJsonFileName, Long initialCount
//			, String dataJsonFileName, Long dataCount) throws Exception {
//
//		Statement stmt = conn.createStatement();
//		ResultSet rs = stmt.executeQuery("select * from pmuser.t_production_detail order by detail_id");
//		long i = 0L;
//		List<ProductionDetail> initialList = new ArrayList<>();
//		List<ProductionDetail> dataList = new ArrayList<>();
//		while (rs.next()) {
//			i++;
//			if (i <= initialCount) {
//				ProductionDetail detail = new ProductionDetail();
//				detail.setDetailId(rs.getLong("DETAIL_ID"));
//				initialList.add(detail);
//			} else if (i <= initialCount + dataCount) {
//				ProductionDetail detail = new ProductionDetail();
//				detail.setDetailId(rs.getLong("DETAIL_ID"));
//				dataList.add(detail);
//			} else {
//				break;
//			}
//
//		}
//		ObjectMapper objectMapper1 = new ObjectMapper();
//		objectMapper1.writeValue(new File(initilJsonFileName), initialList);
//		
//		ObjectMapper objectMapper2 = new ObjectMapper();
//		objectMapper2.writeValue(new File(dataJsonFileName), dataList);
//
//		rs.close();
//		stmt.close();
//
//	}
	
//	public static void prepareDataSql(String fileName, int rows) throws Exception {
//		DbConfig dbConfig = DbConnectionUtil.getDbConnConfig("config.properties");
//		Connection sourceConn = null;
//		Connection sinkConn = null;
//		
//		Class.forName(dbConfig.sourceDbDriver);
//		sourceConn = DriverManager.getConnection(dbConfig.sourceDbUrl, dbConfig.sourceDbUsername, dbConfig.sourceDbPassword);
//		
//		Class.forName(dbConfig.sinkDbDriver);
//		sinkConn = DriverManager.getConnection(dbConfig.sinkDbUrl, dbConfig.sinkDbUsername, dbConfig.sinkDbPassword);
//		
//		Statement stmt; 
//		Statement stmt2; 
//		int i = 0;
//	
//		FileInputStream inputStream = null;
//		Scanner sc = null;
//		try {
//			stmt = sourceConn.createStatement();
//			stmt.executeUpdate("truncate table " + fullTableNameT);
//			stmt.close();
//			logger.info("   truncate table={}", fullTableNameT);
//			
//			stmt2 = sinkConn.createStatement();
//			stmt2.executeUpdate("truncate table " + fullTableNameK);
//			stmt2.close();
//			logger.info("   truncate table={}", fullTableNameK);
//			
//			stmt = sourceConn.createStatement();
//		    inputStream = new FileInputStream(fileName);
//		    sc = new Scanner(inputStream, "UTF-8");
//		  //  System.out.println("scanner:" + sc);
//		   // System.out.println("nextLine:" + sc.nextLine());
//		    
//		    // skip first 2 lines
//		    sc.nextLine();
//		    sc.nextLine();
//		    while (sc.hasNextLine()) {
//		    	i++;
//		    	if (i > rows) break;
//		    	
//		        String line = sc.nextLine();
//		        line =  StringUtils.substring(line, 0, line.length()-1); // trim ';'
//		        stmt.addBatch(line);
//		        if (i % 100 == 0) {
//					stmt.executeBatch();
//					System.out.println("count:" + i);
//				}
//		        // System.out.println(line);
//		    }
//		    stmt.executeBatch();
//			
//			stmt.close();
//			
//		 // note that Scanner suppresses exceptions
//		    if (sc.ioException() != null) {
//		        throw sc.ioException();
//		    }
//		} finally {
//		    if (inputStream != null) {
//		        inputStream.close();
//		    }
//		    if (sc != null) {
//		        sc.close();
//		    }
//		    if (sourceConn != null) {
//		    	sourceConn.close();
//		    }
//		    if (sinkConn != null) {
//		    	sinkConn.close();
//		    }
//		}
//	}
//	public static void main(String[] args) {
//
//
//		try {
///*
//			doWriteOriginalTProductionDetailAsJson(
//					"./data/initial_tproductiondetail.json"
//					, 1000L
//					, "./data/data_tproductiondetail.json"
//					, 10000L);  // write data for initial and testing data
//	*/	
//			
//	//		prepareData("./data/initial_tproductiondetail.json", 1000); // load initial data to table from file
//			
//			prepareDataSql("./data/t_production_detail_11000.sql", 1000);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//	}
//	private static void doWriteOriginalTProductionDetailAsJson (
//			String initialJsonFileName, Long initialCount, String dataJsonFileName, Long dataCount) throws Exception {
//
//		DbConfig dbConfig = DbConnectionUtil.getDbConnConfig("config.properties");
//		Connection sourceConn = null;
//
//		Class.forName(dbConfig.sourceDbDriver);
//		sourceConn = DriverManager.getConnection(dbConfig.sourceDbUrl, dbConfig.sourceDbUsername, dbConfig.sourceDbPassword);
//
//		DbConnectionUtil.writeOriginalTProductionDetailAsJson(
//				sourceConn, initialJsonFileName, initialCount, dataJsonFileName, dataCount);
//		
//		sourceConn.close();
//	}
//	public static List<ProductionDetail> getProductionDetailListFromJsonFile(String fileName) throws Exception {
//		File file = new File(fileName);
//		ObjectMapper objectMapper = new ObjectMapper();
//		
//		List<ProductionDetail> detailList= objectMapper.readValue(file, new TypeReference<List<ProductionDetail>>() {});
//        
////		for (ProductionDetail detail : detailList) {
////			System.out.println(ToStringBuilder.reflectionToString(detail));
////		}
//		
//		Collections.sort(detailList, new Comparator<ProductionDetail>() {
//
//			@Override
//			public int compare(ProductionDetail o1, ProductionDetail o2) {
//				return o1.getDetailId().compareTo(o2.getDetailId());
//			}
//			
//		});
//		
//		return detailList;
//	}
//	private static void prepareData(String fileName, int rows) throws Exception {
//		
//		List<ProductionDetail> detailList = getProductionDetailListFromJsonFile(fileName);
//		
//		DbConfig dbConfig = DbConnectionUtil.getDbConnConfig("config.properties");
//		
//		Class.forName(dbConfig.sourceDbDriver);
//		Connection sourceConn = DriverManager.getConnection(dbConfig.sourceDbUrl, dbConfig.sourceDbUsername, dbConfig.sourceDbPassword);
//
//		
//		writeTProductionDetailToDb(sourceConn, detailList.subList(0, rows));
//		
//		sourceConn.close();
//
//	}
}
