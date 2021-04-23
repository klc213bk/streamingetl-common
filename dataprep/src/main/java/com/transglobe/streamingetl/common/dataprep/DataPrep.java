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

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.Update;

public class DataPrep {

	private static final Logger logger = LoggerFactory.getLogger(DataPrep.class);


	private DbConfig dbConfig;;

	public DataPrep() throws Exception {
		dbConfig = DbConnectionUtil.getDbConnConfig("config.properties");
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
			int i = 0;
			for (String sql : dataSql) {
				i++;
				try {
					stmt.execute(sql);
				
				} catch (SQLException e) {
					logger.info("    insert count={}, ex={}",i, ExceptionUtils.getStackTrace(e));
				}
				//stmt.addBatch(sql);

				if ( i % 100 == 0 || i == dataSql.size()) {
					//stmt.executeBatch();
					logger.info("    insert count:" + i);
				}
			}
			stmt.close();

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
	public static void main(String[] args){
		logger.info(">>> DataPrep t_policy_holder !!!");
		String policyHolderSqlFile = "./data/t_policy_holder.sql";
		
		try {
			DataPrep dataPrep = new DataPrep();
			
			dataPrep.truncateSourceTable("PMUSER", "T_POLICY_HOLDER");
			dataPrep.insertDataIntoSource(policyHolderSqlFile, 1, null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logger.info(">>> DataPrep t_insured_list !!!");
		String insuredListSqlFile = "./data/t_insured_list.sql";
		
		try {
			DataPrep dataPrep = new DataPrep();
			
			dataPrep.truncateSourceTable("PMUSER", "T_INSURED_LIST");
			dataPrep.insertDataIntoSource(insuredListSqlFile, 1, null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logger.info(">>> DataPrep t_contract_bene !!!");
		String contractBeneSqlFile = "./data/t_contract_bene.sql";
		
		try {
			DataPrep dataPrep = new DataPrep();
			
			dataPrep.truncateSourceTable("PMUSER", "T_CONTRACT_BENE");
			dataPrep.insertDataIntoSource(contractBeneSqlFile, 1, null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
