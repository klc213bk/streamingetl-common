package com.transglobe.streamingetl.common.dataprep;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class DbConfig {
	public String sourceDbDriver;
	public String sourceDbUrl;
	public String sourceDbUsername;
	public String sourceDbPassword;
	public String[] sourceTables;
	public String sinkDbDriver;
	public String sinkDbUrl;
	public String sinkDbUsername;
	public String sinkDbPassword;
	public String[] sinkTables;

	public List<String> getSourceTableOwners() {

		List<String> ownerList = new ArrayList<>();
		for (String table : sourceTables) {
			ownerList.add(table.split("\\.")[0]);
		}
		return ownerList;
	}
	public List<String> getSourceTableNames() {

		List<String> tableNameList = new ArrayList<>();
		for (String table : sourceTables) {
			tableNameList.add(table.split("\\.")[1]);
		}
		return tableNameList;
	}
	public List<String> getDestTableOwners() {

		List<String> ownerList = new ArrayList<>();
		for (String table : sinkTables) {
			ownerList.add(table.split("\\.")[0]);
		}
		return ownerList;
	}
	public List<String> getDestTableNames() {

		List<String> tableNameList = new ArrayList<>();
		for (String table : sinkTables) {
			tableNameList.add(table.split("\\.")[1]);
		}
		return tableNameList;
	}
	
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
