package com.transglobe.streamingetl.common.dataprep.util;

import java.io.InputStream;
import java.util.Properties;


public class ConfigUtils {

	
	public static Properties getProperties(String fileName) throws Exception {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();

		Properties prop = new Properties();
		try (InputStream input = loader.getResourceAsStream(fileName)) {

			// load a properties file
			prop.load(input);

			return prop;
		} catch (Exception ex) {
			throw ex;

		} 
		
	}
}
