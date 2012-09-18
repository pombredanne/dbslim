/*******************************************************************************
 * The MIT License
 * 
 * Copyright (c) 2010, Mark S.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 ******************************************************************************/
package services;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DbConnectionFactory {

	protected static Map<String, DbConnectionDetails> connectionDetailsMap = new ConcurrentHashMap<String, DbConnectionDetails>();

//	public static synchronized void loadJdbcDriver(String jdbcDriver) {
//		
//		System.out.println("Loading underlying JDBC driver.");
//		try {
//			Class.forName(jdbcDriver);
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		}
//	}

	public static synchronized DataSource getDataSource(
			String connectionPoolName){
		
		DbConnectionDetails connectionDetails = connectionDetailsMap.get(connectionPoolName);
		
		if (null == connectionDetails){
			return null;
		}else{
			return connectionDetails.dataSource;
		}
		
	}
	
	public static synchronized DataSource getDataSource(
			String connectionPoolName,
			String jdbcDriverClass,
			String connectURI, String username, String password,
			int minIdle, int maxActive) throws Exception {

		if (null == connectionDetailsMap.get(connectionPoolName)) {
			connectionDetailsMap.put(connectionPoolName, new DbConnectionDetails());
		}

		DbConnectionDetails connectionDetails = connectionDetailsMap.get(connectionPoolName);

		if (null != connectionDetails.dataSource
				&& String.valueOf(jdbcDriverClass).equals(connectionDetails.jdbcDriverClass)
				&& String.valueOf(connectURI).equals(connectionDetails.connectURI)
				&& String.valueOf(username).equals(connectionDetails.username)
				&& String.valueOf(password).equals(connectionDetails.password) 
				&& minIdle == connectionDetails.minIdle
				&& maxActive == connectionDetails.maxActive) {
			return connectionDetails.dataSource;
		}

//		DataSource unpooledDatasource = DataSources.unpooledDataSource(
//				connectURI,
//				username,
//				password);
//		
//		Map<String, String> connectionPoolOverrideProps = new HashMap<String, String>();
//		connectionPoolOverrideProps.put ( "maxStatements", "100");
//		connectionPoolOverrideProps.put ( "maxPoolSize", String.valueOf(maxActive));
//		
//		DataSource pooledDatasource = DataSources.pooledDataSource( unpooledDatasource, connectionPoolName, connectionPoolOverrideProps );

		
		ComboPooledDataSource pooledDatasource = new ComboPooledDataSource(); 
		pooledDatasource.setDriverClass(jdbcDriverClass); 
		pooledDatasource.setJdbcUrl(connectURI); 
		pooledDatasource.setUser(username); 
		pooledDatasource.setPassword(password);
		pooledDatasource.setMinPoolSize(minIdle); 
		pooledDatasource.setAcquireIncrement(5); 
		pooledDatasource.setMaxPoolSize(maxActive); 
		
		connectionDetails.jdbcDriverClass = jdbcDriverClass;
		connectionDetails.connectURI = connectURI;
		connectionDetails.username = username;
		connectionDetails.password = password;
		connectionDetails.minIdle = minIdle;
		connectionDetails.maxActive = maxActive;
		connectionDetails.dataSource = pooledDatasource;
		
		return pooledDatasource;
	}

}


