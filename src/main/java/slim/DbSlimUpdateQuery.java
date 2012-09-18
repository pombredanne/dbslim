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
package slim;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import services.DbConnectionFactory;

public class DbSlimUpdateQuery {
	
	private String connectionPoolName;
	private String sql;

	public DbSlimUpdateQuery(String sql) {
		this(DbSlimSetup.DEFAULT_CONNECTION_POOL_NAME, sql);
	}

	public DbSlimUpdateQuery(String connectionPoolName, String sql) {
		
		this.connectionPoolName = connectionPoolName;
		
		sql = sql.replaceAll("\\n"," ");
		sql = sql.replaceAll("\\t"," ");
		sql = sql.replaceAll("<br/>", " ");
		sql = sql.trim();
		
		this.sql = sql;
	}

	public void table(List<List<String>> table) {
		// optional function
	}

	public List<Object> query() {

		List<List<List<String>>> dataTable = getDataTable();
		return new ArrayList<Object>(dataTable);
		
	}
	
	public String rowsUpdated(){
		List<List<List<String>>> dataTable = getDataTable();
		return dataTable.get(0).get(0).get(1);
	}
	
	protected List<List<List<String>>> getDataTable(){
		
		DataSource dataSource = DbConnectionFactory.getDataSource(connectionPoolName);

		//
		// Now, we can use JDBC DataSource as we normally would.
		//
		Connection conn = null;
		Statement stmt = null;
		ResultSet rset = null;

		ArrayList<List<List<String>>> dataTable = new ArrayList<List<List<String>>>(); 
		
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(true);
			stmt = conn.createStatement();
			int rowsUpdated = stmt.executeUpdate(sql);
			
			ArrayList<List<String>> dataRow = new ArrayList<List<String>>(); 
			ArrayList<String> dataItem = new ArrayList<String>();
			dataItem.add(String.valueOf("rowsUpdated"));
			dataItem.add(String.valueOf(rowsUpdated));
			
			dataRow.add(dataItem);
			dataTable.add(dataRow);
		
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			try {
				if (rset != null)
					rset.close();
			} catch (Exception e) {
			}
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception e) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (Exception e) {
			}
		}
		
		return dataTable;
	}
}
