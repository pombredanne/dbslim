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

public class DbSlimSelectQuery {

	private String connectionPoolName;
	private String sql;

	public DbSlimSelectQuery(String sql) {
		this(DbSlimSetup.DEFAULT_CONNECTION_POOL_NAME, sql);
	}

	public DbSlimSelectQuery(String connectionPoolName, String sql) {
		
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
	
	
	
	public String data(String columnId, String rowIndex){
		
		try{
			Integer.parseInt(columnId);
			return dataByColumnIndexAndRowIndex(columnId, rowIndex);
		}catch(NumberFormatException e){
			return dataByColumnNameAndRowIndex(columnId, rowIndex);
		}
		 
	}
	
	public String dataByColumnIndexAndRowIndex(String columnIndex, String rowIndex){
		
		int rowIndexInteger = Integer.parseInt(rowIndex);
		int columnIndexInteger = Integer.parseInt(columnIndex);
		
		List<List<List<String>>> dataTable = getDataTable();
				
		return dataTable.get(rowIndexInteger).get(columnIndexInteger).get(1);
	}
	
	
	public String dataByColumnNameAndRowIndex(String columnName, String rowIndex){
		
		int rowIndexInteger = Integer.parseInt(rowIndex);
		
		List<List<List<String>>> dataTable = getDataTable();
				
		List<List<String>> dataRow = dataTable.get(rowIndexInteger);
		
		for (List<String> dataItem : dataRow) {
			if (String.valueOf(dataItem.get(0)).toUpperCase().equals(String.valueOf(columnName).toUpperCase())){
				return dataItem.get(1);
			}
		}
		
		return null;
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
			stmt = conn.createStatement();
			rset = stmt.executeQuery(sql);
			
			ArrayList<String> columnNames = new ArrayList<String>();
						
			int numcols = rset.getMetaData().getColumnCount();
			
			for (int i = 1; i <= numcols; i++) {
				columnNames.add(rset.getMetaData().getColumnName(i));
			}
			
			
			while (rset.next()) {
				ArrayList<List<String>> dataRow = new ArrayList<List<String>>(); 
				for (int i = 1; i <= numcols; i++) {
					ArrayList<String> dataItem = new ArrayList<String>();
					dataItem.add(String.valueOf(columnNames.get(i - 1)));
					dataItem.add(String.valueOf(rset.getString(i)));
					dataRow.add(dataItem);
				}
				dataTable.add(dataRow);
			}
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
