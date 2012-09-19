/*******************************************************************************
 * The MIT License
 * 
 * Copyright (c) 2012, Mark Fink
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

import javax.sql.DataSource;

import services.DbConnectionFactory;

public class DbSlimWaitQuery {

	private String connectionPoolName;
	private String sql;
    private int timeout;

    public DbSlimWaitQuery(String sql) {
        this(DbSlimSetup.DEFAULT_CONNECTION_POOL_NAME, sql, DbSlimSetup.DEFAULT_WAIT_TIMEOUT);
    }

    public DbSlimWaitQuery(String sql, int timeout) {
        this(DbSlimSetup.DEFAULT_CONNECTION_POOL_NAME, sql, timeout);
    }

	public DbSlimWaitQuery(String connectionPoolName, String sql, int timeout) {
		
		this.connectionPoolName = connectionPoolName;
		
		sql = sql.replaceAll("\\n"," ");
		sql = sql.replaceAll("\\t"," ");
		sql = sql.replaceAll("<br/>", " ");
		sql = sql.trim();
		
		this.sql = sql;
		this.timeout = timeout;
		this.waitForRowcount();
	}

	private void waitForRowcount() {
	    // execute the db query until rowcount > 0 or timeout
        long start_time = System.nanoTime();
	    
	    DataSource dataSource = DbConnectionFactory.getDataSource(connectionPoolName);

        Connection conn = null;
        Statement stmt = null;
        ResultSet rset = null;
        int rowcount = 0; 
        int sleeptime = 0; // at start the sleeptime is 0 seconds
	    
        while(rowcount == 0 && ((System.nanoTime() - start_time)/1000000 < timeout) ) {
            try {
                Thread.sleep(sleeptime);
            } catch(InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            sleeptime += 1000; // next time we will sleep 1 second longer

            try {
                conn = dataSource.getConnection();
                stmt = conn.createStatement();
                rset = stmt.executeQuery(sql);
                rset.next();
                
                rowcount = rset.getInt("rowcount");
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
        }

	    if(rowcount == 0 && ((System.nanoTime() - start_time)/1000000 > timeout)) {
	        throw new RuntimeException("message:<<wait for rowcount could not be completed within " + timeout + " ms>>");
	    }
	    System.out.println("wait time for rowcount was " + (System.nanoTime() - start_time)/1000000 + "ms.");
	}
}
