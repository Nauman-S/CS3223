package simpledb.jdbc.network;

import simpledb.jdbc.StatementAdapter;
import simpledb.plan.Plan;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * An adapter class that wraps RemoteStatement. Its methods do nothing except
 * transform RemoteExceptions into SQLExceptions.
 *
 * @author Edward Sciore
 */
public class NetworkStatement extends StatementAdapter {
    private RemoteStatement rstmt;

    public NetworkStatement(RemoteStatement s) {
        rstmt = s;
    }

    public ResultSet executeQuery(String qry) throws SQLException {
        try {
            RemoteResultSet rrs = rstmt.executeQuery(qry);
            return new NetworkResultSet(rrs);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public int executeUpdate(String cmd) throws SQLException {
        try {
            return rstmt.executeUpdate(cmd);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Returns a plan for the query. Rolls back and throws an SQLException if it cannot create
     * the plan.
     */
    public Plan getQueryPlan(String sql) throws SQLException {
        try {
            return rstmt.getQueryPlan(sql);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public void close() throws SQLException {
        try {
            rstmt.close();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
