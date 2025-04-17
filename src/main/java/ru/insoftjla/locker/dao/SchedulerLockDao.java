package ru.insoftjla.locker.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import javax.sql.DataSource;

public class SchedulerLockDao {

    private static final String GET_BY_NAME_QUERY = "SELECT name, locked_at, locked_granted FROM scheduler_lock WHERE name = ?";
    private static final String GET_BY_NAME_AND_LOCKED_AT_QUERY = "SELECT name FROM scheduler_lock WHERE name = ? and locked_at > ?";
    private static final String EXISTS_BY_NAME_QUERY = "SELECT EXISTS(SELECT name FROM scheduler_lock WHERE name = ?)";
    private static final String INSERT_QUERY = "INSERT INTO scheduler_lock (name, locked_at, locked_granted) VALUES (?, ?, ?)";
    private static final String UPDATE_QUERY = "UPDATE scheduler_lock SET locked_at = ?, locked_granted = ? WHERE name = ?";

    private final DataSource dataSource;

    public SchedulerLockDao(DataSource dataSource) {
        this.dataSource = dataSource;
        validate();
    }

    private void validate() {
        try (var connection = dataSource.getConnection()) {
            var pstm = connection.prepareStatement(GET_BY_NAME_QUERY);
            pstm.setString(1, "InitializeQuery");
            var rs = pstm.executeQuery();
            rs.next();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean doLock(String name, LocalDateTime lockedAt) {
        try (var connection = dataSource.getConnection()) {
            return doLock(connection, name, lockedAt);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean doLock(Connection connection, String name, LocalDateTime lockedAt) throws SQLException {
        var autoCommit = connection.getAutoCommit();
        var transactionIsolation = connection.getTransactionIsolation();
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        try {
            var now = LocalDateTime.now();
            var pstm = connection.prepareStatement(GET_BY_NAME_AND_LOCKED_AT_QUERY);
            pstm.setString(1, name);
            pstm.setTimestamp(2, Timestamp.valueOf(now));
            var rs = pstm.executeQuery();
            if (rs.next()) {
                return false;
            }

            pstm = connection.prepareStatement(EXISTS_BY_NAME_QUERY);
            pstm.setString(1, name);
            rs = pstm.executeQuery();
            rs.next();

            if (rs.getBoolean(1)) {
                pstm = connection.prepareStatement(UPDATE_QUERY);
                pstm.setTimestamp(1, Timestamp.valueOf(lockedAt));
                pstm.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                pstm.setString(3, name);
            } else {
                pstm = connection.prepareStatement(INSERT_QUERY);
                pstm.setString(1, name);
                pstm.setTimestamp(2, Timestamp.valueOf(lockedAt));
                pstm.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
            }
            return pstm.executeUpdate() > 0;
        } catch (SQLException e) {
            connection.rollback();
            return false;
        } finally {
            connection.commit();
            connection.setAutoCommit(autoCommit);
            connection.setTransactionIsolation(transactionIsolation);
        }
    }
}
