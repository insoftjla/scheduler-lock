package ru.insoftjla.locker.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import javax.sql.DataSource;

/**
 * Data access object responsible for acquiring and managing scheduler locks in
 * the underlying database. The implementation uses plain JDBC with
 * repeatable-read transactions to guarantee exclusive lock access.
 */
public class SchedulerLockDao {

    private static final String GET_BY_NAME_QUERY = "SELECT name, locked_at, locked_granted FROM scheduler_lock WHERE name = ?";
    private static final String GET_BY_NAME_AND_LOCKED_AT_QUERY = "SELECT name FROM scheduler_lock WHERE name = ? and locked_at > ?";
    private static final String EXISTS_BY_NAME_QUERY = "SELECT EXISTS(SELECT name FROM scheduler_lock WHERE name = ?)";
    private static final String INSERT_QUERY = "INSERT INTO scheduler_lock (name, locked_at, locked_granted) VALUES (?, ?, ?)";
    private static final String UPDATE_QUERY = "UPDATE scheduler_lock SET locked_at = ?, locked_granted = ? WHERE name = ?";

    private final DataSource dataSource;

    /**
     * Creates a DAO instance using the provided {@link DataSource}. A validation
     * query is executed during construction to ensure the required table is
     * accessible.
     *
     * @param dataSource data source used for acquiring connections
     */
    public SchedulerLockDao(DataSource dataSource) {
        this.dataSource = dataSource;
        validate();
    }

    /**
     * Executes a lightweight query to verify that the required database table is
     * accessible. The method is invoked during construction and throws a runtime
     * exception if the validation fails.
     */
    private void validate() {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement pstm = connection.prepareStatement(GET_BY_NAME_QUERY);
            pstm.setString(1, "InitializeQuery");
            ResultSet rs = pstm.executeQuery();
            rs.next();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Attempts to acquire a lock with the given name until the specified
     * expiration time.
     *
     * @param name name of the lock
     * @param lockedAt expiration timestamp of the lock
     * @return {@code true} if the lock was successfully acquired
     */
    public boolean doLock(String name, LocalDateTime lockedAt) {
        try (Connection connection = dataSource.getConnection()) {
            return doLock(connection, name, lockedAt);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Performs the actual lock acquisition within a transaction using the
     * provided connection.
     *
     * @param connection connection used to perform the lock operation
     * @param name name of the lock
     * @param lockedAt expiration timestamp of the lock
     * @return {@code true} if the lock was acquired successfully
     * @throws SQLException if a database error occurs
     */
    private boolean doLock(Connection connection, String name, LocalDateTime lockedAt) throws SQLException {
        boolean autoCommit = connection.getAutoCommit();
        int transactionIsolation = connection.getTransactionIsolation();
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        try {
            LocalDateTime now = LocalDateTime.now();
            PreparedStatement pstm = connection.prepareStatement(GET_BY_NAME_AND_LOCKED_AT_QUERY);
            pstm.setString(1, name);
            pstm.setTimestamp(2, Timestamp.valueOf(now));
            ResultSet rs = pstm.executeQuery();
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
