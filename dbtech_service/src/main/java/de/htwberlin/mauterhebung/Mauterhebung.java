package de.htwberlin.mauterhebung;

import de.htwberlin.exceptions.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;

public class Mauterhebung implements DAOs{

    private static final Logger L = LoggerFactory.getLogger(Mauterhebung.class);
    private Connection connection;

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    protected Connection useConnection() {
        if (connection != null) {
            return this.connection;
        } else {
            throw new RuntimeException("Connection not existing");
        }
    }

    public void deleteMaut(long maut_id) { }

    public void insert(int mautabschnitt, float kosten, long fzg_id, long kategorie){
        String sql = "insert into Mauterhebung values " + "(?,?,?,?,current_timestamp,?)";
        L.info(sql);
        try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
            ps.setLong(1, getID());
            ps.setInt(2, mautabschnitt);
            ps.setLong(3, fzg_id);
            ps.setLong(4, kategorie);
            BigDecimal count = new BigDecimal(String.valueOf(kosten)).setScale(2, 1);
            ps.setBigDecimal(5, count);
            ps.executeUpdate();
        } catch (SQLException e) {
            L.error("", e);
            throw new DataException(e);
        }
    }
    public Mauterhebung(){}

    public int getID() {
        String sql = "SELECT Max(Maut_id) FROM Mauterhebung";
        L.info(sql);
        try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) + 1;
                } else {
                    throw new DataException();
                }
            }
        } catch (SQLException e) {
            L.error("", e);
            throw new DataException(e);
        }
    }
}
