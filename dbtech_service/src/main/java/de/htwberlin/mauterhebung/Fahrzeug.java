package de.htwberlin.mauterhebung;

import de.htwberlin.exceptions.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Fahrzeug implements DAOs {

    private Connection connection;
    private static final Logger L = LoggerFactory.getLogger(MauterServiceImpl.class);
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
    public long getSSKL_Id(String kennzeichen){
        String sql = "SELECT SSKL_ID FROM Fahrzeug WHERE Kennzeichen LIKE ?";
        L.info(sql);
        try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                } else {
                    throw new DataException();
                }
            }
        } catch (SQLException e) {
            L.error("", e);
            throw new DataException(e);
        }
    }
    public long vehicle_id(String kennzeichen) {
        String sql = "SELECT fz_id FROM Fahrzeug WHERE kennzeichen LIKE ?";
        L.info(sql);
        try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
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
