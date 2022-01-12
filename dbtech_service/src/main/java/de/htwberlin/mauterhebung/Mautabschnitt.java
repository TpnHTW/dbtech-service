package de.htwberlin.mauterhebung;

import de.htwberlin.exceptions.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Mautabschnitt implements DAOs{
    private static final Logger L = LoggerFactory.getLogger(Mautabschnitt.class);
    private Connection connection;

    protected Connection useConnection() {
        if (connection != null) {
            return this.connection;
        } else {
            throw new RuntimeException("Connection not existing");
        }
    }
        public float getMautabschnittslaenge(int mautabschnitt) {
            String sql = "SELECT Laenge FROM Mautabschnitt WHERE Abschnitts_id = ?";
            L.info(sql);
            try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
                ps.setInt(1, mautabschnitt);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getFloat(1);
                    } else {
                        throw new DataException();
                    }
                }
            } catch (SQLException e) {
                L.error("", e);
                throw new DataException(e);
            }
    }

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
