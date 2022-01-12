package de.htwberlin.mauterhebung;

import de.htwberlin.exceptions.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Buchung implements DAOs{
    private static final Logger L = LoggerFactory.getLogger(MauterServiceImpl.class);
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
    public void statustimeUpdate(long buchung_id){
        String sql = "UPDATE Buchung SET B_id = ?, Befahrungsdatum = current_timestamp WHERE Buchung_id = ?";
        L.info(sql);
        try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
            ps.setInt(1, 3);
            ps.setLong(2,buchung_id);
            ps.executeUpdate();
        } catch (SQLException e) {
            L.error("", e);
            throw new DataException(e);
        }
    }
    public int buchungsID(String kennzeichen, int mautabschnitt){
        String sql = "Select B.BUCHUNG_ID From BUCHUNG B join MAUTABSCHNITT M on B.ABSCHNITTS_ID = M.ABSCHNITTS_ID WHERE B.KENNZEICHEN LIKE ? AND B.ABSCHNITTS_ID = ? AND B.B_ID =1";
        L.info(sql);
        try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);
            ps.setInt(2, mautabschnitt);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int result = rs.getInt(1);
                    return result;
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
