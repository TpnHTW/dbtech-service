package de.htwberlin.mauterhebung;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.exceptions.AlreadyCruisedException;
import de.htwberlin.exceptions.DataException;
import de.htwberlin.exceptions.InvalidVehicleDataException;
import de.htwberlin.exceptions.UnkownVehicleException;

/**
 * Die Klasse realisiert den AusleiheService.
 *
 * @author Patrick Dohmeier
 */
public class MauterServiceImpl implements IMauterhebung {
	private Buchung buchung = new Buchung();
	private Fahrzeug fahrzeug = new Fahrzeug();
	private Mautabschnitt mautabschnitt = new Mautabschnitt();
	private Mauterhebung mauterhebung = new Mauterhebung();

	private static final Logger L = LoggerFactory.getLogger(MauterServiceImpl.class);
	private Connection connection;

	final List<DAOs> daosList= new ArrayList<>();

	@Override
	public void setConnection(Connection connection) {
		daosList.add(buchung);
		daosList.add(fahrzeug);
		daosList.add(mautabschnitt);
		daosList.add(mauterhebung);

		this.connection = connection;

		for(DAOs dao : daosList){
			dao.setConnection(this.getConnection());
		}
	}

	private Connection getConnection() {
		if (connection == null) {
			throw new DataException("Connection not set");
		}
		return connection;
	}

	protected Connection useConnection() {
		if (connection != null) {
			return this.connection;
		} else {
			throw new RuntimeException("Connection not existing");
		}
	}

	/***
	 * Die Methode realisiert einen Algorithmus, der die �bermittelten
	 * Fahrzeugdaten mit der Datenbank auf Richtigkeit �berpr�ft und f�r einen
	 * mautpflichtigen Streckenabschnitt die zu zahlende Maut f�r ein Fahrzeug
	 * im Automatischen Verfahren berechnet.
	 *
	 * Zuvor wird �berpr�ft, ob das Fahrzeug registriert ist und �ber ein
	 * eingebautes Fahrzeugger�t verf�gt und die �bermittelten Daten des
	 * Kontrollsystems korrekt sind. Bei Fahrzeugen im Manuellen Verfahren wird
	 * dar�berhinaus gepr�ft, ob es noch offene Buchungen f�r den Mautabschnitt
	 * gibt oder eine Doppelbefahrung aufgetreten ist. Besteht noch eine offene
	 * Buchung f�r den Mautabschnitt, so wird diese Buchung f�r das Fahrzeug auf
	 * abgeschlossen gesetzt.
	 *
	 * Sind die Daten des Fahrzeugs im Automatischen Verfahren korrekt, wird
	 * anhand der Mautkategorie (die sich aus der Achszahl und der
	 * Schadstoffklasse des Fahrzeugs zusammensetzt) und der Mautabschnittsl�nge
	 * die zu zahlende Maut berechnet, in der Mauterhebung gespeichert und
	 * letztendlich zur�ckgegeben.
	 * ***/
	@Override
	public float berechneMaut(int mautAbschnitt, int achszahl, String kennzeichen)
			throws UnkownVehicleException, InvalidVehicleDataException, AlreadyCruisedException {
		if(openbooking(kennzeichen)){
			if(achszahlKorrektmanu(achszahl,kennzeichen)){
				alreadyCruised(kennzeichen,mautAbschnitt);
				int buchungsID = buchung.buchungsID(kennzeichen, mautAbschnitt);
				buchung.statustimeUpdate(buchungsID);
			}
		}
		else if (knownVehicle(kennzeichen)){
			long fz_id = fahrzeug.vehicle_id(kennzeichen);
			if (achszahlKorrektauto(achszahl,kennzeichen)) {
					float kosten = (mautabschnitt.getMautabschnittslaenge(mautAbschnitt)*getMautsatz(kennzeichen,achszahl)*0.01f*0.001f);
					BigDecimal count = new BigDecimal(String.valueOf(kosten)).setScale(2, 1);
					mauterhebung.insert(mautAbschnitt,count.floatValue(), getfzg_id(fz_id),getMautkategorie(kennzeichen, achszahl));
					return count.floatValue();
			}
		}
		else{
			throw new UnkownVehicleException("kennzeichen doesn't exist in db:" + kennzeichen);
		}
		return 0;
	}
	public boolean knownVehicle(String kennzeichen) {
		String sql = "SELECT * FROM Fahrzeug F join FAHRZEUGGERAT FG on F.FZ_ID= FG.FZ_ID WHERE F.kennzeichen LIKE ? AND FG.STATUS Like 'active' AND F.ABMELDEDATUM IS NULL";
		L.info(sql);
		try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
			ps.setString(1, kennzeichen);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next();
			}
		} catch (SQLException e) {
			L.error("", e);
			throw new DataException(e);
		}
	}
	public boolean achszahlKorrektauto(int achszahl, String kennzeichen) {
		String sql = "SELECT achsen FROM Fahrzeug WHERE kennzeichen=?";
		L.info(sql);
		try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
			ps.setString(1, kennzeichen);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
						if(rs.getInt(1) == achszahl) { return true; }
						else{ throw new InvalidVehicleDataException("Driving with the wrong achszahl:" + achszahl); }
					}
			}
		} catch (SQLException e) {
			L.error("", e);
			throw new DataException(e);
		}
		return false;
	}
	public boolean achszahlKorrektmanu(int achszahl, String kennzeichen) {
		String sql = "SELECT MAUTKATEGORIE.ACHSZAHL FROM MAUTKATEGORIE join BUCHUNG B on MAUTKATEGORIE.KATEGORIE_ID = B.KATEGORIE_ID where KENNZEICHEN = ?";
		L.info(sql);
		try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
			ps.setString(1, kennzeichen);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					String res = rs.getString(1);
					int value = Integer.parseInt(res.replaceAll("[^0-9]", ""));
					if(value < 5) {
						if (value == achszahl) { return true; }
						else { throw new InvalidVehicleDataException("Driving with the wrong achszahl:" + achszahl); }
					}
					else{
						if (achszahl > 4){return true;}
						else {throw new  InvalidVehicleDataException("Driving with the wrong achszahl:" + achszahl);}
					}

				}
				}
		} catch (SQLException e) {
			L.error("", e);
			throw new DataException(e);
		}
		return false;
	}
	public void alreadyCruised(String kennzeichen,int mautabschnitt){
		String sql = "Select status,M.ABSCHNITTS_ID From BUCHUNGSTATUS join BUCHUNG B on BUCHUNGSTATUS.B_ID = B.B_ID join MAUTABSCHNITT M on B.ABSCHNITTS_ID = M.ABSCHNITTS_ID  Where B.KENNZEICHEN = ?";
		L.info(sql);
		try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
			ps.setString(1, kennzeichen);
			try (ResultSet rs = ps.executeQuery()) {
				if(rs.next()){
					if(rs.getString(1).equals("abgeschlossen") && rs.getInt(2) == mautabschnitt) {
						throw new AlreadyCruisedException();
					}
				}
			}
		} catch (SQLException e) {
			L.error("", e);
			throw new DataException(e);
		}
	}
	public float getMautsatz(String kennzeichen,int achszahl) {
		String sql = "SELECT Mautsatz_je_km FROM Mautkategorie WHERE SSKL_ID = ? AND ACHSZAHL = ?";
		L.info(sql);
		String achs;
		if(achszahl > 4){
			achs = ">= 5";
		}
		else{
			achs = "= " +achszahl;
		}
		try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
			ps.setLong(1, fahrzeug.getSSKL_Id(kennzeichen));
			ps.setString(2, achs);
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
	public int getMautkategorie(String kennzeichen,int achszahl) {
		String sql = "SELECT Kategorie_id FROM Mautkategorie WHERE SSKL_ID = ? AND ACHSZAHL = ?";
		L.info(sql);
		String achs;
		if(achszahl > 4){
			achs = ">= 5";
		}
		else{
			achs = "= " +achszahl;
		}
		try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
			ps.setLong(1, fahrzeug.getSSKL_Id(kennzeichen));
			ps.setString(2, achs);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getInt(1);
				} else {
					throw new DataException();
				}
			}
		} catch (SQLException e) {
			L.error("", e);
			throw new DataException(e);
		}
	}
	public boolean openbooking(String kennzeichen){
		String sql = "Select * From BUCHUNGSTATUS join BUCHUNG B on BUCHUNGSTATUS.B_ID = B.B_ID join MAUTABSCHNITT M on B.ABSCHNITTS_ID = M.ABSCHNITTS_ID  Where B.KENNZEICHEN = ? AND B.B_ID = 1";
		L.info(sql);
		try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
			ps.setString(1, kennzeichen);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next();
			}
		} catch (SQLException e) {
			L.error("", e);
			throw new DataException(e);
		}
	}
	public Long getfzg_id(Long fz_id) {
		String sql = "SELECT fzg_id FROM Fahrzeuggerat WHERE fz_id = ?";
		L.info(sql);
		try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
			ps.setLong(1, fz_id);
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
