package de.htwberlin.maut.test;

import static org.junit.Assert.assertEquals;
import java.net.URL;
import java.util.Properties;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.PropertiesBasedJdbcDatabaseTester;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.csv.CsvURLDataSet;
import org.dbunit.ext.oracle.OracleDataTypeFactory;
import org.dbunit.operation.DatabaseOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.exceptions.AlreadyCruisedException;
import de.htwberlin.exceptions.InvalidVehicleDataException;
import de.htwberlin.exceptions.UnkownVehicleException;
import de.htwberlin.mauterhebung.IMauterhebung;
import de.htwberlin.mauterhebung.MauterServiceImpl;
import de.htwberlin.test.utils.DbUnitUtils;
import de.htwberlin.utils.DbCred;

/**
 * Die Klasse enthaelt die Testfaelle fuer die Methoden des Mautservice /
 * Mauterhebung.
 * 
 * @author Patrick Dohmeier
 */
public class MautServiceTest {
	private static final Logger L = LoggerFactory.getLogger(MautServiceTest.class);
	private static IDatabaseTester dbTester;
	private static IDatabaseConnection dbTesterCon = null;
	private static String dataDirPath = "de/htwberlin/test/data/jdbc/";
	private static URL dataFeedUrl = ClassLoader.getSystemResource(dataDirPath);
	private static IDataSet feedDataSet = null;

	private static IMauterhebung maut = new MauterServiceImpl();

	// Wird vor jedem Test ausgeführt
	@org.junit.Before
	public void setUp() throws Exception {
		L.debug("start");
		try {

			dbTester = new JdbcDatabaseTester(DbCred.driverClass, DbCred.url, DbCred.user, DbCred.password,
					DbCred.schema);
			dbTesterCon = dbTester.getConnection();
			// use dbConn instead calling 'dbTester.getConnection()'
			feedDataSet = new CsvURLDataSet(dataFeedUrl);
			dbTester.setDataSet(feedDataSet);
			DatabaseOperation.CLEAN_INSERT.execute(dbTesterCon, feedDataSet);
			maut.setConnection(dbTesterCon.getConnection());
		} catch (Exception e) {
			DbUnitUtils.closeDbUnitConnectionQuietly(dbTesterCon);
			throw new RuntimeException(e);
		}
	}

	// Wird nach jedem Test ausgeführt
	@org.junit.After
	public void tearDown() throws Exception {
		L.debug("start");
		DbUnitUtils.closeDbUnitConnectionQuietly(dbTesterCon);

	}

	/**
	 * Der Testfall testet die Mauterhebung eines Fahrzeuges im Fehlerfall.
	 * 
	 */
	@org.junit.Test(expected = UnkownVehicleException.class)
	public void testMauterhebung_1() throws Exception {
		// Das Fahrzeug mit dem Kennzeichen LDS 677 ist nicht registriert bzw.
		// nicht aktiv und hat auch kein Fahrzeugger�t verbaut. Es
		// liegt auch keine offenen Buchung im Manuellen Verfahren vor . Das
		// unbekannte Fahrzeug f�hrt zu einer UnkownVehicleException.
		maut.berechneMaut(1200, 4, "LDS 677");
	}

	/**
	 * Der Testfall testet die Mauterhebung eines Fahrzeuges im Fehlerfall.
	 * 
	 */
	@org.junit.Test(expected = InvalidVehicleDataException.class)
	public void testMauterhebung_2() throws Exception {
		// Das Fahrzeug mit dem Kennzeichen HH 8499 ist bekannt und im
		// automatischen System unterwegs. Das Fahrzeug f�hrt mit einer h�heren
		// Achszahl als in der Datenbank registriert. In der Mautberechnung
		// f�hrt das zu einer niedrigeren Mauterhebung je Kilometer und zu einer
		// InvalidVehicleDataException.
		maut.berechneMaut(1200, 4, "HH 8499");
	}

	/**
	 * Der Testfall testet die Mauterhebung eines Fahrzeuges im Fehlerfall.
	 * 
	 */
	@org.junit.Test(expected = InvalidVehicleDataException.class)
	public void testMauterhebung_3() throws Exception {
		// Das Fahrzeug ist bekannt und im Manuellen Verfahren unterwegs.
		// Gebucht wurde aber eine falsche Mautkategorie, sprich eine falsche
		// Achszahl. Der Fahrer bezahlt also weniger Maut f�r die
		// Streckenbefahrung als er m�sste. Dies f�hrt zu einer
		// InvalidVehicleDataException.
		maut.berechneMaut(1200, 10, "B CV 8890");
	}

	/**
	 * Der Testfall testet die Mauterhebung eines Fahrzeuges im Fehlerfall.
	 * 
	 */
	@org.junit.Test(expected = AlreadyCruisedException.class)
	public void testMauterhebung_4() throws Exception {
		// Das Fahrzeug ist bekannt und im manuellen Verfahren unterwegs.
		// Auch die Mautkategorie stimmt. Allerdings f�hrt eine Doppelbefahrung
		// bei Einmalbuchung der gleichen Strecke zu einer
		// AlreadyCruisedException
		maut.berechneMaut(4174, 10, "DV 9413 NJ");
	}

	/**
	 * Der Testfall testet die Mauterhebung eines Fahrzeuges im Erfolgsfall.
	 * 
	 */
	@org.junit.Test
	public void testMauterhebung_5() throws Exception {
		// Das Fahrzeug ist bekannt und im manuellen Verfahren unterwegs.
		// Die Streckenbefahrung ist noch im Status offen, und
		// die Kontrolldaten stimmen mit den gebuchten Daten �berein.
		// Der Buchungsstatus der Streckenbefahrung wird auf "abgeschlossen"
		// gesetzt
		maut.berechneMaut(1200, 4, "B CV 8890");

		// hole Daten aus der aktuellen Tabelle BUCHUNG
		QueryDataSet databaseDataSet = new QueryDataSet(dbTesterCon);
		String sql = "select * from BUCHUNG order by BUCHUNG_ID asc";
		databaseDataSet.addTable("BUCHUNG", sql);
		// Vergleiche zu erwartendes Ergebnis mit dem tats�chlichen Ergebnis aus
		// der DB
		ITable actualTable = databaseDataSet.getTable("BUCHUNG");

		assertEquals("Die Buchung ist nicht auf abgeschlossen gesetzt worden", "3",
				actualTable.getValue(3, "B_ID").toString());
	}

	/**
	 * Der Testfall testet die Mauterhebung eines Fahrzeuges im Erfolgsfall.
	 * 
	 */
	@org.junit.Test
	public void testMauterhebung_6() throws Exception {
		// Das Fahrzeug ist bekannt & aktiv, sowie mit einem Fahrzeugger�t
		// ausgestattet und im Automatischen Verfahren unterwegs.
		// Die H�he der Maut wird anhand des gefahrenen Abschnitts und der
		// Mautkategorie berechnet und verbucht.
		float maut_berechnet = maut.berechneMaut(1433, 5, "M 6569");

		// hole Daten aus der aktuellen Tabelle MAUTERHEBUNG
		QueryDataSet databaseDataSet = new QueryDataSet(dbTesterCon);
		String sql = "select * from MAUTERHEBUNG order by maut_id asc";
		databaseDataSet.addTable("MAUTERHEBUNG", sql);
		// Vergleiche zu erwartendes Ergebnis mit dem tats�chlichen Ergebnis aus
		// der DB
		ITable actualTable = databaseDataSet.getTable("MAUTERHEBUNG");

		assertEquals("Die Berechnung der Maut war nicht korrekt", Float.toString(maut_berechnet), "0.68");

	}

}
