package de.htwberlin.exceptions;

public class InvalidVehicleDataException extends ServiceException {

	 /** Die Konstante identifiziert die Klassenversion. */
	private static final long serialVersionUID = 1L;

	/**
     * Der Konstruktor erzeugt eine Exception.
     */
    public InvalidVehicleDataException() {
    }

    /**
     * Der Konstruktor erzeugt eine Exception mit einer Message.
     * @param message hält die Message.
     */
    public InvalidVehicleDataException(String message) {
        super(message);
    }

    /**
     * Der Konstruktor erzeugt eine Exception und verweist auf ein Throwable t.
     * @param t hält ein Throwable.
     */
    public InvalidVehicleDataException(Throwable t) {
        super(t);
    }
	
}
