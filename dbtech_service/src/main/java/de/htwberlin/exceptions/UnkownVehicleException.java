package de.htwberlin.exceptions;

public class UnkownVehicleException extends ServiceException {

	 /** Die Konstante identifiziert die Klassenversion. */
	private static final long serialVersionUID = 1L;

	/**
     * Der Konstruktor erzeugt eine Exception.
     */
    public UnkownVehicleException() {
    }

    /**
     * Der Konstruktor erzeugt eine Exception mit einer Message.
     * @param message hält die Message.
     */
    public UnkownVehicleException(String message) {
        super(message);
    }

    /**
     * Der Konstruktor erzeugt eine Exception und verweist auf ein Throwable t.
     * @param t hält ein Throwable.
     */
    public UnkownVehicleException(Throwable t) {
        super(t);
    }
	
}
