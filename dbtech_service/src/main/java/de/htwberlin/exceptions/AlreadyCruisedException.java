package de.htwberlin.exceptions;

public class AlreadyCruisedException extends ServiceException {

	 /** Die Konstante identifiziert die Klassenversion. */
	private static final long serialVersionUID = 1L;

	/**
     * Der Konstruktor erzeugt eine Exception.
     */
    public AlreadyCruisedException() {
    }

    /**
     * Der Konstruktor erzeugt eine Exception mit einer Message.
     * @param message hält die Message.
     */
    public AlreadyCruisedException(String message) {
        super(message);
    }

    /**
     * Der Konstruktor erzeugt eine Exception und verweist auf ein Throwable t.
     * @param t hält ein Throwable.
     */
    public AlreadyCruisedException(Throwable t) {
        super(t);
    }
	
}
