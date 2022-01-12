package de.htwberlin.exceptions;

/**
 * Die Exception ist eine Oberklasse für alle Service-Exceptions.
 *
 * @author Martin Kempa
 */
public /* abstract */ class ServiceException extends RuntimeException {

    /** Die Konstante identifiziert die Klassenversion. */
    private static final long serialVersionUID = 1L;

    /**
     * Der Konstruktor erzeugt eine ServiceException.
     */
    public ServiceException() {
    }

    /**
     * Der Konstruktor erzeugt eine ServiceException mit einer Message.
     * @param message hält die Message.
     */
    public ServiceException(String message) {
        super(message);
    }

    /**
     * Der Konstruktor erzeugt eine ServiceException und verweist
     * auf ein Throwable t.
     * @param t hält ein Throwable.
     */
    public ServiceException(Throwable t) {
        super(t);
    }

}
