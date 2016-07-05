package cn.edu.ruc.realtime.utils;

/**
 * Created by Jelly on 7/4/16.
 * Config property not exist exception
 */
public class PropertyNotExistException extends Exception {
    public PropertyNotExistException(String message) {
        super(message);
    }

    public PropertyNotExistException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public PropertyNotExistException(Throwable throwable) {
        super(throwable);
    }
}
