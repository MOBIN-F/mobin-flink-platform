package com.mobin;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2021/9/10
 * Time: 4:34 PM
 */
public class MlinkException extends RuntimeException{
    private static final long serialVersionUID = 1L;

    public MlinkException(String message) {
        super(message);
    }

    public MlinkException(String message, Throwable e) {
        super(message, e);
    }
}
