package com.github.dragan;

/**
 * Created by dragan on 29.07.15.
 */
public class ClusterStarterException extends RuntimeException {
    public ClusterStarterException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClusterStarterException(String message) {
        super(message);
    }
}