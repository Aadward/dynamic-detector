package com.syh.example.exception;

public class JsonException extends RuntimeException {

    public JsonException(String message, Throwable cause) {
        super(message, cause);
    }
}
