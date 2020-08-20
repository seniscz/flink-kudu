package com.birdsoaring.utils.exceptions;

import java.io.IOException;

/**
 * @author chezhao
 */
public class KuduTableException extends IOException {

    public KuduTableException(String msg) {
        super(msg);
    }
}
