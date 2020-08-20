package com.birdsoaring.utils.exceptions;

import java.io.IOException;

/**
 * @author chezhao
 */
public class KuduClientException extends IOException {

    public KuduClientException(String msg) {
        super(msg);
    }
}
