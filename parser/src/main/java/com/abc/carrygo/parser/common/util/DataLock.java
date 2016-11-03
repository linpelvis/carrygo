package com.abc.carrygo.parser.common.util;

/**
 * Created by plin on 10/11/16.
 */
public class DataLock {

    public static final Object writeDataLock = new Object();

    public static final Object readDataLock = new Object();
}
