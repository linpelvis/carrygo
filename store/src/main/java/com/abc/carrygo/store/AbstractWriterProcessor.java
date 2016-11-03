package com.abc.carrygo.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by plin on 10/17/16.
 */
public abstract class AbstractWriterProcessor {
    private static final Logger log = LoggerFactory.getLogger(AbstractWriterProcessor.class);


    public abstract void process();

}
