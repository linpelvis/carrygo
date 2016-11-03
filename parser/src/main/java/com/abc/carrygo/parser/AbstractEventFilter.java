package com.abc.carrygo.parser;

import java.util.List;

/**
 * Created by plin on 9/20/16.
 */
public abstract class AbstractEventFilter {

    protected <T> List<T> process(Event<T> event) throws Exception {
        return event.parser();
    }

    protected interface Event<T> {
        List<T> parser() throws Exception;
    }
}
