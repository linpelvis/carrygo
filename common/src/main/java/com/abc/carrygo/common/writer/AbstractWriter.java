package com.abc.carrygo.common.writer;

/**
 * Created by plin on 10/20/16.
 */
public abstract class AbstractWriter {
    public abstract void close() throws Exception;

    protected void process(Writer writer) {
        writer.message();
    }

    protected interface Writer<E> {
        void message();
    }

    protected interface WriterTransaction<T, F> {

        void alter(T t);

        void create(T t);

        void put(F t);

        void del(F t);

    }
}
