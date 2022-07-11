package com.dododo.osmtosql.job;

import java.util.concurrent.Callable;

public abstract class AbstractJob implements Callable<Integer> {

    protected AbstractJob next;

    public final void setNext(AbstractJob next) {
        this.next = next;
    }

    protected final int callNext() throws Exception {
        return next == null ? 0 : next.call();
    }
}
