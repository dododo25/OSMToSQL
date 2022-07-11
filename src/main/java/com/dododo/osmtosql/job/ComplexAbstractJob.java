package com.dododo.osmtosql.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

public abstract class ComplexAbstractJob extends AbstractJob {

    private final Logger logger;

    protected ComplexAbstractJob() {
        this.logger = LoggerFactory.getLogger(getClass());
    }

    @Override
    public Integer call() throws Exception {
        long startTime = System.currentTimeMillis();

        logger.info("started.");

        for (Callable<Integer> job : prepareJobs()) {
            int res = job.call();

            if (res != 0) {
                return res;
            }
        }

        logger.info("finished successfully in {}ms.", System.currentTimeMillis() - startTime);
        return callNext();
    }

    protected abstract List<Callable<Integer>> prepareJobs();
}
