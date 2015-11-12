package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

public class BulkProcessorHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkProcessorHelper.class.getName());

    public static void flush(BulkProcessor bulkProcessor) {
        try {
            Field field = bulkProcessor.getClass().getDeclaredField("bulkRequest");
            if (field != null) {
                field.setAccessible(true);
                BulkRequest bulkRequest = (BulkRequest) field.get(bulkProcessor);
                if (bulkRequest.numberOfActions() > 0) {
                    Method method = bulkProcessor.getClass().getDeclaredMethod("execute");
                    if (method != null) {
                        method.setAccessible(true);
                        method.invoke(bulkProcessor);
                    }
                }
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void waitFor(BulkProcessor bulkProcessor, TimeValue maxWait) {
        try {
            bulkProcessor.awaitClose(maxWait.getMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("interrupted");
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }
}
