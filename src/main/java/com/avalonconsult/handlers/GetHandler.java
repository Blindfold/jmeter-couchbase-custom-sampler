package com.avalonconsult.handlers;

import java.util.Random;

import com.avalonconsult.constants.CBArguments;
import com.couchbase.client.java.document.StringDocument;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;

/**
 * @author moorejm
 */
public class GetHandler extends AbstractCouchbaseHandler {

    protected String key;
    protected boolean random_key;
    protected int random_key_min;
    protected int random_key_max;
    private Random random;

    public GetHandler(JavaSamplerContext context) {
        super(context);
        this.random = new Random();
        this.random_key = context.getParameter(CBArguments.RANDOM_KEY, "false").equals("true");
        this.random_key_min = context.getIntParameter(CBArguments.RANDOM_KEY_MIN, 0);
        this.random_key_max = context.getIntParameter(CBArguments.RANDOM_KEY_MAX, 1000000);
        this.key = context.getParameter(CBArguments.KEY);
    }

    @Override
    public void handle() {
        if (random_key) {
            key = String.valueOf(random.nextInt(random_key_max - random_key_min));
        }

        if (debug) {
            LOGGER.debug(String.format("[%s] Getting key: %s",
                    getClass().getSimpleName(), key));
        }

        StringDocument doc = bucket.get(key, StringDocument.class);

        if (debug && doc != null) {
            LOGGER.info("GET Result: " + String.valueOf(doc.content()));
        }
    }
}
