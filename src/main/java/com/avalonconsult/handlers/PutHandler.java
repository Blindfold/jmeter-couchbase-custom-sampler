package com.avalonconsult.handlers;

import com.avalonconsult.constants.CBArguments;
import com.couchbase.client.java.document.StringDocument;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

/**
 * @author moorejm
 */
public class PutHandler extends AbstractCouchbaseHandler {

    protected String putContents;
    protected boolean random_key;
    protected int random_key_min;
    protected int random_key_max;
    protected String key;
    protected String value;
    private Random random;

    public PutHandler(JavaSamplerContext context) {
        super(context);

        this.random = new Random();
        this.random_key = context.getParameter(CBArguments.RANDOM_KEY, "false").equals("true");
        this.random_key_min = context.getIntParameter(CBArguments.RANDOM_KEY_MIN, 0);
        this.random_key_max = context.getIntParameter(CBArguments.RANDOM_KEY_MAX, 1000000);
        this.key = context.getParameter(CBArguments.KEY);
        this.value = context.getParameter(CBArguments.VALUE);

        String file = context.getParameter(CBArguments.LOCAL_FILE_PATH);
        if (!(file == null || file.isEmpty())) {
            try {
                this.putContents = Files.readAllLines(Paths.get(file)).toString();
            } catch (IOException e) {
                LOGGER.error(getClass().getSimpleName() + ": File read error", e);
            }
        }

    }

    @Override
    public void handle() {
        if (random_key) {
            key = String.valueOf(random.nextInt(random_key_max - random_key_min));
        }

        if (value.isEmpty()) {
            value = this.putContents;
        }

        if (debug) {
            LOGGER.info(String.format("[%s] Putting key-value: (%s,%s)",
                    getClass().getSimpleName(), key, value));
        }

        bucket.upsert(StringDocument.create(key, value));

    }
}
