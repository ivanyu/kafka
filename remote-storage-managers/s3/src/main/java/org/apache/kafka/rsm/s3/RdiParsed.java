package org.apache.kafka.rsm.s3;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kafka.log.remote.RDI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RdiParsed {

    private static final Logger log = LoggerFactory.getLogger(RdiParsed.class);

    private static final String RDI_POSITION_SEPARATOR = "#";
    private static final Pattern RDI_PATTERN = Pattern.compile("(.*)" + RDI_POSITION_SEPARATOR + "(\\d+)");

    private final String s3Key;
    private final int position;

    RdiParsed(byte[] rdi) {
        String rdiStr = new String(rdi, StandardCharsets.UTF_8);
        log.debug("Parsing RDI {}", rdiStr);

        Matcher m = RDI_PATTERN.matcher(rdiStr);
        if (!m.matches()) {
            throw new IllegalArgumentException("Can't parse RDI: " + rdiStr);
        }

        this.s3Key = m.group(1);
        this.position = Integer.parseInt(m.group(2));
    }

    final String getS3Key() {
        return s3Key;
    }

    final int getPosition() {
        return position;
    }

    static RDI createRDI(String s3Key, long position) {
        return new RDI((s3Key + RDI_POSITION_SEPARATOR + position)
            .getBytes(StandardCharsets.UTF_8));
    }
}
