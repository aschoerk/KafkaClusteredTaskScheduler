package net.oneandone.kafka.clusteredjobs;

import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;

/**
 * @author aschoerk
 */
public class KbXStream {
    public static com.thoughtworks.xstream.XStream jsonXStream
            = new com.thoughtworks.xstream.XStream(new JettisonMappedXmlDriver());
    static {
        jsonXStream.ignoreUnknownElements();
        jsonXStream.allowTypesByRegExp(new String[]{"(net|com)\\.oneandone\\..*"});
    }
}
