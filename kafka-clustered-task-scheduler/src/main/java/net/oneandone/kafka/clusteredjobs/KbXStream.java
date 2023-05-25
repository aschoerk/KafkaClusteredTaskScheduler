package net.oneandone.kafka.clusteredjobs;

import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;

/**
 * Used to marshal and unmarshall kafka events
 */
public class KbXStream {
    /**
     * The object used for marshalling and unmarshalling Signal-Events and NodeInformations
     */
    public static com.thoughtworks.xstream.XStream jsonXStream
            = new com.thoughtworks.xstream.XStream(new JettisonMappedXmlDriver());
    static {
        jsonXStream.ignoreUnknownElements();
        jsonXStream.allowTypesByRegExp(new String[]{"(net|com)\\.oneandone\\..*"});
    }

    KbXStream() {
        
    }
}
