package com.synack.nifi.gcp.pubsub.consumer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.cloud.pubsub.ReceivedMessage;
import com.google.cloud.pubsub.Subscription;
import com.google.cloud.pubsub.SubscriptionInfo;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * @author Mikhail Sosonkin
 */
@Tags({ "gcp", "pubsub", "consumer", "nifi" })
@CapabilityDescription("Consumer of GCP Pubsib topic")
@SeeAlso({})
@ReadsAttributes({
        @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "name of the flow based on time"),
        @WritesAttribute(attribute = "ack_id", description = "GCP meassge ACK id") })
public class GcpPubsubNifiConsumer extends AbstractProcessor {

    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final PropertyDescriptor authProperty = new PropertyDescriptor.Builder().name("Authentication Keys")
            .description("Required if outside of GCS. OAuth token (contents of myproject.json)")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor topicProperty = new PropertyDescriptor.Builder().name("Topic")
            .description("Name of topic")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor subProperty = new PropertyDescriptor.Builder().name("Subscription")
            .description("Name of the subscription")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor projectIdProperty = new PropertyDescriptor.Builder().name("Project ID")
            .description("Project ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor pullCountProperty = new PropertyDescriptor.Builder().name("Pull Count")
            .description("Number of messages to pull at request")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles received from Pubsub.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    private PubSub pubsub;
    private Subscription sub;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final PropertyDescriptor clientNameWithDefault = new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(subProperty)
                .defaultValue("NiFi-" + getIdentifier())
                .build();

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(authProperty);
        descriptors.add(topicProperty);
        descriptors.add(projectIdProperty);
        descriptors.add(clientNameWithDefault);
        descriptors.add(pullCountProperty);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (pubsub == null) {
            PubSubOptions.Builder opts = PubSubOptions.newBuilder()
                    .setProjectId(context.getProperty(projectIdProperty).getValue());

            PropertyValue auth = context.getProperty(authProperty);
            if (auth.isSet()) {
                try {
                    opts = opts.setCredentials(ServiceAccountCredentials.fromStream(new ByteArrayInputStream(auth.getValue().getBytes())));
                } catch (IOException ex) {
                    throw new ProcessException("Error setting credentials", ex);
                }
            }


            pubsub = opts.build().getService();

            if (pubsub == null) {
                throw new ProcessException("Pubsub not initialized");
            }

            getLogger().info("created pubsub");
        }

        if (sub == null) {
            String subName = context.getProperty(subProperty).getValue();
            String topicName = context.getProperty(topicProperty).getValue();

            sub = pubsub.getSubscription(subName);

            if (sub == null) {
                sub = pubsub.create(SubscriptionInfo.of(topicName, subName));
            }

            if (sub == null) {
                throw new ProcessException("Subscription not initialized: " + subName);
            }

            getLogger().info("subscription: " + subName);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        int pullCount = context.getProperty(pullCountProperty).asInteger();

        if (pullCount <= 0) {
            pullCount = 10;

            getLogger().info("PullCount is not valid(" + pullCount + ") defaulting to 10");
        }

        Iterator<ReceivedMessage> messages = sub.pull(pullCount);
        while (messages.hasNext()) {
            final ReceivedMessage msg = messages.next();
            final PubSubMessage pubSubMessage = new PubSubMessage(msg.getAttributes(), msg.getPayloadAsString());
            FlowFile flow = session.create();

            flow = session.write(flow, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(OBJECT_MAPPER.writeValueAsBytes(pubSubMessage));
                }
            });

            flow = session.putAttribute(flow, "ack_id", msg.getAckId());
            flow = session.putAttribute(flow, "filename", Long.toString(System.nanoTime()));

            session.transfer(flow, REL_SUCCESS);

            msg.ack();
        }

        session.commit();
    }

    public static class PubSubMessage {
        private Map<String, String> attributes;
        private String data;

        public PubSubMessage() {
        }

        public PubSubMessage(Map<String, String> attributes, String data) {
            this.attributes = attributes;
            this.data = data;
        }

        public Map<String, String> getAttributes() {
            return attributes;
        }

        public void setAttributes(Map<String, String> attributes) {
            this.attributes = attributes;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;

        }
    }
}
