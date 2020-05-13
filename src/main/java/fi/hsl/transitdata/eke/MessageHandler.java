package fi.hsl.transitdata.eke;

import fi.hsl.common.hfp.*;
import fi.hsl.common.mqtt.proto.*;
import fi.hsl.common.pulsar.*;
import fi.hsl.common.transitdata.*;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import fi.hsl.eke.*;
import org.apache.pulsar.client.api.*;
import org.slf4j.*;

import java.io.*;


public class MessageHandler implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);
    private final EkeParser parser = EkeParser.newInstance();
    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;

    MessageHandler(PulsarApplicationContext context) {
        consumer = context.getConsumer();
        producer = context.getProducer();
    }

    public void handleMessage(Message received) {
        try {
            if (TransitdataSchema.hasProtobufSchema(received, ProtobufSchema.MqttRawMessage)) {
                final long timestamp = received.getEventTime();
                byte[] data = received.getData();

                FiHslEke.EkeMessage converted = parseData(data, timestamp);
                //We convert the EKE message here to make sure it's a valid EKE message even if we're merely passing it on
                //in the future EKE messages will likely be splitted onto other topics for organization
                sendPulsarMessage(received.getMessageId(), converted, timestamp);
            } else {
                log.warn("Received unexpected schema, ignoring.");
                ack(received.getMessageId()); //Ack so we don't receive it again
            }
        } catch (EkeParser.InvalidEkeMessageException invalidEKEException) {
            log.warn("Failed to parse EKE messge", invalidEKEException);
            //Ack messages with invalid data so they don't fill Pulsar backlog
            ack(received.getMessageId());
        } catch (Exception e) {
            log.error("Exception while handling message", e);
        }
    }

    private FiHslEke.EkeMessage parseData(byte[] data, long timestamp) throws IOException, HfpParser.InvalidHfpPayloadException, HfpParser.InvalidHfpTopicException {
        final Mqtt.RawMessage raw = Mqtt.RawMessage.parseFrom(data);
        final byte[] rawPayload = raw.getPayload().toByteArray();
        return parser.parseEkeMessage(rawPayload);
    }

    private void sendPulsarMessage(MessageId received, FiHslEke.EkeMessage ekeMessage, long timestamp) {
        producer.newMessage()
                //.key(dvjId) //TODO think about this
                .eventTime(timestamp)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, ProtobufSchema.FiHslEke.toString())
                .value(ekeMessage.toByteArray())
                .sendAsync()
                .whenComplete((MessageId id, Throwable t) -> {
                    if (t != null) {
                        log.error("Failed to send Pulsar message", t);
                        //Should we abort?
                    } else {
                        //Does this become a bottleneck? Does pulsar send more messages before we ack the previous one?
                        //If yes we need to get rid of this
                        ack(received);
                    }
                });

    }

    private void ack(MessageId received) {
        consumer.acknowledgeAsync(received)
                .exceptionally(throwable -> {
                    log.error("Failed to ack Pulsar message", throwable);
                    return null;
                })
                .thenRun(() -> {
                });
    }

}
