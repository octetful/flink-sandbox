package com.ktech.repertoire.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

public class AggregatingJob {

  public static final String BOOTSTRAP_SERVERS = "broker:29092";
  public static final String INPUT_TOPIC = "source-event-stream";
  public static final String GROUP_ID = "";
  public static final String OUTPUT_TOPIC = "aggregated-agreements";

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    KafkaSource<Onboarding> kafkaSource = buildKafkaSource();

    KafkaSink<Agreement> kafkaSink = buildKafkaSink();

    env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
        .keyBy(Onboarding::getCorrelationId)
        .window(GlobalWindows.create())
        .trigger(new OnboardingCompletedTrigger())
        .aggregate(new AggregatorFunction())
        .sinkTo(kafkaSink);

    env.execute();
  }

  private static KafkaSink<Agreement> buildKafkaSink() {
    JsonSerializationSchema<Agreement> agreementJsonFormat =
        new JsonSerializationSchema<>();

    return KafkaSink.<Agreement>builder()
        .setBootstrapServers(BOOTSTRAP_SERVERS)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(OUTPUT_TOPIC)
            .setValueSerializationSchema(agreementJsonFormat)
            .build()
        )
        .build();
  }

  private static KafkaSource<Onboarding> buildKafkaSource() {
    JsonDeserializationSchema<Onboarding> onboardingJsonFormat =
        new JsonDeserializationSchema<>(Onboarding.class);

    return KafkaSource.<Onboarding>builder()
        .setBootstrapServers(BOOTSTRAP_SERVERS)
        .setTopics(INPUT_TOPIC)
        .setGroupId(GROUP_ID)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(onboardingJsonFormat)
        .build();
  }
}
