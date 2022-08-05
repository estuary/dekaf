package org.estuary;

import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.StringDeserializer;

public class FromDekaf {
  public interface Options extends StreamingOptions {
    @Description("Apache Kafka topic to read from.")
    @Validation.Required
    String getInputTopic();

    void setInputTopic(String value);

    @Description("Apache Kafka bootstrap servers in the form 'hostname:port'.")
    @Validation.Required
    String getBootstrapServer();

    void setBootstrapServer(String value);

    @Description("Schema registry url.")
    @Validation.Required
    String getRegistry();

    void setRegistry(String value);
  }

  public static void main(final String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);

    var pipeline = Pipeline.create(options);

    Schema gameResultSchema = null;

    try {
      gameResultSchema = pipeline.getSchemaRegistry().getSchema(GameResult.class);
    } catch (NoSuchSchemaException e) {
      throw new IllegalArgumentException("Unable to get Schema for GameResult class.");
    }
    SerializableFunction<GenericRecord, Row> converter = AvroUtils.getGenericRecordToRowFunction(gameResultSchema);

    PCollection<GenericRecord> records = pipeline
        .apply("Read messages from Kafka",
            KafkaIO.<String, GenericRecord>read()
                .withBootstrapServers(options.getBootstrapServer())
                .withTopic(options.getInputTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(
                    ConfluentSchemaRegistryDeserializerProvider.of(options.getRegistry(),
                        options.getInputTopic() + "-value"))

                .withLogAppendTime()
                .commitOffsetsInFinalize()
                .withConsumerConfigUpdates(Map.of("group.id", "some-group", "auto.offset.reset", "earliest"))
                .withoutMetadata())
        // Extract values as GenericRecords.
        .apply(MapElements.via(
            new SimpleFunction<KV<String, GenericRecord>, GenericRecord>() {
              @Override
              public GenericRecord apply(KV<String, GenericRecord> kafkaVal) {
                return kafkaVal.getValue();
              }
            }));

    PCollection<GameResult> gameResults = records
        .apply("Convert generic records to Beam Rows", MapElements.via(
            new SimpleFunction<GenericRecord, Row>() {
              @Override
              public Row apply(GenericRecord rec) {
                return converter.apply(rec);
              }
            }))
        .setRowSchema(gameResultSchema)
        .apply("Convert to a user type with a compatible schema registered", Convert.to(GameResult.class));

    gameResults
        .apply(MapElements.into(TypeDescriptors.strings())
            .via(ts -> {
              System.out.println(ts);
              return ts.getTeam();
            }));

    pipeline.run();
  }
}
