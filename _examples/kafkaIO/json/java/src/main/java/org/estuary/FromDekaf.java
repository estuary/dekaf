package org.estuary;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
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

    PCollection<String> json = pipeline
        .apply("Read messages from Kafka",
            KafkaIO.<String, String>read()
                // Standard configurations.
                .withBootstrapServers(options.getBootstrapServer())
                .withTopic(options.getInputTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)

                .withLogAppendTime()
                .commitOffsetsInFinalize()
                .withConsumerConfigUpdates(Map.of("group.id", "some-group", "auto.offset.reset", "earliest"))
                .withoutMetadata())
        // Extract values as JSON.
        .apply(Values.<String>create());

    PCollection<GameResult> gameResults = json
        .apply("Parse JSON to Beam Rows", JsonToRow.withSchema(gameResultSchema))
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
