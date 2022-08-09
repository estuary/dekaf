# based on https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/avro_consumer.py & https://github.com/sohailRa/beamAvroPubSub

from datetime import date
import logging
import os
import argparse
import typing

import apache_beam as beam
from apache_beam.io.external.kafka import ReadFromKafka
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

logging.getLogger().setLevel(logging.WARN)


class GameResult(typing.NamedTuple):
    team: str
    user: str
    score: int
    finished_at: date


def run(args):
    pipeline_options = beam.options.pipeline_options.PipelineOptions(
        runner="Direct",
        streaming=True,
    )

    # This is the schema the consumer will use for deserialization.
    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/../game_result.avsc") as f:
        schema_str = f.read()

    # The schema registry client will be used to get the schema used by the producer to encode the data.
    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    # Creating a deserializer requires both the consumer schema, and the schema registry client to look
    # up schemas used by the producer.
    avro_deserializer = AvroDeserializer(
        schema_registry_client, schema_str, lambda x, ctx: GameResult(**x))

    p = beam.Pipeline(options=pipeline_options)

    _ = (
        p
        | "read" >> ReadFromKafka(
            consumer_config={
                'bootstrap.servers': args.bootstrap_servers,
                'auto.offset.reset': "earliest",
            },
            topics=[args.topic],
            # Needed workaround for demo, see https://github.com/apache/beam/issues/20979
            max_num_records=10,
        )
        # Python beam SDK does not support schema registry, so we have to deserialize like this, see
        # https://github.com/apache/beam/issues/21225
        | "deserialize as types" >> beam.Map(lambda x: avro_deserializer(x[1], None))
        | "print results" >> beam.Map(lambda x: print(x))
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Beam example with Avro deserialization capabilities")

    # Servers must be accessible from the docker container that the beam runtime will start to run
    # the Java extension service. If running the kafka/registry servers on the same localhost as
    # this pipeline via direct runner, "localhost" will refer to the localhost of the Java extension
    # container, which will not have access to the localhost starting this pipeline.

    # Example usage, if you are running all services locally on a machine accessible via LAN at
    # 192.168.2.47: python main.py -b 192.168.2.47:9092 -r http://192.168.2.47:8081 -t game-results

    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-r', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", required=True,
                        help="Topic name")

    run(parser.parse_args())
