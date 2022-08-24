from datetime import date
import logging
import os
import argparse
import json
import typing

import apache_beam as beam
from apache_beam.io.external.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

logging.getLogger().setLevel(logging.WARN)


class GameResult(typing.NamedTuple):
    team: str
    user: str
    score: int
    finished_at: date


def run(parser, argv=None):
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True

    p = beam.Pipeline(options=pipeline_options)

    _ = (
        p
        | "read" >> ReadFromKafka(
            consumer_config={
                'bootstrap.servers': known_args.bootstrap_servers,
                'auto.offset.reset': 'earliest',
            },
            topics=[known_args.topic],
            # ByteArrayDeserializer is the default for both key and value; declaring explicitly here for clarity.
            key_deserializer="org.apache.kafka.common.serialization.ByteArrayDeserializer",
            value_deserializer="org.apache.kafka.common.serialization.ByteArrayDeserializer",
        )
        | "deserialize" >> beam.Map(lambda x: json.loads(x[1]))
        | "as types" >> beam.Map(lambda x: GameResult(**x)).with_output_types(GameResult)
        | "print results" >> beam.Map(lambda x: print(x))
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Servers must be accessible from the docker container that the beam runtime will start to run
    # the Java extension service. If running the kafka/registry servers on the same localhost as
    # this pipeline via direct runner, "localhost" will refer to the localhost of the Java extension
    # container, which will not have access to the localhost starting this pipeline.

    # Example usage, if you are running all services locally on a machine accessible via LAN at
    # 192.168.2.47: python main.py -b 192.168.2.47:9092 -r http://192.168.2.47:8081 -t game-results

    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-t', dest="topic", required=True,
                        help="Topic name")

    run(parser)
