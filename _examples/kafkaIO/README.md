# Emulating Kafka for Beam Pipelines

The examples here are a "work-in-progress". They use `dekaf` to emulate a Kafka server for use in Apache Beam pipelines written using the [Java](https://beam.apache.org/documentation/sdks/java/) and [Python](https://beam.apache.org/documentation/sdks/python/) SDKs, using the [KafkaIO connector](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/kafka/KafkaIO.html) as a source.

Currently, the pipelines are set up to:
- Read messages from the unbounded KafkaIO source, starting from the first message
- Deserialize the messages from raw bytes into a data structure, depending on the message format & programming language
- Convert that data structure into an instance of class
- Print the result to `stdout`

The data produced by the emulator is roughly based on the [LeaderBoard: Streaming Processing with Real-Time Game Data](https://beam.apache.org/get-started/mobile-gaming-example/#leaderboard-streaming-processing-with-real-time-game-data) example from the Apache Beam docs and the pipelines could be further extended with more interesting behaviors along those lines. Demonstrating more complex Beam pipelines is not currently the focus of these examples.

**Note**: The Python Beam SDK uses an [expansion service](https://beam.apache.org/documentation/sdks/java-multi-language-pipelines/) to enable the KafkaIO connector. To use the direct runner with the expansion service using docker (the default mode of operation), the code running in the expansion service will be initiating network calls from within the container. Most notably, this means that running process locally on the same machine will not be reachable via `localhost` inside the expansion service container. Example commands listed here assume that the machine running the local process is available on the LAN at `192.168.2.47` because of this.

## JSON-Formatted Messages

Pipelines reading JSON-formatted Messages should run the emulation server from the `json` directory using a command similar to:
```
go run json/go-server/main.go -host 192.168.2.47 -port 9092 -topic game-results
```

The `dekaf` server produces JSON-formatted messages with a pseudo-random values:
```json
{
    "user":"user3",
    "team":"team5",
    "score":6,
    "finished_at":"2022-08-09T09:00:59.021432-03:00"
}
```

### Python JSON Pipeline

Instructions for installing Python, pip, and the Python Beam SDK can be found in the [Apache Beam Python SDK Quickstart](https://beam.apache.org/get-started/quickstart-py/).

An easy way to run the Python pipelines is to use a [virtual environment](https://docs.python.org/3/tutorial/venv.html) to install the dependencies:

```
$ python -m venv env
$ source env/bin/activate
$ pip install -r json/python/requirements.txt
```
- Regular usage of virtual environments may warrant consideration for tooling like [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io). These example commands assume a very minimal Python setup.

The pipeline can then be run using the direct runner:

```
$ python json/python/main.py -b 192.168.2.47:9092 -t game-results
```

Exit the virtual environment when finished by:
```
$ deactivate
```

### Java JSON Pipeline

Instructions for installing the Java Development Kit, Apache Maven, and the Java Beam SDK can be found in the [Apache Beam Java SDK Quickstart](https://beam.apache.org/get-started/quickstart-java/). Gradle is not used for the example Java pipelines.

Run the Java pipeline with the direct runner by executing this command in the same directory as the `pom.xml` file (`~/estuary/dekaf/_examples/kafkaIO/json/java`, for example):
```
$ mvn -e compile exec:java -Dexec.mainClass=org.estuary.FromDekaf -Pdirect-runner -Dexec.args="--inputTopic=game-results --bootstrapServer=192.168.2.47:9092"
```

## Avro-Formatted Messages

[Apache Avro](https://avro.apache.org/) is a common format for serializing Kafka messages. A separate `dekaf` emulation server is capable of producing Avro messages, interacting with a [schema registry](https://issues.apache.org/jira/browse/AVRO-1124) to enable production and consumption of these messages.

[https://github.com/aiven/karapace](https://github.com/aiven/karapace) can be used to run a schema registry locally. A [docker compose](https://docs.docker.com/get-started/08_using_compose/) file is available for starting karapace and its dependencies. Note that the karapace schema registry uses Kafka for its storage backend, and this instance of Kafka should not be confused with the emulated `dekaf` Kafka server. The compose file used in these examples does not expose the Kafka port used by karapace externally.

Avro messages are produced having this schema:
```json
{
   "type":"record",
   "name":"GameResult",
   "fields":[
      {
         "default":"",
         "name":"user",
         "type":"string"
      },
      {
         "default":"",
         "name":"team",
         "type":"string"
      },
      {
         "default":0,
         "name":"score",
         "type":"long"
      },
      {
         "default":0,
         "name":"finished_at",
         "type":{
            "logicalType":"timestamp-micros",
            "type":"long"
         }
      }
   ]
}
```

Start the schema registry using `docker-compose`:
```
$ docker-compose -f avro/docker-compose.yml up
```

Start the `dekaf` emulator producing Avro messages:
```
$ go run avro/go-server/main.go -host 192.168.2.47 -port 9092 -topic game-results -schema-registry http://localhost:8081
```
- The `schema-registry` URL in this case can be `localhost`, by the `host` argument needs to _not_ be `localhost` so that the host advertised by the `dekaf` "broker" is accessible by Java expansion service that runs in a separate docker container.



### Python Avro Pipeline

Similar to the JSON pipeline, use a virtual environment and run the Avro pipeline:
```
$ python -m venv env
$ source env/bin/activate
$ pip install -r avro/python/requirements.txt
$ python avro/python/main.py -b 192.168.2.47:9092 -r http://192.168.2.47:8081 -t game-results
$ deactivate
```

### Java Avro Pipeline

From the directory with the `pom.xml` file (`~/estuary/dekaf/_examples/kafkaIO/avro/java`, for example):
```
$ mvn -e compile exec:java -Dexec.mainClass=org.estuary.FromDekaf -Pdirect-runner -Dexec.args="--inputTopic=game-results --bootstrapServer=192.168.2.47:9092 --registry=http://localhost:8081"
```
