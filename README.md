
# jpo-deduplicator

<a name="deduplicator"></a>

## jpo-deduplicator
The JPO-Deduplicator is a Kafka Java spring-boot application designed to reduce the number of messages stored and processed in the ODE system. This is done by reading in messages from an input topic (such as topic.ProcessedMap) and outputting a subset of those messages on a related output topic (topic.DeduplicatedProcessedMap). Functionally, this is done by removing deduplicate messages from the input topic and only passing on unique messages. In addition, each topic will pass on at least 1 message per hour even if the message is a duplicate. This behavior helps ensure messages are still flowing through the system. The following topics currently support deduplication.

- topic.ProcessedMap -> topic.DeduplicatedProcessedMap
- topic.ProcessedMapWKT -> topic.DeduplicatedProcessedMapWKT
- topic.OdeMapJson -> topic.DeduplicatedOdeMapJson
- topic.OdeTimJson -> topic.DeduplicatedOdeTimJson
- topic.OdeRawEncodedTIMJson -> topic.DeduplicatedOdeRawEncodedTIMJson
- topic.OdeBsmJson -> topic.DeduplicatedOdeBsmJson
- topic.ProcessedSpat -> topic.DeduplicatedProcessedSpat

### Deduplication Config

When running the jpo-deduplication as a submodule in jpo-utils, the deduplicator will automatically turn on deduplication for a topic when that topic is created. For example if the KAFKA_TOPIC_CREATE_GEOJSONCONVERTER environment variable is set to true, the deduplicator will start performing deduplication for ProcessedMap, ProcessedMapWKT, and ProcessedSpat data. 

To manually configure deduplication for a topic, the following environment variables can also be used.

| Environment Variable | Description |
|---|---|
| `ENABLE_PROCESSED_MAP_DEDUPLICATION` | `true` / `false` - Enable ProcessedMap message Deduplication |
| `ENABLE_PROCESSED_MAP_WKT_DEDUPLICATION` | `true` / `false` - Enable ProcessedMap WKT message Deduplication |
| `ENABLE_ODE_MAP_DEDUPLICATION` | `true` / `false` - Enable ODE MAP message Deduplication |
| `ENABLE_ODE_TIM_DEDUPLICATION` | `true` / `false` - Enable ODE TIM message Deduplication |
| `ENABLE_ODE_RAW_ENCODED_TIM_DEDUPLICATION` | `true` / `false` - Enable ODE Raw Encoded TIM Deduplication |
| `ENABLE_PROCESSED_SPAT_DEDUPLICATION` | `true` / `false` - Enable ProcessedSpat Deduplication |
| `ENABLE_ODE_BSM_DEDUPLICATION` | `true` / `false` - Enable ODE BSM Deduplication |

### Generate a Github Token

A GitHub token is required to pull artifacts from GitHub repositories. This is required to obtain the jpo-deduplicator jars and must be done before attempting to build this repository.

1. Log into GitHub.
2. Navigate to Settings -> Developer settings -> Personal access tokens.
3. Click "New personal access token (classic)".
   1. As of now, GitHub does not support `Fine-grained tokens` for obtaining packages.
4. Provide a name and expiration for the token.
5. Select the `read:packages` scope.
6. Click "Generate token" and copy the token.
7. Copy the token name and token value into your `.env` file.

For local development the following steps are also required
8. Create a copy of [settings.xml](jpo-deduplicator/jpo-deduplicator/settings.xml) and save it to `~/.m2/settings.xml`
9. Update the variables in your `~/.m2/settings.xml` with the token value and target jpo-ode organization.

### Quick Run
1. Create a copy of `sample.env` and rename it to `.env`.
2. Update the variable `MAVEN_GITHUB_TOKEN` to a github token used for downloading jar file dependencies. For full instructions on how to generate a token please see here: 
3. Set the password for `MONGO_ADMIN_DB_PASS` and `MONGO_READ_WRITE_PASS` environmental variables to a secure password.
4. Set the `COMPOSE_PROFILES` variable to: `kafka,kafka_ui,kafka_setup, jpo-deduplicator`
5. Navigate back to the root directory and run the following command: `docker compose up -d`
6. Produce a sample message to one of the sink topics by using `kafka_ui` by:
   1. Go to `localhost:8001`
   2. Click local -> Topics
   3. Select `topic.OdeMapJson`
   4. Select `Produce Message`
   5. Copy in sample JSON for a Map Message
   6. Click `Produce  Message` multiple times
7. View the synced message in `kafka_ui` by:
   1. Go to `localhost:8001`
   2. Click local -> Topics
   3. Select `topic.DeduplicatedOdeMapJson`
   4. You should now see only one copy of the map message sent. 