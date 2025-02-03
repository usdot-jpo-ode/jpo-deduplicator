
# jpo-deduplicator

**US Department of Transportation (USDOT) Intelligent Transportation Systems (ITS) Joint Program Office (JPO) Message Deduplicator**

The JPO-Deduplicator is a Kafka Java spring-boot application designed to reduce the number of messages stored and processed in the ODE system. This is done by reading in messages from an input topic (such as topic.ProcessedMap) and outputting a subset of those messages on a related output topic (topic.DeduplicatedProcessedMap). Functionally, this is done by removing deduplicate messages from the input topic and only passing on unique messages. In addition, each topic will pass on at least 1 message per hour even if the message is a duplicate. This behavior helps ensure messages are still flowing through the system.

The following topics currently support deduplication.

- topic.ProcessedMap -> topic.DeduplicatedProcessedMap
- topic.ProcessedMapWKT -> topic.DeduplicatedProcessedMapWKT
- topic.OdeMapJson -> topic.DeduplicatedOdeMapJson
- topic.OdeTimJson -> topic.DeduplicatedOdeTimJson
- topic.OdeRawEncodedTIMJson -> topic.DeduplicatedOdeRawEncodedTIMJson
- topic.OdeBsmJson -> topic.DeduplicatedOdeBsmJson
- topic.ProcessedSpat -> topic.DeduplicatedProcessedSpat

## Release Notes

The current version and release history of the JPO Deduplicator: [Release Notes](<docs/Release_notes.md>)

<a name="table-of-contents"/>

## Table of Contents

- [jpo-deduplicator](#jpo-deduplicator)
  - [Release Notes](#release-notes)
  - [Table of Contents](#table-of-contents)
  - [1. System Requirements](#1-system-requirements)
  - [2. Usage Example](#2-usage-example)
  - [3. Configuration](#3-configuration)
    - [Software Prerequisites](#software-prerequisites)
    - [Feature Flags](#feature-flags)
    - [Generate a Github Token](#generate-a-github-token)
  - [4. Development Setup](#4-development-setup)
    - [Integrated Development Environment (IDE)](#integrated-development-environment-ide)
  - [5. Contact Information](#5-contact-information)
    - [License information](#license-information)
  - [6. Contributing](#6-contributing)

<a name="system-requirements"/>

## 1. System Requirements

Recommended machine specs running Docker to run the JPO-Deduplicator:

- Minimum RAM: 16 GB

- Minimum storage space: 100 GB
- Supported operating systems:

  - Ubuntu 20.04 Linux (Recommended)
  - Windows 10/11 Professional (Professional version required for Docker virtualization)
  - OSX 10 Mojave

The JPO-Deduplicator software can run on most standard Window, Mac, or Linux based computers with
Pentium core processors. Performance of the software will be based on the computing power and available RAM in
the system.  Larger data flows can require much larger space requirements depending on the amount of data being processed by the software. The JPO-Deduplicator software application was developed using the open source programming language Java. If running the JPO-Deduplicator outside of Docker, the application requires the Java 21 runtime environment.

[Back to top](#table-of-contents)

<a name="usage-example"/>

## 2. Usage Example

1. Create a copy of `sample.env` and rename it to `.env`.
2. Create a copy of the `jpo-utils/sample.env` file and rename it to `.env` in the `jpo-utils` directory.
3. Update the variable `MAVEN_GITHUB_TOKEN` in the root `.env` file to a github token used for downloading jar file dependencies. For full instructions on how to generate a token please see [here](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry#authenticating-to-github-packages).
4. Navigate back to the root directory and run the following command: `make build`
   1. Make sure that you have [docker-compose](https://docs.docker.com/compose/install/) and [make](https://www.gnu.org/software/make/manual/make.html) installed.
5. Run the following command: `make start`
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

[Back to top](#table-of-contents)

<a name="configuration"/>

## 3. Configuration

### Software Prerequisites

The JPO-Deduplicator is a micro service that runs as an independent application but serves the sole purpose of deduplicating JSON objects created by the JPO-ODE via Apache Kafka. To support these JSON objects, the JPO-Deduplicator application utilizes some classes from the JPO-ODE, JPO-GeojsonConverter, and the JPO-ConflictMonitor. These classes are referenced in the JPO-Deduplicator by pulling the built `.jar` artifact from GitHub Maven Central. All other required dependencies will automatically be downloaded and installed as part of the Docker build process.

- Docker: <https://docs.docker.com/engine/installation/>
- Docker-Compose: <https://docs.docker.com/compose/install/>

### Feature Flags

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

[Back to top](#table-of-contents)

<a name="development-setup"/>

## 4. Development Setup

### Integrated Development Environment (IDE)

Install the IDE of your choice:

- Eclipse: [https://eclipse.org/](https://eclipse.org/)
- STS: [https://spring.io/tools/sts/all](https://spring.io/tools/sts/all)
- IntelliJ: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
- VS Code: [https://code.visualstudio.com/](https://code.visualstudio.com/)

[Back to top](#table-of-contents)

<a name="contact-information"/>

## 5. Contact Information

Contact the developers of the JPO-Deduplicator application by submitting a [Github issue](https://github.com/usdot-jpo-ode/jpo-deduplicator/issues).

### License information

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
file except in compliance with the License.

You may obtain a copy of the License at <http://www.apache.org/licenses/LICENSE-2.0>
Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied. See the License for the specific language governing
permissions and limitations under the [License](http://www.apache.org/licenses/LICENSE-2.0).

[Back to top](#table-of-contents)

<a name="contributing"/>

## 6. Contributing

Please read the ODE [contributing guide](https://github.com/usdot-jpo-ode/jpo-ode/blob/develop/docs/contributing_guide.md) to learn about our development process, how to propose pull requests and improvements, and how to build and test your changes to this project.

[Back to top](#table-of-contents)