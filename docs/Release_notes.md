JPO-Deduplicator Release Notes
Version 2.0.0
Summary
The first release of the jpo-deduplicator package. This package focuses on removing duplicate messages from Kafka topics. This in turns saves deployers costs associated with data transfer and storage within the ODE ecosystem. This initial release included deduplication for the following messages types
 - ProcessedMap
 - ProcessedMapWKT
 - ProcessedSpat
 - OdeMapJson
 - OdeBSMJson
 - OdeTIMJson
 - OdeRawEncodedTIm

This release also makes additional changes to the submodule components as follows

Refactored docker-compose files to use compose profiles for modular component approach
Updated jpo-deduplicator for compatibility with jpo-ode version 4.0.0
Updated jpo-deduplicator to use zstd compression for output messages