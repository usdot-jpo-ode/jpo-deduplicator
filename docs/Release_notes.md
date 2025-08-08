JPO-Deduplicator Release Notes
Version 2.1.0
Summary
The second release of the jpo-deduplicator package. This release makes minor updates to how the deduplicator handles invalid messages. 
- Add error handling to all message topologies to prevent bad messages from crashing individual topologies
- Added additional null testing for BSM and Processed BSM messages. 
- Updated unit test coverage to expand coverage for BSM and Processed BSM use cases
- Removes Conflict Monitor as a dependency


Version 2.0.0
Summary
The first release of the jpo-deduplicator package. This package focuses on removing duplicate messages from Kafka topics. This in turns saves deployers costs associated with data transfer and storage within the ODE ecosystem. This initial release included deduplication for the following messages types
 - ProcessedMap
 - ProcessedMapWKT
 - ProcessedSpat
 - OdeMapJson
 - OdeBsmJson
 - OdeTimJson
 - OdeRawEncodedTim

This release also makes additional changes to the submodule components as follows

Refactored docker-compose files to use compose profiles for modular component approach
Updated jpo-deduplicator for compatibility with jpo-ode version 4.0.0
Updated jpo-deduplicator to use zstd compression for output messages