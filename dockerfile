FROM stain/jena-fuseki

COPY ontology/classes.ttl              /staging/classes.ttl
COPY ontology/individuals.ttl         /staging/individuals.ttl
COPY ontology/properties.ttl          /staging/properties.ttl
COPY ontology/description-bundle.owl  /staging/description-bundle.owl
COPY ontology/cti-pe.owl              /staging/cti-pe.owl
COPY ontology/vocab-bundle.owl        /staging/vocab-bundle.owl
COPY ontology/cti.owl                 /staging/cti.owl

COPY fuseki-config.ttl /fuseki/configuration/cti.ttl
