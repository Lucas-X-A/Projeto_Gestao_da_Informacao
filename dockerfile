FROM stain/jena-fuseki

# Copia os arquivos de ontologia
COPY --chown=100:100 ontology/classes.ttl             /staging/classes.ttl
COPY --chown=100:100 ontology/individuals.ttl         /staging/individuals.ttl
COPY --chown=100:100 ontology/properties.ttl          /staging/properties.ttl
COPY --chown=100:100 ontology/description-bundle.owl  /staging/description-bundle.owl
COPY --chown=100:100 ontology/cti-pe.owl              /staging/cti-pe.owl
COPY --chown=100:100 ontology/vocab-bundle.owl        /staging/vocab-bundle.owl
COPY --chown=100:100 ontology/cti.owl                 /staging/cti.owl

# Garante que o diretório existe e pertence ao usuário do Fuseki
USER root
RUN mkdir -p /fuseki/configuration && chown -R 100:100 /fuseki

# Copia a configuração já com o dono correto
COPY --chown=100:100 fuseki-config.ttl /fuseki/configuration/cti.ttl

# Volta para o usuário do Fuseki
USER 100
