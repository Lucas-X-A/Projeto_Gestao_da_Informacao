FROM stain/jena-fuseki

# Garante permissões corretas para o Fuseki
USER root
RUN mkdir -p /fuseki/configuration /fuseki/databases/cti \
    && chown -R 100:100 /fuseki

# Copia o banco TDB2 pré-compilado
COPY --chown=100:100 tdb2/ /fuseki/databases/cti/

# Copia a configuração do dataset
COPY --chown=100:100 fuseki-config.ttl /fuseki/configuration/cti.ttl

# Volta para o usuário do Fuseki
USER 100
