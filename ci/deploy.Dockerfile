FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends bash postgresql-client ca-certificates && \
    rm -rf /var/lib/apt/lists/* && npm install --global caprover@2.3.1
