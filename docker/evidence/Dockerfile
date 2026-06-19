ARG NODE_IMAGE=node:22.17.0-bookworm-slim
FROM ${NODE_IMAGE}

WORKDIR /workspace/evidence

COPY evidence/package.json /workspace/evidence/package.json
RUN npm install

COPY evidence /workspace/evidence
