# Multi-stage build for standalone ChatBridge API, worker, and migrator containers.
FROM node:22-alpine3.21 AS builder

WORKDIR /app

COPY package.json package-lock.json tsconfig.json ./
RUN npm ci

COPY src ./src
COPY tests ./tests
COPY chat-core ./chat-core
COPY channel-management ./channel-management
COPY message-normalization ./message-normalization
COPY queue-and-retry ./queue-and-retry
COPY database-adapters ./database-adapters

RUN npm run build

FROM node:22-alpine3.21 AS runtime

RUN apk add --no-cache postgresql16-client

WORKDIR /app

ENV NODE_ENV=production

COPY --from=builder --chown=node:node /app/package.json ./package.json
COPY --from=builder --chown=node:node /app/dist ./dist
COPY --from=builder --chown=node:node /app/database-adapters/migrations ./database-adapters/migrations

USER node

EXPOSE 4200

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
  CMD ["node", "-e", "fetch('http://127.0.0.1:' + (process.env.CHATBRIDGE_PORT || '4200') + '/readyz').then((r) => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"]

CMD ["node", "dist/src/server.js"]
