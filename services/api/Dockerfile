FROM node:20-alpine

RUN apk add --no-cache ffmpeg

WORKDIR /app

# Install API dependencies
COPY services/api/package.json services/api/package-lock.json* ./services/api/
RUN cd services/api && npm install

# Install script dependencies
COPY scripts/package.json ./scripts/
RUN cd scripts && npm install

# Copy source
COPY services/api/ ./services/api/
COPY scripts/ ./scripts/
COPY shared/ ./shared/
COPY public/ ./public/

# Build TypeScript
RUN cd services/api && npx tsc

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD wget -q --spider http://localhost:3000/health || exit 1

CMD ["node", "services/api/dist/index.js"]
