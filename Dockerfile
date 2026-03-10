# --- STAGE 1: Build Stage ---
FROM node:20-alpine AS builder
WORKDIR /app

# Install build dependencies
COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile

# Copy source and build
COPY . .
RUN yarn build

# Remove development dependencies
RUN yarn install --production --frozen-lockfile

# --- STAGE 2: Production Stage ---
FROM node:20-alpine AS runner

# Set to production
ENV NODE_ENV=production
WORKDIR /app

# Security: Create a non-root user
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nestjs -u 1001 -G nodejs

# Copy only compiled code and production modules
COPY --chown=nestjs:nodejs --from=builder /app/package.json ./package.json
COPY --chown=nestjs:nodejs --from=builder /app/node_modules ./node_modules
COPY --chown=nestjs:nodejs --from=builder /app/dist ./dist

# Switch to the non-root user
USER nestjs

# Port is 3002 internally, but we won't expose it to the public
EXPOSE 3002

# Health check (internal)
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3002/api/health', (r) => { process.exit(r.statusCode === 200 ? 0 : 1) })"

# Start the application directly with Node (for proper signal handling)
CMD ["node", "dist/main.js"]