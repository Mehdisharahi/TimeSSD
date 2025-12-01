# Base image with Node.js 20 on Debian
FROM node:20-bullseye

# Create app directory
WORKDIR /app

# Install system fonts we may use in canvas
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     fonts-dejavu-core \
  && rm -rf /var/lib/apt/lists/*

# Install dependencies first (better cache)
COPY package*.json ./
RUN npm install

# Copy rest of the source
COPY . .

# Build TypeScript
RUN npm run build

# Environment
ENV NODE_ENV=production

# Default command (Koyeb can override, but this is fine)
CMD ["node", "dist/index.js"]
