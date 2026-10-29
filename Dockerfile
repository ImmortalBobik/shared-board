FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY server.js ./
COPY public ./public
ENV NODE_OPTIONS=--max-old-space-size=160
EXPOSE 8000
CMD ["node", "server.js"]
