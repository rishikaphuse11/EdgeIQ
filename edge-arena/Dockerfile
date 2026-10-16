FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 5000
EXPOSE 7000-7100
CMD ["node", "agent.js"]
