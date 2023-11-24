FROM --platform=linux/amd64 node:18

WORKDIR /app
COPY . .
RUN npm install

CMD ["node", "--max-old-space-size=8192", "jobs/generate_analysis_files.js"]