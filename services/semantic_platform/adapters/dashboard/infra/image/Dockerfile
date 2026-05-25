FROM nginx:1.27-alpine

COPY services/semantic_platform/adapters/dashboard/infra/image/nginx.conf /etc/nginx/conf.d/default.conf
COPY services/semantic_platform/adapters/dashboard/app/index.html /usr/share/nginx/html/index.html
COPY services/semantic_platform/adapters/dashboard/app/app.js /usr/share/nginx/html/app.js
COPY services/semantic_platform/adapters/dashboard/app/styles.css /usr/share/nginx/html/styles.css
