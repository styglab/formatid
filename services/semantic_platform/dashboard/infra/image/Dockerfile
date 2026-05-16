FROM nginx:1.27-alpine

COPY services/semantic_platform/dashboard/infra/image/nginx.conf /etc/nginx/conf.d/default.conf
COPY services/semantic_platform/dashboard/index.html /usr/share/nginx/html/index.html
COPY services/semantic_platform/dashboard/app.js /usr/share/nginx/html/app.js
COPY services/semantic_platform/dashboard/styles.css /usr/share/nginx/html/styles.css
