FROM nginx:1.27-alpine

COPY services/runtime_dashboard/infra/image/nginx.conf /etc/nginx/conf.d/default.conf
COPY services/runtime_dashboard/index.html /usr/share/nginx/html/index.html
COPY services/runtime_dashboard/app.js /usr/share/nginx/html/app.js
COPY services/runtime_dashboard/styles.css /usr/share/nginx/html/styles.css
