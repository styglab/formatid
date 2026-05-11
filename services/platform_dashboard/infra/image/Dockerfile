FROM nginx:1.27-alpine

COPY services/platform_dashboard/infra/image/nginx.conf /etc/nginx/conf.d/default.conf
COPY services/platform_dashboard/index.html /usr/share/nginx/html/index.html
COPY services/platform_dashboard/app.js /usr/share/nginx/html/app.js
COPY services/platform_dashboard/styles.css /usr/share/nginx/html/styles.css
