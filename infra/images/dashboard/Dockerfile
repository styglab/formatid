FROM nginx:1.27-alpine

COPY infra/images/dashboard/nginx.conf /etc/nginx/conf.d/default.conf
COPY services/dashboard /usr/share/nginx/html
