FROM nginx:1.27-alpine

RUN mkdir -p /etc/nginx/app-routes

COPY services/nginx/infra/nginx/default.conf /etc/nginx/conf.d/default.conf
COPY services/nginx/infra/nginx/routes /etc/nginx/routes
COPY deploy/compose/nginx/app-routes /etc/nginx/app-routes
