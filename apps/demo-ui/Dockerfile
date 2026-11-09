# demo-ui: build estático de Vite servido por nginx (sin dev server, ~10 MB de RAM).
FROM node:22.23.4-alpine AS build
ENV PNPM_HOME=/pnpm PATH=/pnpm:$PATH
RUN corepack enable
WORKDIR /repo
COPY pnpm-lock.yaml pnpm-workspace.yaml package.json ./
COPY apps/demo-ui ./apps/demo-ui
RUN --mount=type=cache,id=pnpm,target=/pnpm/store \
    pnpm install --frozen-lockfile --filter @poc/demo-ui
ARG VITE_API_URL=http://localhost:3000
ENV VITE_API_URL=${VITE_API_URL}
RUN pnpm --filter @poc/demo-ui build

FROM nginx:1.30.6-alpine
COPY apps/demo-ui/docker/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /repo/apps/demo-ui/dist /usr/share/nginx/html
EXPOSE 5173
