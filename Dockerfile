# Dockerfile único para las apps NestJS del monorepo: docker build --build-arg APP=billing-service .
# Imagen final liviana: solo dist + dependencias de producción (pnpm deploy).

FROM node:22.23.4-alpine AS base
ENV PNPM_HOME=/pnpm PATH=/pnpm:$PATH
RUN corepack enable
WORKDIR /repo

FROM base AS build
ARG APP
COPY pnpm-lock.yaml pnpm-workspace.yaml package.json tsconfig.base.json ./
COPY libs ./libs
COPY apps/${APP} ./apps/${APP}
RUN --mount=type=cache,id=pnpm,target=/pnpm/store \
    pnpm install --frozen-lockfile --filter "@poc/${APP}..."
RUN pnpm --filter "@poc/${APP}..." build
RUN --mount=type=cache,id=pnpm,target=/pnpm/store \
    pnpm --filter "@poc/${APP}" deploy --prod /out

FROM node:22.23.4-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build --chown=node:node /out ./
USER node
CMD ["node", "--max-old-space-size=128", "dist/main.js"]
