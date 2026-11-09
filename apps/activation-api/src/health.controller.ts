import { Controller, Get } from '@nestjs/common';

/** Usado por el healthcheck de Docker Compose. */
@Controller('health')
export class HealthController {
  @Get()
  check() {
    return { status: 'ok' };
  }
}
