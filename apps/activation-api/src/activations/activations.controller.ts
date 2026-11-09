import { Body, Controller, DefaultValuePipe, Get, HttpCode, HttpStatus, Param, ParseIntPipe, Post, Query } from '@nestjs/common';
import { ActivationsService } from './activations.service';
import { CreateActivationDto } from './create-activation.dto';

@Controller('activations')
export class ActivationsController {
  constructor(private readonly service: ActivationsService) {}

  /** RF-02: 202 Accepted con activationId y PENDING, sin esperar a los demás servicios. */
  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  create(@Body() dto: CreateActivationDto) {
    return this.service.create(dto);
  }

  /** Activaciones recientes, para la lista seleccionable de la demo-ui (no es un RF del documento). */
  @Get()
  findRecent(@Query('limit', new DefaultValuePipe(20), ParseIntPipe) limit: number) {
    return this.service.findRecent(Math.min(Math.max(limit, 1), 100));
  }

  /** RF-09: estado actual + historial de eventos. */
  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.service.findOne(id);
  }
}
