import { Body, Controller, Get, HttpCode, HttpStatus, Param, Post } from '@nestjs/common';
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

  /** RF-09: estado actual + historial de eventos. */
  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.service.findOne(id);
  }
}
