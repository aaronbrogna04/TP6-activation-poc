import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { createTransport, Transporter } from 'nodemailer';

export interface Mail {
  to: string;
  subject: string;
  text: string;
  html: string;
}

/** SMTP hacia Mailhog (mailhog:1025 en Compose; localhost:1025 desde el host). Sin auth ni TLS. */
@Injectable()
export class Mailer implements OnModuleDestroy {
  private readonly logger = new Logger(Mailer.name);
  private readonly from = process.env.MAIL_FROM ?? 'Activaciones <no-reply@activation-poc.example>';
  private readonly transport: Transporter = createTransport({
    host: process.env.SMTP_HOST ?? 'localhost',
    port: Number(process.env.SMTP_PORT ?? 1025),
    secure: false,
    ignoreTLS: true,
  });

  async send(mail: Mail): Promise<string> {
    const info = await this.transport.sendMail({ from: this.from, ...mail });
    return info.messageId;
  }

  onModuleDestroy(): void {
    this.transport.close();
    this.logger.log('SMTP cerrado');
  }
}
