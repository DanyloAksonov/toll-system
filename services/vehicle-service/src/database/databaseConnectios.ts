import { TypeOrmModule } from '@nestjs/typeorm';
import { join } from 'path';

export const databaseConnections = [
  TypeOrmModule.forRoot({
    type: 'postgres',
    host: process.env.DB_HOST || 'localhost',
    port: Number(process.env.DB_PORT) || 5432,
    username: process.env.DB_USERNAME || 'postgres',
    password: process.env.DB_PASSWORD || 'postgres',
    database: process.env.DB_NAME || 'postgres',
    entities: [
      // Add the paths to modules that use the main database here
      join(__dirname, '..', '**', '*.entity{.ts,.js}'),
    ],
    synchronize: true,
  }),
];
