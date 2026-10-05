import { Controller, Get, Param } from '@nestjs/common';
import { UserService } from './user.service';
import { UserEntity } from 'src/user/entities/user.entity';

@Controller('user')
export class UserController {
  constructor(private readonly userService: UserService) {}

  @Get(':id')
  findById(@Param('id') id: string): Promise<UserEntity> {
    return this.userService.findById(id);
  }
}
