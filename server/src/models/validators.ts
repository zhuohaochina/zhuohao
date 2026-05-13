/**
 * 数据验证函数
 */
import { OutputConfig, CreateMetadataDto } from './types';

// 验证错误类
export class ValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ValidationError';
  }
}

/**
 * 验证出参配置
 */
export function validateOutputConfig(config: OutputConfig): boolean {
  if (!config.name || typeof config.name !== 'string') {
    throw new ValidationError('字段名称不能为空');
  }
  if (!config.type || !['TEXT', 'INTEGER', 'REAL'].includes(config.type)) {
    throw new ValidationError(`字段类型无效: ${config.type}，必须是 TEXT、INTEGER 或 REAL`);
  }
  return true;
}

/**
 * 验证表名格式
 */
export function validateTableName(tableName: string): boolean {
  if (!tableName || typeof tableName !== 'string') {
    throw new ValidationError('表名不能为空');
  }
  // 表名只能包含字母、数字和下划线，且必须以字母开头
  const tableNameRegex = /^[a-zA-Z][a-zA-Z0-9_]*$/;
  if (!tableNameRegex.test(tableName)) {
    throw new ValidationError('表名格式无效，只能包含字母、数字和下划线，且必须以字母开头');
  }
  // 表名长度限制
  if (tableName.length > 64) {
    throw new ValidationError('表名长度不能超过 64 个字符');
  }
  return true;
}

/**
 * 验证 URL 格式
 */
export function validateUrl(url: string, fieldName: string): boolean {
  if (!url || typeof url !== 'string') {
    throw new ValidationError(`${fieldName} 不能为空`);
  }
  try {
    new URL(url);
    return true;
  } catch {
    throw new ValidationError(`${fieldName} 格式无效`);
  }
}

/**
 * 验证创建元数据 DTO
 */
export function validateCreateMetadataDto(dto: CreateMetadataDto): boolean {
  // 验证中文名
  if (!dto.cn_name || typeof dto.cn_name !== 'string' || dto.cn_name.trim() === '') {
    throw new ValidationError('接口中文名不能为空');
  }

  // 验证来源 URL
  validateUrl(dto.source_url, '来源页面 URL');

  // 验证表名
  validateTableName(dto.table_name);

  // 验证出参配置
  if (!Array.isArray(dto.output_config) || dto.output_config.length === 0) {
    throw new ValidationError('出参配置不能为空');
  }
  dto.output_config.forEach((output, index) => {
    try {
      validateOutputConfig(output);
    } catch (e) {
      throw new ValidationError(`出参配置[${index}]: ${(e as Error).message}`);
    }
  });

  return true;
}
