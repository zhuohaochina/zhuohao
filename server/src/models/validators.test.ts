/**
 * 验证函数单元测试
 */
import {
  validateOutputConfig,
  validateTableName,
  validateUrl,
  validateCreateMetadataDto,
  ValidationError
} from './validators';

describe('验证函数', () => {
  describe('validateOutputConfig', () => {
    it('应该接受有效的出参配置', () => {
      expect(validateOutputConfig({
        name: 'field1',
        type: 'TEXT'
      })).toBe(true);
    });

    it('应该拒绝空的字段名', () => {
      expect(() => validateOutputConfig({
        name: '',
        type: 'TEXT'
      })).toThrow(ValidationError);
    });

    it('应该拒绝无效的字段类型', () => {
      expect(() => validateOutputConfig({
        name: 'field1',
        type: 'VARCHAR'
      })).toThrow(ValidationError);
    });
  });

  describe('validateTableName', () => {
    it('应该接受有效的表名', () => {
      expect(validateTableName('valid_table_name')).toBe(true);
    });

    it('应该拒绝以数字开头的表名', () => {
      expect(() => validateTableName('123table')).toThrow(ValidationError);
    });

    it('应该拒绝包含特殊字符的表名', () => {
      expect(() => validateTableName('table-name')).toThrow(ValidationError);
    });

    it('应该拒绝空表名', () => {
      expect(() => validateTableName('')).toThrow(ValidationError);
    });

    it('应该拒绝过长的表名', () => {
      expect(() => validateTableName('a'.repeat(65))).toThrow(ValidationError);
    });
  });

  describe('validateUrl', () => {
    it('应该接受有效的 URL', () => {
      expect(validateUrl('https://example.com', 'URL')).toBe(true);
    });

    it('应该拒绝无效的 URL', () => {
      expect(() => validateUrl('not-a-url', 'URL')).toThrow(ValidationError);
    });

    it('应该拒绝空 URL', () => {
      expect(() => validateUrl('', 'URL')).toThrow(ValidationError);
    });
  });

  describe('validateCreateMetadataDto', () => {
    const validDto = {
      cn_name: '测试接口',
      source_url: 'https://example.com',
      output_config: [{ name: 'field1', type: 'TEXT' }],
      table_name: 'test_table'
    };

    it('应该接受有效的 DTO', () => {
      expect(validateCreateMetadataDto(validDto)).toBe(true);
    });

    it('应该拒绝空的中文名', () => {
      expect(() => validateCreateMetadataDto({
        ...validDto,
        cn_name: ''
      })).toThrow(ValidationError);
    });

    it('应该拒绝空的出参配置', () => {
      expect(() => validateCreateMetadataDto({
        ...validDto,
        output_config: []
      })).toThrow(ValidationError);
    });
  });
});
