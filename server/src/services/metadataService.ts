/**
 * 元数据服务模块
 * 负责接口元数据的 CRUD 操作
 */
import {
  ApiMetadata,
  ApiMetadataWithCount,
  CreateMetadataDto,
  UpdateMetadataDto,
  OutputConfig
} from '../models/types';
import { getDatabase, query, run, tableExists, saveDatabase } from './database';
import { validateCreateMetadataDto, ValidationError } from '../models/validators';
import { updateLogService } from './updateLogService';

/**
 * 元数据服务类
 */
export class MetadataService {
  private readonly allowedRowBgColors = new Set([
    '#ffffff',
    '#fdf2f2',
    '#fff7e8',
    '#f7f8e8',
    '#f0f9eb',
    '#ecf5ff',
    '#f4f0ff'
  ]);

  /**
   * 创建新的元数据
   */
  async createMetadata(dto: CreateMetadataDto): Promise<ApiMetadata> {
    // 验证输入
    validateCreateMetadataDto(dto);
    
    // 检查表名是否已存在
    const existing = query<ApiMetadata>(
      'SELECT * FROM api_metadata WHERE table_name = ?',
      [dto.table_name]
    );
    if (existing.length > 0) {
      throw new ValidationError(`表名 ${dto.table_name} 已存在`);
    }
    
    const now = new Date().toISOString();
    const requestMethod = dto.request_method || 'GET';
    
    // 插入元数据记录
    run(
      `INSERT INTO api_metadata (cn_name, source_url, request_method, output_config, table_name, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        dto.cn_name,
        dto.source_url,
        requestMethod,
        JSON.stringify(dto.output_config),
        dto.table_name,
        now,
        now
      ]
    );
    
    // 通过 table_name 查询刚插入的记录（table_name 是唯一的）
    const inserted = query<ApiMetadata>(
      'SELECT * FROM api_metadata WHERE table_name = ?',
      [dto.table_name]
    );
    
    if (inserted.length === 0) {
      throw new Error('创建元数据失败');
    }
    
    const id = inserted[0].id;
    
    // 创建对应的业务数据表
    await this.createBusinessTable(dto.table_name, dto.output_config, id);
    
    return inserted[0];
  }

  /**
   * 获取所有元数据列表（含记录计数）
   */
  async getMetadataList(includeDisabled = false): Promise<ApiMetadataWithCount[]> {
    const whereClause = includeDisabled ? '' : 'WHERE COALESCE(is_active, 1) = 1';
    const metadataList = query<ApiMetadata>(
      `SELECT * FROM api_metadata ${whereClause} ORDER BY sort_order ASC, id ASC`
    );

    const result: ApiMetadataWithCount[] = [];
    for (const metadata of metadataList) {
      const recordCount = await this.getBusinessDataCount(metadata.table_name);
      const lastDuration = updateLogService.getLatestDuration(metadata.id);
      result.push({
        ...metadata,
        record_count: recordCount,
        last_duration: lastDuration
      });
    }

    return result;
  }

  /**
   * 更新排序顺序
   */
  async updateSortOrder(orders: { id: number; sort_order: number }[]): Promise<void> {
    for (const item of orders) {
      run(
        'UPDATE api_metadata SET sort_order = ? WHERE id = ?',
        [item.sort_order, item.id]
      );
    }
  }

  /**
   * 根据 ID 获取单个元数据
   */
  async getMetadataById(id: number): Promise<ApiMetadata> {
    const results = query<ApiMetadata>(
      'SELECT * FROM api_metadata WHERE id = ?',
      [id]
    );
    
    if (results.length === 0) {
      throw new Error(`元数据不存在: ID ${id}`);
    }
    
    return results[0];
  }

  /**
   * 更新元数据
   */
  async updateMetadata(id: number, dto: UpdateMetadataDto): Promise<ApiMetadata> {
    // 获取现有记录
    const existing = await this.getMetadataById(id);
    
    const updates: string[] = [];
    const values: any[] = [];
    
    if (dto.cn_name !== undefined) {
      updates.push('cn_name = ?');
      values.push(dto.cn_name);
    }
    
    if (dto.source_url !== undefined) {
      updates.push('source_url = ?');
      values.push(dto.source_url);
    }
    
    if (dto.request_method !== undefined) {
      updates.push('request_method = ?');
      values.push(dto.request_method);
    }

    if (dto.output_config !== undefined) {
      updates.push('output_config = ?');
      values.push(JSON.stringify(dto.output_config));
      
      // 如果出参配置变更，需要更新业务表结构
      await this.alterBusinessTable(existing.table_name, dto.output_config, id);
    }

    if (dto.fields_verified !== undefined) {
      const rawValue = dto.fields_verified;
      if (
        rawValue !== 0 &&
        rawValue !== 1 &&
        rawValue !== true &&
        rawValue !== false
      ) {
        throw new ValidationError('fields_verified 只能是 0/1 或 true/false');
      }

      const normalizedValue = rawValue === 1 || rawValue === true ? 1 : 0;
      updates.push('fields_verified = ?');
      values.push(normalizedValue);
    }

    if (dto.row_bg_color !== undefined) {
      const normalizedColor = dto.row_bg_color === '' ? null : dto.row_bg_color;

      if (normalizedColor !== null && !this.allowedRowBgColors.has(normalizedColor)) {
        throw new ValidationError('row_bg_color 不在允许的浅色范围内');
      }

      updates.push('row_bg_color = ?');
      values.push(normalizedColor);
    }

    if (dto.is_active !== undefined) {
      const rawValue = dto.is_active;
      if (
        rawValue !== 0 &&
        rawValue !== 1 &&
        rawValue !== true &&
        rawValue !== false
      ) {
        throw new ValidationError('is_active must be 0/1 or true/false');
      }

      const normalizedValue = rawValue === 1 || rawValue === true ? 1 : 0;
      updates.push('is_active = ?');
      values.push(normalizedValue);
    }
    
    if (updates.length === 0) {
      return existing;
    }
    
    updates.push('updated_at = ?');
    values.push(new Date().toISOString());
    values.push(id);
    
    run(
      `UPDATE api_metadata SET ${updates.join(', ')} WHERE id = ?`,
      values
    );
    
    return this.getMetadataById(id);
  }

  /**
   * 删除元数据
   */
  async deleteMetadata(id: number): Promise<void> {
    const metadata = await this.getMetadataById(id);

    // 删除更新日志
    run('DELETE FROM update_logs WHERE metadata_id = ?', [id]);

    // 删除业务数据表
    await this.dropBusinessTable(metadata.table_name);

    // 删除元数据记录
    run('DELETE FROM api_metadata WHERE id = ?', [id]);
  }

  /**
   * 创建业务数据表
   */
  private async createBusinessTable(tableName: string, outputConfig: OutputConfig[], metadataId: number): Promise<void> {
    const columns = outputConfig.map(col => `${col.name} ${col.type}`).join(', ');
    
    const sql = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metadata_id INTEGER NOT NULL,
        ${columns},
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (metadata_id) REFERENCES api_metadata(id)
      )
    `;
    
    const db = getDatabase();
    db.run(sql);
    saveDatabase();
  }

  /**
   * 删除业务数据表
   */
  private async dropBusinessTable(tableName: string): Promise<void> {
    const db = getDatabase();
    db.run(`DROP TABLE IF EXISTS ${tableName}`);
    saveDatabase();
  }

  /**
   * 修改业务数据表结构
   * SQLite 不支持直接修改列，需要重建表
   */
  private async alterBusinessTable(tableName: string, newOutputConfig: OutputConfig[], metadataId: number): Promise<void> {
    const db = getDatabase();
    const tempTableName = `${tableName}_temp_${Date.now()}`;
    
    // 创建新表
    const columns = newOutputConfig.map(col => `${col.name} ${col.type}`).join(', ');
    db.run(`
      CREATE TABLE ${tempTableName} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metadata_id INTEGER NOT NULL,
        ${columns},
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (metadata_id) REFERENCES api_metadata(id)
      )
    `);
    
    // 获取旧表的列名
    const oldColumns = this.getTableColumns(tableName);
    const newColumnNames = newOutputConfig.map(c => c.name);
    
    // 找出共同的列
    const commonColumns = oldColumns.filter(col =>
      newColumnNames.includes(col) || ['id', 'metadata_id', 'created_at', 'collected_at'].includes(col)
    );
    
    if (commonColumns.length > 0) {
      // 复制数据到新表
      const columnList = commonColumns.join(', ');
      db.run(`INSERT INTO ${tempTableName} (${columnList}) SELECT ${columnList} FROM ${tableName}`);
    }
    
    // 删除旧表
    db.run(`DROP TABLE ${tableName}`);
    
    // 重命名新表
    db.run(`ALTER TABLE ${tempTableName} RENAME TO ${tableName}`);
    
    saveDatabase();
  }

  /**
   * 获取表的列名
   */
  private getTableColumns(tableName: string): string[] {
    const db = getDatabase();
    const result = db.exec(`PRAGMA table_info(${tableName})`);
    if (result.length === 0) return [];
    
    return result[0].values.map((row: any) => row[1] as string);
  }

  /**
   * 获取业务数据表的记录数
   */
  async getBusinessDataCount(tableName: string): Promise<number> {
    if (!tableExists(tableName)) {
      return 0;
    }
    
    const db = getDatabase();
    const result = db.exec(`SELECT COUNT(*) as count FROM ${tableName}`);
    if (result.length === 0 || result[0].values.length === 0) {
      return 0;
    }
    
    return result[0].values[0][0] as number;
  }

  /**
   * 更新最后更新时间
   */
  async updateLastUpdateTime(id: number): Promise<void> {
    run(
      'UPDATE api_metadata SET last_update_time = ?, updated_at = ? WHERE id = ?',
      [new Date().toISOString(), new Date().toISOString(), id]
    );
  }

  /**
   * 更新接口实际返回的字段列表
   * @param id 元数据 ID
   * @param fields 字段名数组
   */
  async updateLastApiFields(id: number, fields: string[]): Promise<void> {
    if (fields.length > 0) {
      run(
        'UPDATE api_metadata SET last_api_fields = ? WHERE id = ?',
        [JSON.stringify(fields), id]
      );
    }
  }
}

export const metadataService = new MetadataService();
