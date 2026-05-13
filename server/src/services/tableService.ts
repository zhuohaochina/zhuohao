/**
 * 动态表管理服务
 * 负责业务数据表的创建、删除和修改
 */
import { getDatabase, saveDatabase, tableExists } from './database';
import { OutputConfig, ColumnConfig } from '../models/types';

const NUMERIC_SORT_FIELDS_BY_TABLE: Record<string, Set<string>> = {
  eastmoney_rzrq_hsa_raw: new Set([
    'RZYE',
    'RZYEZB',
    'FIN_BALANCE_GR'
  ])
}

/**
 * 动态表管理服务类
 */
export class TableService {
  private quoteIdentifier(identifier: string): string {
    return `"${identifier.replace(/"/g, '""')}"`
  }

  private escapeSqlLiteral(value: string): string {
    return value.replace(/'/g, "''")
  }

  private buildWhereClause(
    tableName: string,
    keyword?: string,
    searchColumn?: string,
    exactMatch?: boolean
  ): string {
    if (!keyword || !keyword.trim()) {
      return ''
    }

    const columns = this.getTableColumns(tableName)
    const escapedKeyword = this.escapeSqlLiteral(keyword)

    if (searchColumn && columns.includes(searchColumn)) {
      if (exactMatch) {
        return `WHERE CAST(${searchColumn} AS TEXT) = '${escapedKeyword}'`
      }
      return `WHERE CAST(${searchColumn} AS TEXT) LIKE '%${escapedKeyword}%'`
    }

    const searchColumns = columns.filter(col => !['id', 'metadata_id'].includes(col))
    const conditions = searchColumns.map(col => `CAST(${col} AS TEXT) LIKE '%${escapedKeyword}%'`)
    return conditions.length > 0 ? `WHERE (${conditions.join(' OR ')})` : ''
  }

  private buildOrderClause(
    tableName: string,
    sortField?: string,
    sortOrder?: 'asc' | 'desc'
  ): string {
    let orderClause = 'ORDER BY id DESC'

    if (tableName === 'eastmoney_stock_raw' && !sortField) {
      orderClause = 'ORDER BY f12 ASC'
    }

    if (tableName === 'eastmoney_shareholder_increase_raw' && !sortField) {
      orderClause = 'ORDER BY notice_date DESC, security_code DESC, eitime DESC'
    }

    if (tableName === 'eastmoney_executive_raw' && !sortField) {
      orderClause = 'ORDER BY change_date DESC, security_code ASC, person_name ASC'
    }

    if (tableName === 'eastmoney_announcement_raw' && !sortField) {
      orderClause = 'ORDER BY sort_date DESC, display_time DESC'
    }

    if (!sortField || !sortOrder) {
      return orderClause
    }

    const columns = this.getTableColumns(tableName)
    if (!columns.includes(sortField)) {
      return orderClause
    }

    const direction = sortOrder === 'asc' ? 'ASC' : 'DESC'
    const quotedSortField = this.quoteIdentifier(sortField)
    if (tableName === 'biz_eastmoney_jgdy_summary' && sortField === 'NOTICE_DATE' && columns.includes('SUM')) {
      const sumEmptyCase = "CASE WHEN SUM = '' OR SUM = '-' OR SUM IS NULL THEN 1 ELSE 0 END"
      const sumNumericExpr = "CAST(NULLIF(NULLIF(SUM, ''), '-') AS REAL)"
      return direction === 'ASC'
        ? `ORDER BY NOTICE_DATE ASC, ${sumEmptyCase}, ${sumNumericExpr} ASC, SECURITY_CODE ASC`
        : `ORDER BY NOTICE_DATE DESC, ${sumEmptyCase}, ${sumNumericExpr} DESC, SECURITY_CODE ASC`
    }

    if (this.shouldUseNumericSort(tableName, sortField)) {
      return `ORDER BY ${this.buildNumericNullCaseExpr(quotedSortField)}, ${this.buildNumericSortExpr(quotedSortField)} ${direction}, ${quotedSortField} ${direction}`
    }

    return `ORDER BY ${quotedSortField} ${direction}`
  }

  private buildNumericNullCaseExpr(quotedField: string): string {
    return `CASE WHEN ${quotedField} IS NULL OR TRIM(CAST(${quotedField} AS TEXT)) = '' OR TRIM(CAST(${quotedField} AS TEXT)) = '-' THEN 1 ELSE 0 END`
  }

  private buildNumericSortExpr(quotedField: string): string {
    return `CAST(NULLIF(NULLIF(TRIM(CAST(${quotedField} AS TEXT)), ''), '-') AS REAL)`
  }

  private normalizeValueForStorage(
    declaredType: string | undefined,
    value: any
  ): any {
    if (value === undefined || value === null) {
      return null
    }

    if (typeof value === 'object') {
      return JSON.stringify(value)
    }

    // 东方财富原始表大量使用 TEXT 存字段。
    // 直接把 JS number 写入 TEXT 列时，SQLite 会落成 "123.0" 这种文本。
    if (typeof value === 'number' && /TEXT/i.test(declaredType || '') && Number.isFinite(value)) {
      return String(value)
    }

    return value
  }

  private shouldUseNumericSort(tableName: string, fieldName: string): boolean {
    if (NUMERIC_SORT_FIELDS_BY_TABLE[tableName]?.has(fieldName)) {
      return true
    }

    if (tableName === 'eastmoney_stock_raw') {
      return !['f12', 'f14', 'f13', 'f1'].includes(fieldName)
    }

    const declaredType = this.getFieldDeclaredType(tableName, fieldName)
    return /INT|REAL|NUM|DEC|FLOAT|DOUBLE/i.test(declaredType)
  }

  private getFieldDeclaredType(tableName: string, fieldName: string): string {
    const outputType = this.getOutputConfigMap(tableName).get(fieldName)?.type || ''
    if (outputType) {
      return outputType
    }

    return this.getTableTypeMap(tableName).get(fieldName) || ''
  }

  private rowsToObjects(result: Array<{ columns: string[]; values: any[][] }>): Record<string, any>[] {
    if (result.length === 0) {
      return []
    }

    const columns = result[0].columns
    return result[0].values.map(row => {
      const obj: Record<string, any> = {}
      columns.forEach((col, index) => {
        obj[col] = row[index]
      })
      return obj
    })
  }
  /**
   * 创建业务数据表
   * @param tableName 表名
   * @param outputConfig 出参配置
   * @param metadataId 元数据 ID
   */
  createBusinessTable(tableName: string, outputConfig: OutputConfig[], metadataId: number): void {
    const db = getDatabase();

    // 生成列定义
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

    db.run(sql);
    saveDatabase();
  }

  /**
   * 删除业务数据表
   * @param tableName 表名
   */
  dropBusinessTable(tableName: string): void {
    const db = getDatabase();
    db.run(`DROP TABLE IF EXISTS ${tableName}`);
    saveDatabase();
  }

  /**
   * 修改业务数据表结构
   * SQLite 不支持直接修改列，需要重建表
   * @param tableName 表名
   * @param newOutputConfig 新的出参配置
   * @param metadataId 元数据 ID
   */
  alterBusinessTable(tableName: string, newOutputConfig: OutputConfig[], metadataId: number): void {
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

    // 找出共同的列（包括系统列）
    const systemColumns = ['id', 'metadata_id', 'created_at', 'collected_at'];
    const commonColumns = oldColumns.filter(col =>
      newColumnNames.includes(col) || systemColumns.includes(col)
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
   * @param tableName 表名
   */
  getTableColumns(tableName: string): string[] {
    const db = getDatabase();
    const result = db.exec(`PRAGMA table_info(${tableName})`);
    if (result.length === 0) return [];

    return result[0].values.map((row: any) => row[1] as string);
  }

  private getTableTypeMap(tableName: string): Map<string, string> {
    const db = getDatabase();
    const result = db.exec(`PRAGMA table_info(${tableName})`);
    if (result.length === 0) return new Map();

    return new Map(
      result[0].values.map((row: any) => [row[1] as string, (row[2] as string) || 'TEXT'])
    );
  }

  private getOutputConfigMap(tableName: string): Map<string, OutputConfig> {
    const db = getDatabase();
    const row = db.prepare(`
      SELECT output_config
      FROM api_metadata
      WHERE table_name = ?
      LIMIT 1
    `).get(tableName) as { output_config?: string } | undefined;

    if (!row?.output_config) {
      return new Map();
    }

    try {
      const config = JSON.parse(row.output_config) as OutputConfig[];
      return new Map((config || []).map((item) => [item.name, item]));
    } catch {
      return new Map();
    }
  }

  /**
   * 检查表是否存在
   * @param tableName 表名
   */
  tableExists(tableName: string): boolean {
    return tableExists(tableName);
  }

  /**
   * 获取表的记录数
   * @param tableName 表名
   */
  getRecordCount(tableName: string): number {
    if (!this.tableExists(tableName)) {
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
   * 清空表数据
   * @param tableName 表名
   */
  truncateTable(tableName: string): void {
    const db = getDatabase();
    db.run(`DELETE FROM ${tableName}`);
    saveDatabase();
  }

  /**
   * 确保表有指定的列，如果没有则自动添加
   * @param tableName 表名
   * @param columnNames 需要的列名数组
   */
  ensureColumns(tableName: string, columnNames: string[]): void {
    const db = getDatabase();
    const existingColumns = this.getTableColumns(tableName);
    const systemColumns = ['id', 'metadata_id', 'created_at', 'collected_at'];

    const existingColumnsLower = new Set(existingColumns.map(col => col.toLowerCase()));
    const systemColumnsLower = new Set(systemColumns.map(col => col.toLowerCase()));

    // 去重（忽略大小写），避免同名不同大小写重复尝试加列
    const dedupedColumnNames: string[] = [];
    const seen = new Set<string>();
    for (const col of columnNames) {
      const lower = col.toLowerCase();
      if (!seen.has(lower)) {
        dedupedColumnNames.push(col);
        seen.add(lower);
      }
    }

    // 找出需要添加的列（排除系统列）
    const missingColumns = dedupedColumnNames.filter(
      col => !existingColumnsLower.has(col.toLowerCase()) && !systemColumnsLower.has(col.toLowerCase())
    );

    if (missingColumns.length > 0) {
      console.log(`[TableService] 检测到 ${missingColumns.length} 个新字段，正在添加: ${missingColumns.join(', ')}`);

      for (const col of missingColumns) {
        try {
          // 默认使用 TEXT 类型，因为东方财富原始数据都存为字符串
          db.run(`ALTER TABLE ${tableName} ADD COLUMN ${col} TEXT`);
          console.log(`[TableService] 已添加列: ${col}`);
        } catch (error) {
          console.warn(`[TableService] 添加列 ${col} 失败:`, (error as Error).message);
        }
      }

      saveDatabase();
    }
  }

  /**
   * 批量插入数据（使用 INSERT OR REPLACE 实现增量更新）
   * @param tableName 表名
   * @param data 数据数组
   * @param metadataId 元数据 ID
   * @param uniqueKey 唯一键字段名（用于判断是否重复，如果提供则使用 INSERT OR REPLACE）
   */
  insertData(tableName: string, data: Record<string, any>[], metadataId: number, uniqueKey?: string): void {
    if (data.length === 0) return;

    const db = getDatabase();

    // 检测数据中的所有字段，确保表有这些列
    const dataFields = new Set<string>();
    for (const row of data) {
      for (const key of Object.keys(row)) {
        dataFields.add(key);
      }
    }
    this.ensureColumns(tableName, Array.from(dataFields));

    // 获取表的列（排除系统列）
    const columns = this.getTableColumns(tableName).filter(
      col => !['id', 'created_at', 'collected_at'].includes(col)
    );
    const columnTypeMap = this.getTableTypeMap(tableName);

    // 如果提供了唯一键，使用 INSERT OR REPLACE
    // 否则使用普通 INSERT
    const insertType = uniqueKey ? 'INSERT OR REPLACE' : 'INSERT';

    // 构建插入语句
    const placeholders = columns.map(() => '?').join(', ');
    const sql = `${insertType} INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;

    // 准备语句（better-sqlite3 会自动优化重复的 prepare）
    const stmt = db.prepare(sql);

    for (const row of data) {
      const rowLowerMap = new Map<string, any>();
      for (const [key, value] of Object.entries(row)) {
        const lowerKey = key.toLowerCase();
        if (!rowLowerMap.has(lowerKey)) {
          rowLowerMap.set(lowerKey, value);
        }
      }

      const values = columns.map(col => {
        if (col === 'metadata_id') return metadataId;

        let value: any;
        if (Object.prototype.hasOwnProperty.call(row, col)) {
          value = row[col];
        } else {
          value = rowLowerMap.get(col.toLowerCase());
        }

        // 如果值是对象或数组，序列化为 JSON 字符串
        if (typeof value === 'object' && value !== null) {
          return JSON.stringify(value);
        }
        return this.normalizeValueForStorage(columnTypeMap.get(col), value);
      });

      // better-sqlite3 使用 run() 方法执行语句
      stmt.run(...values);
    }

    saveDatabase();
  }

  updateJgdySummaryMainContent(
    tableName: string,
    updates: Array<{
      securityCode: string;
      receiveStartDate: string;
      noticeDate: string;
      mainContent: string;
    }>
  ): { attempted: number; updated: number } {
    if (!updates.length) {
      return { attempted: 0, updated: 0 };
    }

    const db = getDatabase();

    // 确保目标列存在
    this.ensureColumns(tableName, ['main_content']);

    const stmt = db.prepare(`
      UPDATE ${tableName}
      SET main_content = ?
      WHERE SECURITY_CODE = ?
        AND substr(RECEIVE_START_DATE, 1, 10) = ?
        AND substr(NOTICE_DATE, 1, 10) = ?
    `);

    let updated = 0;
    const transaction = db.transaction((items: typeof updates) => {
      for (const item of items) {
        const result = stmt.run(
          item.mainContent,
          item.securityCode,
          item.receiveStartDate,
          item.noticeDate
        );
        updated += result.changes || 0;
      }
    });

    transaction(updates);
    saveDatabase();

    return {
      attempted: updates.length,
      updated
    };
  }

  /**
   * 确保表有唯一索引（用于 INSERT OR REPLACE）
   * @param tableName 表名
   * @param uniqueKey 唯一键字段名（可以是逗号分隔的多个字段）
   */
  ensureUniqueIndex(tableName: string, uniqueKey: string): void {
    const db = getDatabase();
    // 将逗号替换为下划线，生成合法的索引名
    const indexName = `idx_${tableName}_${uniqueKey.replace(/,/g, '_')}`;

    // 创建唯一索引（如果不存在）
    try {
      db.run(`CREATE UNIQUE INDEX IF NOT EXISTS ${indexName} ON ${tableName} (${uniqueKey})`);
      saveDatabase();
      console.log(`[TableService] 已创建唯一索引: ${indexName} ON ${tableName} (${uniqueKey})`);
    } catch (error) {
      console.warn(`[TableService] 创建唯一索引失败: ${(error as Error).message}`);
    }
  }

  /**
   * 分页查询数据
   * @param tableName 表名
   * @param page 页码（从 1 开始）
   * @param pageSize 每页条数
   * @param keyword 搜索关键词（可选，对所有列进行模糊匹配）
   * @param sortField 排序字段（可选）
   * @param sortOrder 排序方向（可选，'asc' 或 'desc'）
   * @param searchColumn 搜索列（可选，指定在哪一列搜索，不指定则搜索所有列）
   * @param exactMatch 是否精确匹配（可选，默认 false 使用 LIKE 模糊匹配）
   */
  queryData(
    tableName: string,
    page: number,
    pageSize: number,
    keyword?: string,
    sortField?: string,
    sortOrder?: 'asc' | 'desc',
    searchColumn?: string,
    exactMatch?: boolean
  ): { data: any[]; total: number } {
    if (!this.tableExists(tableName)) {
      return { data: [], total: 0 };
    }

    const db = getDatabase();
    const offset = (page - 1) * pageSize;

    // 构建搜索条件
    let whereClause = '';
    if (keyword && keyword.trim()) {
      const columns = this.getTableColumns(tableName);
      const escapedKeyword = keyword.replace(/'/g, "''");

      if (searchColumn && columns.includes(searchColumn)) {
        // 指定列搜索
        if (exactMatch) {
          // 精确匹配模式
          whereClause = `WHERE CAST(${searchColumn} AS TEXT) = '${escapedKeyword}'`;
        } else {
          // 模糊匹配模式
          whereClause = `WHERE CAST(${searchColumn} AS TEXT) LIKE '%${escapedKeyword}%'`;
        }
      } else {
        // 全列搜索（只支持模糊匹配）
        const searchColumns = columns.filter(col => !['id', 'metadata_id'].includes(col));
        const conditions = searchColumns.map(col => `CAST(${col} AS TEXT) LIKE '%${escapedKeyword}%'`);
        whereClause = `WHERE (${conditions.join(' OR ')})`;
      }
    }

    // 获取总数
    const countResult = db.exec(`SELECT COUNT(*) FROM ${tableName} ${whereClause}`);
    const total = countResult.length > 0 ? countResult[0].values[0][0] as number : 0;

    // 构建排序子句
    let orderClause = 'ORDER BY id DESC';  // 默认按 id 降序

    // 对于东方财富原始数据表，默认按股票代码(f12)升序排序
    if (tableName === 'eastmoney_stock_raw' && !sortField) {
      orderClause = 'ORDER BY f12 ASC';
    }

    // 对于股东增减持表，默认按公告日期、股票代码、数据更新时间降序排序（与官网一致）
    if (tableName === 'eastmoney_shareholder_increase_raw' && !sortField) {
      orderClause = 'ORDER BY notice_date DESC, security_code DESC, eitime DESC';
    }

    // 对于高管增减持表，默认按变动日期降序、股票代码升序、人名升序排序（与官网API一致）
    if (tableName === 'eastmoney_executive_raw' && !sortField) {
      orderClause = 'ORDER BY change_date DESC, security_code ASC, person_name ASC';
    }

    // 对于A股公告表，默认按排序日期降序、显示时间降序（与东方财富官网一致）
    if (tableName === 'eastmoney_announcement_raw' && !sortField) {
      orderClause = 'ORDER BY sort_date DESC, display_time DESC';
    }

    if (sortField && sortOrder) {
      // 验证排序字段是否存在于表中，防止 SQL 注入
      const columns = this.getTableColumns(tableName);
      if (columns.includes(sortField)) {
        const direction = sortOrder === 'asc' ? 'ASC' : 'DESC';
        if (tableName === 'biz_eastmoney_jgdy_summary' && sortField === 'NOTICE_DATE' && columns.includes('SUM')) {
          const sumEmptyCase = "CASE WHEN SUM = '' OR SUM = '-' OR SUM IS NULL THEN 1 ELSE 0 END";
          const sumNumericExpr = "CAST(NULLIF(NULLIF(SUM, ''), '-') AS REAL)";
          orderClause = direction === 'ASC'
            ? `ORDER BY NOTICE_DATE ASC, ${sumEmptyCase}, ${sumNumericExpr} ASC, SECURITY_CODE ASC`
            : `ORDER BY NOTICE_DATE DESC, ${sumEmptyCase}, ${sumNumericExpr} DESC, SECURITY_CODE ASC`;
        }
        // 对于东方财富原始数据表，数值字段需要转换为数字类型排序
        // 使用 CAST 将字段转换为 REAL 类型，确保按数字大小排序
        else if (tableName === 'eastmoney_stock_raw') {
          // 排除已知的文本字段（股票代码、股票名称等）
          const textFields = ['f12', 'f14', 'f13', 'f1'];
          if (!textFields.includes(sortField)) {
            // 数值字段：使用 CAST 转换为 REAL 进行排序
            // NULLIF 处理空字符串和 '-' 等非数字值，将其视为 NULL
            // 升序时 NULL 值排在最后，降序时 NULL 值排在最后
            if (direction === 'ASC') {
              // 升序：先按是否为空排序（0=非空，1=空），再按数值升序
              orderClause = `ORDER BY CASE WHEN ${sortField} = '' OR ${sortField} = '-' OR ${sortField} IS NULL THEN 1 ELSE 0 END, CAST(NULLIF(NULLIF(${sortField}, ''), '-') AS REAL) ${direction}`;
            } else {
              // 降序：先按是否为空排序（0=非空，1=空），再按数值降序
              orderClause = `ORDER BY CASE WHEN ${sortField} = '' OR ${sortField} = '-' OR ${sortField} IS NULL THEN 1 ELSE 0 END, CAST(NULLIF(NULLIF(${sortField}, ''), '-') AS REAL) ${direction}`;
            }
          } else {
            orderClause = `ORDER BY ${sortField} ${direction}`;
          }
        } else {
          orderClause = `ORDER BY ${sortField} ${direction}`;
        }
      }
    }

    // 获取分页数据
    orderClause = this.buildOrderClause(tableName, sortField, sortOrder);
    const dataResult = db.exec(`SELECT * FROM ${tableName} ${whereClause} ${orderClause} LIMIT ${pageSize} OFFSET ${offset}`);

    if (dataResult.length === 0) {
      return { data: [], total };
    }

    // 转换为对象数组
    const columns = dataResult[0].columns;
    const data = dataResult[0].values.map(row => {
      const obj: Record<string, any> = {};
      columns.forEach((col, index) => {
        obj[col] = row[index];
      });
      return obj;
    });

    return { data, total };
  }

  queryAllData(
    tableName: string,
    keyword?: string,
    sortField?: string,
    sortOrder?: 'asc' | 'desc',
    searchColumn?: string,
    exactMatch?: boolean,
    selectedColumns?: string[]
  ): { data: any[]; total: number } {
    if (!this.tableExists(tableName)) {
      return { data: [], total: 0 };
    }

    const db = getDatabase();
    const tableColumns = this.getTableColumns(tableName);
    const whereClause = this.buildWhereClause(tableName, keyword, searchColumn, exactMatch);
    const orderClause = this.buildOrderClause(tableName, sortField, sortOrder);
    const countResult = db.exec(`SELECT COUNT(*) FROM ${tableName} ${whereClause}`);
    const total = countResult.length > 0 ? countResult[0].values[0][0] as number : 0;

    const validSelectedColumns = (selectedColumns || []).filter(col => tableColumns.includes(col));
    const exportColumns = validSelectedColumns.length > 0 ? validSelectedColumns : tableColumns;
    const selectClause = exportColumns.map(col => this.quoteIdentifier(col)).join(', ');
    const dataResult = db.exec(`SELECT ${selectClause} FROM ${tableName} ${whereClause} ${orderClause}`);

    return { data: this.rowsToObjects(dataResult), total };
  }

  /**
   * 获取每日数据统计
   * @param tableName 表名
   * @param dateField 日期字段名
   * @param dateFormat 日期格式化模板（使用 %s 作为字段占位符）
   * @returns 每日统计数组，按日期降序排列
   */
  getDailyStats(tableName: string, dateField: string, dateFormat: string): { date: string; count: number }[] {
    if (!this.tableExists(tableName)) {
      return [];
    }

    const db = getDatabase();

    // 检查字段是否存在
    const columns = this.getTableColumns(tableName);
    if (!columns.includes(dateField)) {
      // 如果指定字段不存在，尝试使用 created_at 或 collected_at
      if (columns.includes('created_at')) {
        dateField = 'created_at';
        dateFormat = 'substr(%s, 1, 10)';
      } else if (columns.includes('collected_at')) {
        dateField = 'collected_at';
        dateFormat = 'substr(%s, 1, 10)';
      } else {
        return [];
      }
    }

    // 构建日期提取表达式
    const dateExpr = dateFormat.replace('%s', dateField);

    // 执行统计查询
    const sql = `
      SELECT ${dateExpr} as date, COUNT(*) as count
      FROM ${tableName}
      WHERE ${dateField} IS NOT NULL AND ${dateField} != ''
      GROUP BY ${dateExpr}
      ORDER BY date DESC
    `;

    try {
      const result = db.exec(sql);

      if (result.length === 0 || result[0].values.length === 0) {
        return [];
      }

      return result[0].values.map(row => ({
        date: row[0] as string,
        count: row[1] as number
      }));
    } catch (error) {
      console.error(`[TableService] 获取每日统计失败:`, error);
      return [];
    }
  }
  /**
   * 按日期范围删除数据
   * @param tableName 表名
   * @param dateField 日期字段名
   * @param startDate 开始日期（格式：YYYY-MM-DD）
   * @param endDate 结束日期（可选，格式：YYYY-MM-DD）
   */
  /**
   * 统计所有数据量
   * @param tableName 表名
   * @returns 数据总数
   */
  countAllData(tableName: string): number {
    if (!this.tableExists(tableName)) {
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
   * 统计指定日期范围的数据量
   * @param tableName 表名
   * @param dateField 日期字段
   * @param startDate 开始日期
   * @param endDate 结束日期（可选）
   * @returns 数据总数
   */
  countDataByDateRange(tableName: string, dateField: string, startDate: string, endDate?: string): number {
    if (!this.tableExists(tableName)) {
      return 0;
    }

    const db = getDatabase();
    let sql = `SELECT COUNT(*) as count FROM ${tableName} WHERE ${dateField} >= ?`;
    const params: any[] = [startDate];

    if (endDate) {
      sql += ` AND ${dateField} <= ?`;
      params.push(endDate);
    }

    try {
      const stmt = db.prepare(sql);
      const result = stmt.get(...params) as any;
      return result?.count || 0;
    } catch (error) {
      console.error(`[TableService] 统计数据失败:`, error);
      return 0;
    }
  }

  deleteDataByDateRange(tableName: string, dateField: string, startDate: string, endDate?: string): void {
    if (!this.tableExists(tableName)) return;

    const db = getDatabase();
    let sql = `DELETE FROM ${tableName} WHERE ${dateField} >= ?`;
    const params: any[] = [startDate];

    if (endDate) {
      sql += ` AND ${dateField} <= ?`;
      params.push(endDate);
    }

    try {
      const info = db.run(sql, params); // better-sqlite3 run returns RunResult
      // @ts-ignore: better-sqlite3 info has changes property
      const changes = info.changes;
      console.log(`[TableService] 已删除 ${tableName} 中 ${startDate} ~ ${endDate || '至今'} 的数据，共 ${changes} 条`);
      saveDatabase();
    } catch (error) {
      console.error(`[TableService] 删除数据失败:`, error);
      throw error;
    }
  }
}


export const tableService = new TableService();
