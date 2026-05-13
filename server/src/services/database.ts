/**
 * 数据库服务模块
 * 负责 SQLite 数据库连接和基础操作
 * 使用 better-sqlite3 替代 sql.js，提供更好的性能和自动持久化
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

function resolveDbPath(): string {
  return process.env.DB_PATH || path.join(__dirname, '../../data/cninfo.db');
}

// 数据库实例
let db: Database.Database | null = null;
let dbPath: string;
// 保存原生 exec 方法的引用
let nativeExec: ((sql: string) => void) | null = null;

function createDatabaseConnection(targetPath: string): Database.Database {
  return new Database(targetPath, {
    verbose: process.env.NODE_ENV === 'development' ? console.log : undefined
  });
}

function closeDatabaseConnection(connection: Database.Database | null): void {
  if (!connection) {
    return;
  }

  try {
    connection.close();
  } catch {
    // 初始化恢复阶段忽略 close 错误
  }
}

function isRecoverableInitError(error: unknown): boolean {
  const code = String((error as { code?: string } | undefined)?.code || '').toUpperCase();
  const message = String((error as Error | undefined)?.message || '').toLowerCase();

  return (
    code === 'SQLITE_IOERR_TRUNCATE' ||
    (code.startsWith('SQLITE_IOERR') && message.includes('disk i/o error'))
  );
}

function cleanupSQLiteSidecars(targetPath: string): void {
  for (const suffix of ['-wal', '-shm', '-journal']) {
    const sidecarPath = `${targetPath}${suffix}`;
    if (!fs.existsSync(sidecarPath)) {
      continue;
    }

    fs.unlinkSync(sidecarPath);
    console.warn(`[Database] removed SQLite sidecar: ${path.basename(sidecarPath)}`);
  }
}

function ensureUpdateLogTables(connection: Database.Database): void {
  connection.exec(`
    CREATE TABLE IF NOT EXISTS update_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      metadata_id INTEGER NOT NULL,
      update_time TEXT NOT NULL,
      total_count INTEGER DEFAULT 0,
      daily_stats TEXT,
      status TEXT DEFAULT 'success',
      error_message TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      duration INTEGER,
      FOREIGN KEY (metadata_id) REFERENCES api_metadata(id)
    )
  `);

  connection.exec(`
    CREATE TABLE IF NOT EXISTS batch_update_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      update_time TEXT NOT NULL,
      total_count INTEGER DEFAULT 0,
      success_count INTEGER DEFAULT 0,
      error_count INTEGER DEFAULT 0,
      duration INTEGER,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);
}

/**
 * 初始化数据库
 */
export function initDatabase(): void {
  dbPath = resolveDbPath();
  const dataDir = path.dirname(dbPath);

  // 确保数据目录存在
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  let connection: Database.Database | null = null;
  try {
    // 创建或打开数据库（better-sqlite3 会自动处理文件）
    connection = createDatabaseConnection(dbPath);
    connection.pragma('journal_mode = WAL');
  } catch (error) {
    closeDatabaseConnection(connection);

    if (!isRecoverableInitError(error)) {
      throw error;
    }

    console.warn('[Database] WAL/SHM init failed, retrying after sidecar cleanup');
    cleanupSQLiteSidecars(dbPath);

    connection = createDatabaseConnection(dbPath);
    connection.pragma('journal_mode = WAL');
    console.warn('[Database] SQLite init recovered after sidecar cleanup');
  }

  db = connection;

  // 保存原生 exec 方法的引用
  nativeExec = db.exec.bind(db);

  // 创建 api_metadata 表
  db.exec(`
    CREATE TABLE IF NOT EXISTS api_metadata (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      cn_name TEXT NOT NULL,
      source_url TEXT NOT NULL,
      request_method TEXT DEFAULT 'GET',
      output_config TEXT NOT NULL,
      table_name TEXT NOT NULL UNIQUE,
      last_update_time TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // 检查并添加缺失的列
  ensureUpdateLogTables(db);

  const columns = getTableColumns('api_metadata');

  // 需要的列及其定义
  const requiredColumns = [
    { name: 'update_mode', definition: 'TEXT DEFAULT "full"' },
    { name: 'date_field', definition: 'TEXT' },
    { name: 'date_range', definition: 'INTEGER' },
    { name: 'future_days', definition: 'INTEGER' },
    { name: 'fixed_begin_date', definition: 'TEXT' },
    { name: 'sort_order', definition: 'INTEGER DEFAULT 0' },
    { name: 'default_sort_field', definition: 'TEXT' },
    { name: 'default_sort_order', definition: 'TEXT DEFAULT "desc"' },
    { name: 'datacenter_config', definition: 'TEXT' },
    { name: 'fields_verified', definition: 'INTEGER DEFAULT 0' },
    { name: 'row_bg_color', definition: 'TEXT' },
    { name: 'is_active', definition: 'INTEGER DEFAULT 1' }
  ];

  for (const col of requiredColumns) {
    if (!columns.includes(col.name)) {
      try {
        db.exec(`ALTER TABLE api_metadata ADD COLUMN ${col.name} ${col.definition}`);
        console.log(`[Database] 已添加列: api_metadata.${col.name}`);
      } catch (error) {
        console.warn(`[Database] 添加列 ${col.name} 失败:`, (error as Error).message);
      }
    }
  }

  console.log('[Database] 数据库初始化完成');
}

/**
 * 获取数据库实例
 */
export function getDatabasePath(): string {
  return dbPath || resolveDbPath();
}

export function getDatabase(): DatabaseWithExec {
  if (!db) {
    throw new Error('数据库未初始化，请先调用 initDatabase()');
  }

  // 添加 exec 方法以兼容 sql.js API
  // 强制覆盖 better-sqlite3 的原生 exec 方法，使用我们的兼容版本
  (db as any).exec = function(sql: string) {
    return exec(sql);
  };

  // 包装 run 方法以支持参数数组
  if (!(db as any)._runWrapped) {
    const originalPrepare = db.prepare.bind(db);
    (db as any).run = function(sql: string, params?: any[]) {
      if (params && params.length > 0) {
        return run(sql, params);
      } else {
        // 无参数的情况，直接执行
        db!.exec(sql);
      }
    };
    (db as any)._runWrapped = true;
  }

  return db as unknown as DatabaseWithExec;
}

/**
 * 保存数据库（better-sqlite3 自动持久化，此函数保留用于兼容性）
 */
export function saveDatabase(): void {
  // better-sqlite3 自动持久化到文件，无需手动保存
  // 此函数保留是为了与原有代码兼容
}

/**
 * 关闭数据库连接
 */
export function execNative(sql: string): void {
  if (!nativeExec) {
    throw new Error('?????????????? SQL');
  }

  nativeExec(sql);
}

export function closeDatabase(): void {
  if (db) {
    db.close();
    db = null;
    console.log('[Database] 数据库连接已关闭');
  }
}

/**
 * 检查表是否存在
 */
export function tableExists(tableName: string): boolean {
  if (!db) {
    throw new Error('数据库未初始化');
  }

  const result = db.prepare(
    "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
  ).get(tableName);

  return result !== undefined;
}

/**
 * 获取表的列名
 */
export function getTableColumns(tableName: string): string[] {
  if (!db) {
    throw new Error('数据库未初始化');
  }

  const result = db.prepare(`PRAGMA table_info(${tableName})`).all();
  return result.map((row: any) => row.name);
}

/**
 * 执行查询并返回结果（用于 SELECT 语句）
 */
export function query<T = any>(sql: string, params: any[] = []): T[] {
  if (!db) {
    throw new Error('数据库未初始化');
  }

  try {
    const stmt = db.prepare(sql);
    const results = stmt.all(...params);
    return results as T[];
  } catch (error) {
    console.error('[Database] 查询失败:', error);
    throw error;
  }
}

/**
 * 执行 SQL 语句（用于 INSERT/UPDATE/DELETE）
 */
export function run(sql: string, params: any[] = []): Database.RunResult {
  if (!db) {
    throw new Error('数据库未初始化');
  }

  try {
    const stmt = db.prepare(sql);
    const result = stmt.run(...params);
    return result;
  } catch (error) {
    console.error('[Database] 执行失败:', error);
    throw error;
  }
}

/**
 * 获取最后插入的行 ID
 */
export function getLastInsertRowId(): number {
  if (!db) {
    throw new Error('数据库未初始化');
  }

  const result = db.prepare('SELECT last_insert_rowid() as id').get() as { id: number };
  return result.id;
}

/**
 * 执行原始 SQL（用于兼容 sql.js 的 exec 方法）
 * 返回格式与 sql.js 兼容：{ columns: string[], values: any[][] }[]
 */
export function exec(sql: string): Array<{ columns: string[], values: any[][] }> {
  if (!db || !nativeExec) {
    throw new Error('数据库未初始化');
  }

  try {
    // 对于不返回结果的语句（CREATE, INSERT, UPDATE, DELETE 等）
    if (/^\s*(CREATE|INSERT|UPDATE|DELETE|DROP|ALTER)/i.test(sql.trim())) {
      nativeExec(sql);  // 使用原生 exec 方法
      return [];
    }

    // 对于返回结果的语句（SELECT 等）
    const stmt = db.prepare(sql);
    const results = stmt.all();

    if (results.length === 0) {
      return [];
    }

    // 获取列名
    const columns = Object.keys(results[0] as Record<string, any>);

    // 转换为 sql.js 格式的 values 数组
    const values = results.map((row: any) => columns.map(col => row[col]));

    return [{ columns, values }];
  } catch (error) {
    console.error('[Database] exec 执行失败:', error);
    throw error;
  }
}

// 为了兼容性，将 exec 方法添加到 Database 实例上
export interface DatabaseWithExec extends Omit<Database.Database, 'exec' | 'run'> {
  exec(sql: string): Array<{ columns: string[], values: any[][] }>;
  run(sql: string, params?: any[]): Database.RunResult | void;
}
