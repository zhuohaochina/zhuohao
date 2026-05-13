/**
 * 数据库服务模块
 * 负责 SQLite 数据库连接和基础操作
 * 使用 better-sqlite3 替代 sql.js，提供更好的性能和自动持久化
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

// 数据库实例
let db: Database.Database | null = null;
let dbPath: string;

/**
 * 初始化数据库
 */
export function initDatabase(): void {
  dbPath = process.env.DB_PATH || path.join(__dirname, '../../data/cninfo.db');
  const dataDir = path.dirname(dbPath);

  // 确保数据目录存在
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  // 创建或打开数据库（better-sqlite3 会自动处理文件）
  db = new Database(dbPath, {
    verbose: process.env.NODE_ENV === 'development' ? console.log : undefined
  });

  // 启用 WAL 模式以提高并发性能
  db.pragma('journal_mode = WAL');

  // 创建 api_metadata 表
  db.exec(`
    CREATE TABLE IF NOT EXISTS api_metadata (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      cn_name TEXT NOT NULL,
      source_url TEXT NOT NULL,
      request_url TEXT NOT NULL,
      request_method TEXT DEFAULT 'GET',
      params_config TEXT NOT NULL,
      output_config TEXT NOT NULL,
      table_name TEXT NOT NULL UNIQUE,
      last_update_time TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

