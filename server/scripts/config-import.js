const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

const serverDir = path.resolve(__dirname, '..');
const dbPath = process.env.DB_PATH || path.join(serverDir, 'data', 'cninfo.db');
const inputPath = process.argv[2]
  ? path.resolve(process.cwd(), process.argv[2])
  : path.join(serverDir, 'config', 'app-config.json');

const CONFIG_TABLES = [
  'api_metadata',
  'field_mapping',
  'data_combo_configs',
  'data_combos'
];

function quoteIdent(identifier) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(identifier)) {
    throw new Error(`Unsafe SQL identifier: ${identifier}`);
  }
  return `"${identifier}"`;
}

function ensureApiMetadataTable(db) {
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

  const existing = new Set(db.prepare('PRAGMA table_info(api_metadata)').all().map((column) => column.name));
  const requiredColumns = [
    ['request_url', 'TEXT'],
    ['params_config', 'TEXT'],
    ['sort_order', 'INTEGER DEFAULT 0'],
    ['update_mode', 'TEXT DEFAULT "full"'],
    ['date_field', 'TEXT'],
    ['date_range', 'INTEGER'],
    ['future_days', 'INTEGER'],
    ['last_api_fields', 'TEXT'],
    ['default_sort_field', 'TEXT'],
    ['default_sort_order', 'TEXT DEFAULT "desc"'],
    ['datacenter_config', 'TEXT'],
    ['fixed_begin_date', 'TEXT'],
    ['fields_verified', 'INTEGER DEFAULT 0'],
    ['row_bg_color', 'TEXT'],
    ['is_active', 'INTEGER DEFAULT 1']
  ];

  for (const [name, definition] of requiredColumns) {
    if (!existing.has(name)) {
      db.exec(`ALTER TABLE api_metadata ADD COLUMN ${quoteIdent(name)} ${definition}`);
    }
  }
}

function ensureFieldMappingTable(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS field_mapping (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      table_name TEXT NOT NULL,
      field_name TEXT NOT NULL,
      cn_name TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      sort_order INTEGER DEFAULT 0,
      hidden INTEGER DEFAULT 0,
      format_as_wan_yi INTEGER DEFAULT 0,
      value_mapping TEXT,
      format_as_timestamp INTEGER DEFAULT 0,
      unit TEXT,
      text_clamp INTEGER DEFAULT 0,
      UNIQUE(table_name, field_name)
    )
  `);

  const existing = new Set(db.prepare('PRAGMA table_info(field_mapping)').all().map((column) => column.name));
  const requiredColumns = [
    ['sort_order', 'INTEGER DEFAULT 0'],
    ['hidden', 'INTEGER DEFAULT 0'],
    ['format_as_wan_yi', 'INTEGER DEFAULT 0'],
    ['value_mapping', 'TEXT'],
    ['format_as_timestamp', 'INTEGER DEFAULT 0'],
    ['unit', 'TEXT'],
    ['text_clamp', 'INTEGER DEFAULT 0']
  ];

  for (const [name, definition] of requiredColumns) {
    if (!existing.has(name)) {
      db.exec(`ALTER TABLE field_mapping ADD COLUMN ${quoteIdent(name)} ${definition}`);
    }
  }
}

function ensureComboTables(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS data_combo_configs (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      definition_json TEXT NOT NULL,
      is_builtin INTEGER DEFAULT 0,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS data_combos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT,
      main_table TEXT NOT NULL,
      main_code_field TEXT NOT NULL,
      join_tables TEXT NOT NULL,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);
}

function ensureUpdateLogTables(db) {
  db.exec(`
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

  db.exec(`
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

function getTableColumns(db, tableName) {
  return db.prepare(`PRAGMA table_info(${quoteIdent(tableName)})`).all().map((column) => column.name);
}

function insertRows(db, tableName, rows) {
  if (!rows || rows.length === 0) {
    return;
  }

  const tableColumns = new Set(getTableColumns(db, tableName));
  const columns = Object.keys(rows[0]).filter((column) => tableColumns.has(column));
  if (columns.length === 0) {
    return;
  }

  const sql = `
    INSERT INTO ${quoteIdent(tableName)} (${columns.map(quoteIdent).join(', ')})
    VALUES (${columns.map(() => '?').join(', ')})
  `;
  const stmt = db.prepare(sql);

  for (const row of rows) {
    stmt.run(columns.map((column) => row[column] ?? null));
  }
}

function createBusinessTables(db) {
  const metadataRows = db.prepare('SELECT id, table_name, output_config FROM api_metadata').all();

  for (const metadata of metadataRows) {
    const tableName = metadata.table_name;
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(tableName)) {
      throw new Error(`Unsafe business table name: ${tableName}`);
    }

    let outputConfig;
    try {
      outputConfig = JSON.parse(metadata.output_config || '[]');
    } catch (error) {
      throw new Error(`Invalid output_config for ${tableName}: ${error.message}`);
    }

    if (!Array.isArray(outputConfig) || outputConfig.length === 0) {
      continue;
    }

    const columns = outputConfig.map((column) => {
      const name = column && column.name;
      const type = String((column && column.type) || 'TEXT').toUpperCase();
      const safeType = ['TEXT', 'INTEGER', 'REAL'].includes(type) ? type : 'TEXT';
      return `${quoteIdent(name)} ${safeType}`;
    });

    db.exec(`
      CREATE TABLE IF NOT EXISTS ${quoteIdent(tableName)} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metadata_id INTEGER NOT NULL,
        ${columns.join(',\n        ')},
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (metadata_id) REFERENCES api_metadata(id)
      )
    `);
  }
}

if (!fs.existsSync(inputPath)) {
  console.error(`[config:import] Config file not found: ${inputPath}`);
  process.exit(1);
}

const payload = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
if (!payload || payload.version !== 1 || !payload.tables) {
  console.error('[config:import] Unsupported config file format');
  process.exit(1);
}

fs.mkdirSync(path.dirname(dbPath), { recursive: true });
const db = new Database(dbPath);

const importConfig = db.transaction(() => {
  ensureApiMetadataTable(db);
  ensureFieldMappingTable(db);
  ensureComboTables(db);
  ensureUpdateLogTables(db);

  for (const tableName of [...CONFIG_TABLES].reverse()) {
    db.prepare(`DELETE FROM ${quoteIdent(tableName)}`).run();
  }

  for (const tableName of CONFIG_TABLES) {
    insertRows(db, tableName, payload.tables[tableName] || []);
  }

  createBusinessTables(db);
});

importConfig();
db.close();

console.log(`[config:import] Imported ${inputPath}`);
console.log(`[config:import] Database ready at ${dbPath}`);
for (const tableName of CONFIG_TABLES) {
  console.log(`[config:import] ${tableName}: ${(payload.tables[tableName] || []).length} rows`);
}
