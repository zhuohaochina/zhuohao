const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

const serverDir = path.resolve(__dirname, '..');
const dbPath = process.env.DB_PATH || path.join(serverDir, 'data', 'cninfo.db');
const outputPath = process.argv[2]
  ? path.resolve(process.cwd(), process.argv[2])
  : path.join(serverDir, 'config', 'app-config.json');

const CONFIG_TABLES = [
  'api_metadata',
  'field_mapping',
  'data_combo_configs',
  'data_combos'
];

function tableExists(db, tableName) {
  return Boolean(db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(tableName));
}

function getRows(db, tableName) {
  if (!tableExists(db, tableName)) {
    return [];
  }
  const rows = db.prepare(`SELECT * FROM ${tableName}`).all();
  if (tableName !== 'api_metadata') {
    return rows;
  }

  return rows.map((row) => {
    const { last_update_time, last_api_fields, ...configRow } = row;
    return configRow;
  });
}

if (!fs.existsSync(dbPath)) {
  console.error(`[config:export] Database not found: ${dbPath}`);
  process.exit(1);
}

const db = new Database(dbPath, { readonly: true });
const payload = {
  version: 1,
  exportedAt: new Date().toISOString(),
  description: 'Portable app configuration only. Runtime data tables are intentionally excluded.',
  tables: Object.fromEntries(CONFIG_TABLES.map((tableName) => [tableName, getRows(db, tableName)]))
};
db.close();

fs.mkdirSync(path.dirname(outputPath), { recursive: true });
fs.writeFileSync(outputPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');

console.log(`[config:export] Wrote ${outputPath}`);
for (const tableName of CONFIG_TABLES) {
  console.log(`[config:export] ${tableName}: ${payload.tables[tableName].length} rows`);
}
