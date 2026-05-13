import { getDatabase } from './database';
import type { OutputConfig } from '../models/types';

type SortOrder = 'asc' | 'desc';

type StoredComboRow = {
  id: string;
  name: string;
  description: string;
  definition_json: string;
  is_builtin: number;
  created_at: string;
  updated_at: string;
};

type InternalComboChildDefinition = {
  key: string;
  label: string;
  tableName: string;
  codeField: string;
  displayFields: string[];
  sortBy?: {
    field: string;
    order: SortOrder;
  };
};

type InternalComboDefinition = {
  id: string;
  name: string;
  description: string;
  mainTableName: string;
  mainCodeField: string;
  mainDisplayFields: string[];
  searchFields: string[];
  children: InternalComboChildDefinition[];
};

export interface DataComboColumn {
  field: string;
  label: string;
  description: string;
  type: string;
  display: {
    formatAsWanYi: boolean;
    formatAsTimestamp: boolean;
    valueMapping: Record<string, string>;
    unit: string;
    textClamp: boolean;
  };
}

export interface DataComboMetadataOption {
  metadataId: number;
  name: string;
  tableName: string;
  fields: DataComboColumn[];
  defaultDisplayFields: string[];
  recommendedCodeFields: string[];
}

export interface CreateDataComboChildInput {
  tableName: string;
  codeField: string;
  displayFields: string[];
  sortField?: string;
  sortOrder?: SortOrder;
}

export interface CreateDataComboInput {
  name: string;
  description?: string;
  mainTableName: string;
  mainCodeField: string;
  mainDisplayFields: string[];
  children: CreateDataComboChildInput[];
}

export interface DataComboSummary {
  id: string;
  name: string;
  description: string;
  mainTableName: string;
  mainTableLabel: string;
  isBuiltIn: boolean;
  children: Array<{ key: string; label: string }>;
}

export interface DataComboChildCollection {
  key: string;
  label: string;
  tableName: string;
  codeField: string;
  dataField: string;
  countField: string;
  columns: DataComboColumn[];
}

export interface DataComboDetail extends DataComboSummary {
  mainCodeField: string;
  mainColumns: DataComboColumn[];
  childCollections: DataComboChildCollection[];
}

export interface DataComboDataResult {
  combo: DataComboDetail;
  rows: Record<string, any>[];
  total: number;
  page: number;
  pageSize: number;
  totalPages: number;
  keyword: string;
}

export interface DataComboRowResult {
  combo: DataComboDetail;
  row: Record<string, any>;
}

type FieldMappingSettings = {
  labelMap: Map<string, string>;
  sortOrderMap: Map<string, number>;
  hiddenFields: Set<string>;
  formatFields: Set<string>;
  timestampFields: Set<string>;
  valueMappings: Map<string, Record<string, string>>;
  unitMap: Map<string, string>;
  textClampFields: Set<string>;
};

const SYSTEM_FIELDS = new Set(['id', 'metadata_id', 'created_at', 'collected_at']);
const COMBO_TABLE_NAME = 'data_combo_configs';


function escapeIdentifier(identifier: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(identifier)) {
    throw new Error(`Invalid identifier: ${identifier}`);
  }

  return `"${identifier}"`;
}

function normalizeJoinValue(value: unknown): string {
  if (value === null || value === undefined) {
    return '';
  }

  return String(value).trim();
}

function uniqueFields(fields: string[]): string[] {
  return Array.from(new Set(fields.filter(Boolean)));
}

function sanitizeKey(input: string): string {
  return input
    .replace(/[^A-Za-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_');
}

export class DataComboService {
  listCombos(): DataComboSummary[] {
    this.ensureStore();

    return this.getStoredCombos().map((item) => this.toSummary(item.definition, item.isBuiltIn));
  }

  getComboById(id: string): DataComboDetail {
    this.ensureStore();

    const combo = this.getStoredCombo(id);
    return this.toDetail(combo.definition, combo.isBuiltIn);
  }

  getComboData(
    id: string,
    page: number = 1,
    pageSize: number = 20,
    keyword: string = '',
    sortField: string = '',
    sortOrder: string = ''
  ): DataComboDataResult {
    this.ensureStore();

    const comboRecord = this.getStoredCombo(id);
    const definition = comboRecord.definition;
    const combo = this.toDetail(definition, comboRecord.isBuiltIn);
    const db = getDatabase();
    const resolvedMainDisplayFields = this.resolveMainDisplayFields(definition);

    const safePage = Number.isFinite(page) && page > 0 ? Math.floor(page) : 1;
    const safePageSize = Number.isFinite(pageSize) && pageSize > 0 ? Math.min(Math.floor(pageSize), 100) : 20;
    const trimmedKeyword = keyword.trim();
    const normalizedSortField = sortField.trim();
    const normalizedSortOrder: SortOrder = sortOrder === 'desc' ? 'desc' : 'asc';
    const searchFilter = this.buildSearchFilter(definition, trimmedKeyword);
    const whereClause = searchFilter.sql ? `WHERE ${searchFilter.sql}` : '';
    const orderClause = this.buildOrderClause(definition, normalizedSortField, normalizedSortOrder);
    const totalRow = db.prepare(`
      SELECT COUNT(*) as total
      FROM ${escapeIdentifier(definition.mainTableName)} main
      ${whereClause}
    `).get(...searchFilter.params) as { total?: number } | undefined;
    const total = totalRow?.total || 0;

    const selectedFields = uniqueFields([
      definition.mainCodeField,
      ...resolvedMainDisplayFields
    ]);

    const offset = (safePage - 1) * safePageSize;
    const mainRows = db.prepare(`
      SELECT ${selectedFields.map((field) => escapeIdentifier(field)).join(', ')}
      FROM ${escapeIdentifier(definition.mainTableName)} main
      ${whereClause}
      ORDER BY ${orderClause}
      LIMIT ? OFFSET ?
    `).all(...searchFilter.params, safePageSize, offset) as Record<string, any>[];

    const codes = Array.from(new Set(mainRows.map((row) => normalizeJoinValue(row[definition.mainCodeField])).filter(Boolean)));
    const childGroups = new Map<string, Map<string, Record<string, any>[]>>();

    for (const child of definition.children) {
      childGroups.set(child.key, this.queryChildGroups(child, codes));
    }

    const rows = mainRows.map((mainRow) => {
      const code = normalizeJoinValue(mainRow[definition.mainCodeField]);
      const row: Record<string, any> = {
        [definition.mainCodeField]: mainRow[definition.mainCodeField]
      };

      for (const field of resolvedMainDisplayFields) {
        row[field] = mainRow[field];
      }

      for (const child of definition.children) {
        const records = childGroups.get(child.key)?.get(code) || [];
        row[child.key] = records;
        row[`${child.key}Count`] = records.length;
      }

      return row;
    });

    return {
      combo,
      rows,
      total,
      page: safePage,
      pageSize: safePageSize,
      totalPages: total === 0 ? 0 : Math.ceil(total / safePageSize),
      keyword: trimmedKeyword
    };
  }

  getComboRow(id: string, mainCode: string): DataComboRowResult {
    this.ensureStore();

    const comboRecord = this.getStoredCombo(id);
    const definition = comboRecord.definition;
    const combo = this.toRowDetail(definition, comboRecord.isBuiltIn);
    const db = getDatabase();
    const normalizedCode = mainCode.trim();
    const detailMainFields = this.resolveDisplayFields(
      definition.mainTableName,
      this.getAvailableFields(definition.mainTableName)
    );

    if (!normalizedCode) {
      throw new Error('Main row code is required');
    }

    const selectedFields = uniqueFields([
      definition.mainCodeField,
      ...detailMainFields
    ]);

    const mainRow = db.prepare(`
      SELECT ${selectedFields.map((field) => escapeIdentifier(field)).join(', ')}
      FROM ${escapeIdentifier(definition.mainTableName)} main
      WHERE CAST(main.${escapeIdentifier(definition.mainCodeField)} AS TEXT) = ?
      LIMIT 1
    `).get(normalizedCode) as Record<string, any> | undefined;

    if (!mainRow) {
      throw new Error('Main row not found');
    }

    const rowCode = normalizeJoinValue(mainRow[definition.mainCodeField]);
    const childGroups = new Map<string, Map<string, Record<string, any>[]>>();
    for (const child of definition.children) {
      childGroups.set(
        child.key,
        this.queryChildGroups(
          child,
          [rowCode],
          this.resolveDisplayFields(child.tableName, this.getAvailableFields(child.tableName))
        )
      );
    }

    const row: Record<string, any> = {
      [definition.mainCodeField]: mainRow[definition.mainCodeField]
    };

    for (const field of detailMainFields) {
      row[field] = mainRow[field];
    }

    for (const child of definition.children) {
      const records = childGroups.get(child.key)?.get(rowCode) || [];
      row[child.key] = records;
      row[`${child.key}Count`] = records.length;
    }

    return { combo, row };
  }

  listMetadataOptions(): DataComboMetadataOption[] {
    this.ensureStore();

    const db = getDatabase();
    const metadataRows = db.prepare(`
      SELECT id, cn_name, table_name
      FROM api_metadata
      ORDER BY sort_order ASC, id ASC
    `).all() as Array<{ id: number; cn_name: string; table_name: string }>;

    return metadataRows
      .filter((row) => this.tableExists(row.table_name))
      .map((row) => {
        const allFields = this.getAvailableFields(row.table_name);
        const fields = this.getColumnMeta(row.table_name, allFields);

        return {
          metadataId: row.id,
          name: row.cn_name,
          tableName: row.table_name,
          fields,
          defaultDisplayFields: this.resolveDisplayFields(row.table_name, allFields),
          recommendedCodeFields: this.getRecommendedCodeFields(fields.map((field) => field.field))
        };
      });
  }

  createCombo(input: CreateDataComboInput): DataComboDetail {
    this.ensureStore();

    const normalizedDefinition = this.normalizeInput(input);
    const db = getDatabase();
    const now = new Date().toISOString();

    db.prepare(`
      INSERT INTO ${COMBO_TABLE_NAME} (id, name, description, definition_json, is_builtin, created_at, updated_at)
      VALUES (?, ?, ?, ?, 0, ?, ?)
    `).run(
      normalizedDefinition.id,
      normalizedDefinition.name,
      normalizedDefinition.description,
      JSON.stringify(normalizedDefinition),
      now,
      now
    );

    return this.toDetail(normalizedDefinition, false);
  }

  deleteCombo(id: string): void {
    this.ensureStore();

    this.getStoredCombo(id);
    getDatabase().prepare(`
      DELETE FROM ${COMBO_TABLE_NAME}
      WHERE id = ?
    `).run(id);
  }

  private ensureStore(): void {
    const db = getDatabase();

    db.exec(`
      CREATE TABLE IF NOT EXISTS ${COMBO_TABLE_NAME} (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT DEFAULT '',
        definition_json TEXT NOT NULL,
        is_builtin INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);



    db.prepare(`
      UPDATE ${COMBO_TABLE_NAME}
      SET is_builtin = 0
      WHERE is_builtin <> 0
    `).run();
  }

  private getStoredCombos(): Array<{ definition: InternalComboDefinition; isBuiltIn: boolean }> {
    const db = getDatabase();
    const rows = db.prepare(`
      SELECT id, name, description, definition_json, is_builtin
      FROM ${COMBO_TABLE_NAME}
      ORDER BY is_builtin DESC, created_at ASC, id ASC
    `).all() as StoredComboRow[];

    return rows.map((row) => ({
      definition: this.parseStoredDefinition(row),
      isBuiltIn: row.is_builtin === 1
    }));
  }

  private getStoredCombo(id: string): { definition: InternalComboDefinition; isBuiltIn: boolean } {
    const row = getDatabase().prepare(`
      SELECT id, name, description, definition_json, is_builtin
      FROM ${COMBO_TABLE_NAME}
      WHERE id = ?
      LIMIT 1
    `).get(id) as StoredComboRow | undefined;

    if (!row) {
      throw new Error('Data combo not found');
    }

    return {
      definition: this.parseStoredDefinition(row),
      isBuiltIn: row.is_builtin === 1
    };
  }

  private parseStoredDefinition(row: StoredComboRow): InternalComboDefinition {
    const parsed = JSON.parse(row.definition_json) as InternalComboDefinition;
    return {
      ...parsed,
      id: row.id,
      name: row.name,
      description: row.description || parsed.description || '',
      mainDisplayFields: uniqueFields(parsed.mainDisplayFields || []),
      searchFields: uniqueFields(parsed.searchFields || []),
      children: (parsed.children || []).map((child) => ({
        ...child,
        displayFields: uniqueFields(child.displayFields || [])
      }))
    };
  }

  private normalizeInput(input: CreateDataComboInput): InternalComboDefinition {
    const name = String(input.name || '').trim();
    if (!name) {
      throw new Error('Combo name is required');
    }

    const description = String(input.description || '').trim();
    const mainTableName = String(input.mainTableName || '').trim();
    const mainCodeField = String(input.mainCodeField || '').trim();
    const mainFields = this.requireTableFields(mainTableName);

    if (!mainFields.includes(mainCodeField)) {
      throw new Error('Main code field is invalid');
    }

    const mainDisplayFields = uniqueFields(input.mainDisplayFields || []).filter((field) => mainFields.includes(field));
    if (mainDisplayFields.length === 0) {
      throw new Error('Select at least one main display field');
    }

    const childrenInput = Array.isArray(input.children) ? input.children : [];
    if (childrenInput.length === 0) {
      throw new Error('Configure at least one child table');
    }

    const usedChildKeys = new Set<string>();
    const children = childrenInput.map((child, index) => {
      const tableName = String(child.tableName || '').trim();
      const childLabel = this.getTableDisplayName(tableName);
      const codeField = String(child.codeField || '').trim();
      const childFields = this.requireTableFields(tableName);
      if (!childFields.includes(codeField)) {
        throw new Error(`Child table ${childLabel} has an invalid code field`);
      }

      const displayFields = uniqueFields(child.displayFields || []).filter((field) => childFields.includes(field));
      if (displayFields.length === 0) {
        throw new Error(`Child table ${childLabel} must include at least one display field`);
      }

      const sortField = child.sortField && childFields.includes(child.sortField) ? child.sortField : undefined;
      const sortOrder: SortOrder = child.sortOrder === 'asc' ? 'asc' : 'desc';
      const key = this.generateUniqueChildKey(childLabel, tableName, usedChildKeys, index);

      return {
        key,
        label: childLabel,
        tableName,
        codeField,
        displayFields,
        sortBy: sortField
          ? {
              field: sortField,
              order: sortOrder
            }
          : undefined
      };
    });

    return {
      id: this.generateComboId(),
      name,
      description,
      mainTableName,
      mainCodeField,
      mainDisplayFields,
      searchFields: uniqueFields([mainCodeField, ...mainDisplayFields]),
      children
    };
  }

  private toSummary(definition: InternalComboDefinition, isBuiltIn: boolean): DataComboSummary {
    return {
      id: definition.id,
      name: definition.name,
      description: definition.description,
      mainTableName: definition.mainTableName,
      mainTableLabel: this.getTableDisplayName(definition.mainTableName),
      isBuiltIn,
      children: definition.children.map((child) => ({
        key: child.key,
        label: child.label
      }))
    };
  }

  private toDetail(definition: InternalComboDefinition, isBuiltIn: boolean): DataComboDetail {
    const resolvedMainDisplayFields = this.resolveMainDisplayFields(definition);

    return {
      ...this.toSummary(definition, isBuiltIn),
      mainCodeField: definition.mainCodeField,
      mainColumns: this.getColumnMeta(definition.mainTableName, resolvedMainDisplayFields),
      childCollections: definition.children.map((child) => ({
        key: child.key,
        label: child.label,
        tableName: child.tableName,
        codeField: child.codeField,
        dataField: child.key,
        countField: `${child.key}Count`,
        columns: this.getColumnMeta(child.tableName, this.resolveChildDisplayFields(child))
      }))
    };
  }

  private toRowDetail(definition: InternalComboDefinition, isBuiltIn: boolean): DataComboDetail {
    const mainFields = this.resolveDisplayFields(
      definition.mainTableName,
      this.getAvailableFields(definition.mainTableName)
    );

    return {
      ...this.toSummary(definition, isBuiltIn),
      mainCodeField: definition.mainCodeField,
      mainColumns: this.getColumnMeta(definition.mainTableName, mainFields),
      childCollections: definition.children.map((child) => ({
        key: child.key,
        label: child.label,
        tableName: child.tableName,
        codeField: child.codeField,
        dataField: child.key,
        countField: `${child.key}Count`,
        columns: this.getColumnMeta(
          child.tableName,
          this.resolveDisplayFields(child.tableName, this.getAvailableFields(child.tableName))
        )
      }))
    };
  }

  private queryChildGroups(
    child: InternalComboChildDefinition,
    codes: string[],
    fieldsOverride?: string[]
  ): Map<string, Record<string, any>[]> {
    const groupedRows = new Map<string, Record<string, any>[]>();

    if (codes.length === 0) {
      return groupedRows;
    }

    const db = getDatabase();
    const resolvedChildDisplayFields = fieldsOverride
      ? this.resolveDisplayFields(child.tableName, fieldsOverride)
      : this.resolveChildDisplayFields(child);
    const selectedFields = uniqueFields([child.codeField, ...resolvedChildDisplayFields]);
    const placeholders = codes.map(() => '?').join(', ');
    const orderParts = [escapeIdentifier(child.codeField)];

    if (child.sortBy) {
      orderParts.push(`${escapeIdentifier(child.sortBy.field)} ${child.sortBy.order.toUpperCase()}`);
    }

    const rows = db.prepare(`
      SELECT ${selectedFields.map((field) => escapeIdentifier(field)).join(', ')}
      FROM ${escapeIdentifier(child.tableName)}
      WHERE ${escapeIdentifier(child.codeField)} IN (${placeholders})
      ORDER BY ${orderParts.join(', ')}
    `).all(...codes) as Record<string, any>[];

    for (const row of rows) {
      const code = normalizeJoinValue(row[child.codeField]);
      if (!code) {
        continue;
      }

      if (!groupedRows.has(code)) {
        groupedRows.set(code, []);
      }

      const childRow: Record<string, any> = {};
      for (const field of resolvedChildDisplayFields) {
        childRow[field] = row[field];
      }

      groupedRows.get(code)!.push(childRow);
    }

    return groupedRows;
  }

  private buildSearchFilter(
    definition: InternalComboDefinition,
    keyword: string
  ): { sql: string; params: any[] } {
    if (!keyword) {
      return { sql: '', params: [] };
    }

    const likeValue = `%${keyword}%`;
    const params: string[] = [];
    const mainFields = uniqueFields([definition.mainCodeField, ...this.resolveMainDisplayFields(definition)]);
    const mainConditions = mainFields.map((field) => {
      params.push(likeValue);
      return `CAST(main.${escapeIdentifier(field)} AS TEXT) LIKE ?`;
    });

    const childExistsConditions = definition.children.map((child, index) => {
      const alias = `child_${index + 1}`;
      const fieldConditions = this.resolveChildDisplayFields(child).map((field) => {
        params.push(likeValue);
        return `CAST(${alias}.${escapeIdentifier(field)} AS TEXT) LIKE ?`;
      });

      if (fieldConditions.length === 0) {
        return '';
      }

      return `
        EXISTS (
          SELECT 1
          FROM ${escapeIdentifier(child.tableName)} ${alias}
          WHERE ${alias}.${escapeIdentifier(child.codeField)} = main.${escapeIdentifier(definition.mainCodeField)}
            AND (${fieldConditions.join(' OR ')})
        )
      `;
    }).filter(Boolean);

    const conditions = [...mainConditions, ...childExistsConditions];
    return {
      sql: conditions.length > 0 ? `(${conditions.join(' OR ')})` : '',
      params
    };
  }

  private buildOrderClause(
    definition: InternalComboDefinition,
    sortField: string,
    sortOrder: SortOrder
  ): string {
    const mainFields = new Set(uniqueFields([
      definition.mainCodeField,
      ...this.resolveMainDisplayFields(definition)
    ]));

    if (mainFields.has(sortField)) {
      const mainFieldRef = `main.${escapeIdentifier(sortField)}`;
      if (this.shouldUseNumericMainSort(definition.mainTableName, sortField)) {
        return `${this.buildNumericNullCaseExpr(mainFieldRef)}, ${this.buildNumericSortExpr(mainFieldRef)} ${sortOrder.toUpperCase()}, ${mainFieldRef} ${sortOrder.toUpperCase()}, main.${escapeIdentifier(definition.mainCodeField)} ASC`;
      }

      return `${mainFieldRef} ${sortOrder.toUpperCase()}, main.${escapeIdentifier(definition.mainCodeField)} ASC`;
    }

    const child = definition.children.find((item) => `${item.key}Count` === sortField);
    if (child) {
      return `(
        SELECT COUNT(*)
        FROM ${escapeIdentifier(child.tableName)} child_sort
        WHERE CAST(child_sort.${escapeIdentifier(child.codeField)} AS TEXT) = CAST(main.${escapeIdentifier(definition.mainCodeField)} AS TEXT)
      ) ${sortOrder.toUpperCase()}, main.${escapeIdentifier(definition.mainCodeField)} ASC`;
    }

    return `main.${escapeIdentifier(definition.mainCodeField)} ASC`;
  }

  private buildNumericNullCaseExpr(fieldRef: string): string {
    return `CASE WHEN ${fieldRef} IS NULL OR TRIM(CAST(${fieldRef} AS TEXT)) = '' OR TRIM(CAST(${fieldRef} AS TEXT)) = '-' THEN 1 ELSE 0 END`;
  }

  private buildNumericSortExpr(fieldRef: string): string {
    return `CAST(NULLIF(NULLIF(TRIM(CAST(${fieldRef} AS TEXT)), ''), '-') AS REAL)`;
  }

  private shouldUseNumericMainSort(tableName: string, fieldName: string): boolean {
    if (tableName === 'eastmoney_stock_raw') {
      return !['f12', 'f14', 'f13', 'f1'].includes(fieldName);
    }

    const outputType = this.getOutputConfigMap(tableName).get(fieldName)?.type || '';
    const tableType = this.getTableTypeMap(tableName).get(fieldName) || '';
    return /INT|REAL|NUM|DEC|FLOAT|DOUBLE/i.test(outputType || tableType);
  }

  private getColumnMeta(tableName: string, fields: string[]): DataComboColumn[] {
    const tableFields = new Set(this.getAvailableFields(tableName));
    const outputConfigMap = this.getOutputConfigMap(tableName);
    const fieldMappingSettings = this.getFieldMappingSettings(tableName);
    const fieldMappingMap = fieldMappingSettings.labelMap;
    const tableTypeMap = this.getTableTypeMap(tableName);

    return uniqueFields(fields)
      .filter((field) => tableFields.has(field) && !SYSTEM_FIELDS.has(field))
      .map((field) => {
        const outputConfig = outputConfigMap.get(field);
        const description = outputConfig?.description || field;

        return {
          field,
          label: fieldMappingMap.get(field) || description,
          description,
          type: outputConfig?.type || tableTypeMap.get(field) || 'TEXT',
          display: {
            formatAsWanYi: fieldMappingSettings.formatFields.has(field),
            formatAsTimestamp: fieldMappingSettings.timestampFields.has(field),
            valueMapping: fieldMappingSettings.valueMappings.get(field) || {},
            unit: fieldMappingSettings.unitMap.get(field) || '',
            textClamp: fieldMappingSettings.textClampFields.has(field)
          }
        };
      });
  }

  private resolveMainDisplayFields(definition: InternalComboDefinition): string[] {
    return this.resolveDisplayFields(definition.mainTableName, definition.mainDisplayFields);
  }

  private resolveChildDisplayFields(child: InternalComboChildDefinition): string[] {
    return this.resolveDisplayFields(child.tableName, child.displayFields);
  }

  private resolveDisplayFields(tableName: string, configuredFields: string[]): string[] {
    const availableFields = new Set(this.getAvailableFields(tableName));
    const selectedFields = uniqueFields(configuredFields)
      .filter((field) => availableFields.has(field) && !SYSTEM_FIELDS.has(field));
    const fieldMappingSettings = this.getFieldMappingSettings(tableName);
    const visibleFields = selectedFields.filter((field) => !fieldMappingSettings.hiddenFields.has(field));
    const originalIndexMap = new Map(visibleFields.map((field, index) => [field, index]));

    return [...visibleFields].sort((left, right) => {
      const leftOrder = fieldMappingSettings.sortOrderMap.get(left) ?? Number.MAX_SAFE_INTEGER;
      const rightOrder = fieldMappingSettings.sortOrderMap.get(right) ?? Number.MAX_SAFE_INTEGER;

      if (leftOrder !== rightOrder) {
        return leftOrder - rightOrder;
      }

      return (originalIndexMap.get(left) ?? 0) - (originalIndexMap.get(right) ?? 0);
    });
  }

  private getAvailableFields(tableName: string): string[] {
    return this.getTableColumns(tableName)
      .filter((field) => !SYSTEM_FIELDS.has(field));
  }

  private getTableColumns(tableName: string): string[] {
    const rows = getDatabase().prepare(`
      PRAGMA table_info(${escapeIdentifier(tableName)})
    `).all() as Array<{ name: string }>;

    return rows.map((row) => row.name);
  }

  private getTableTypeMap(tableName: string): Map<string, string> {
    const rows = getDatabase().prepare(`
      PRAGMA table_info(${escapeIdentifier(tableName)})
    `).all() as Array<{ name: string; type: string }>;

    return new Map(rows.map((row) => [row.name, row.type || 'TEXT']));
  }

  private getOutputConfigMap(tableName: string): Map<string, OutputConfig> {
    const row = getDatabase().prepare(`
      SELECT output_config
      FROM api_metadata
      WHERE table_name = ?
      LIMIT 1
    `).get(tableName) as { output_config?: string } | undefined;

    if (!row?.output_config) {
      return new Map();
    }

    try {
      const outputConfig = JSON.parse(row.output_config) as OutputConfig[];
      return new Map(outputConfig.map((item) => [item.name, item]));
    } catch {
      return new Map();
    }
  }

  private getFieldMappingSettings(tableName: string): FieldMappingSettings {
    try {
      const rows = getDatabase().prepare(`
        SELECT field_name, cn_name, sort_order, hidden, format_as_wan_yi, format_as_timestamp, value_mapping, unit, text_clamp
        FROM field_mapping
        WHERE table_name = ?
      `).all(tableName) as Array<{
        field_name: string;
        cn_name: string | null;
        sort_order: number | null;
        hidden: number | null;
        format_as_wan_yi: number | null;
        format_as_timestamp: number | null;
        value_mapping: string | null;
        unit: string | null;
        text_clamp: number | null;
      }>;

      const valueMappings = new Map<string, Record<string, string>>();
      for (const row of rows) {
        if (!row.value_mapping) {
          continue;
        }

        try {
          const parsed = JSON.parse(row.value_mapping) as Record<string, string>;
          valueMappings.set(row.field_name, parsed);
        } catch {
          // Ignore malformed persisted value mappings and fall back to raw values.
        }
      }

      return {
        labelMap: new Map(
          rows
            .filter((row) => row.cn_name && row.cn_name.trim())
            .map((row) => [row.field_name, row.cn_name!.trim()])
        ),
        sortOrderMap: new Map(
          rows
            .filter((row) => Number(row.sort_order) > 0)
            .map((row) => [row.field_name, Number(row.sort_order)])
        ),
        hiddenFields: new Set(
          rows
            .filter((row) => Number(row.hidden) === 1)
            .map((row) => row.field_name)
        ),
        formatFields: new Set(
          rows
            .filter((row) => Number(row.format_as_wan_yi) === 1)
            .map((row) => row.field_name)
        ),
        timestampFields: new Set(
          rows
            .filter((row) => Number(row.format_as_timestamp) === 1)
            .map((row) => row.field_name)
        ),
        valueMappings,
        unitMap: new Map(
          rows
            .filter((row) => Boolean(row.unit && row.unit.trim()))
            .map((row) => [row.field_name, row.unit!.trim()])
        ),
        textClampFields: new Set(
          rows
            .filter((row) => Number(row.text_clamp) === 1)
            .map((row) => row.field_name)
        )
      };
    } catch {
      try {
        const rows = getDatabase().prepare(`
          SELECT field_name, cn_name, sort_order, hidden
          FROM field_mapping
          WHERE table_name = ?
        `).all(tableName) as Array<{
          field_name: string;
          cn_name: string | null;
          sort_order: number | null;
          hidden: number | null;
        }>;

        return {
          labelMap: new Map(
            rows
              .filter((row) => row.cn_name && row.cn_name.trim())
              .map((row) => [row.field_name, row.cn_name!.trim()])
          ),
          sortOrderMap: new Map(
            rows
              .filter((row) => Number(row.sort_order) > 0)
              .map((row) => [row.field_name, Number(row.sort_order)])
          ),
          hiddenFields: new Set(
            rows
              .filter((row) => Number(row.hidden) === 1)
              .map((row) => row.field_name)
          ),
          formatFields: new Set(),
          timestampFields: new Set(),
          valueMappings: new Map(),
          unitMap: new Map(),
          textClampFields: new Set()
        };
      } catch {
        return {
          labelMap: new Map(),
          sortOrderMap: new Map(),
          hiddenFields: new Set(),
          formatFields: new Set(),
          timestampFields: new Set(),
          valueMappings: new Map(),
          unitMap: new Map(),
          textClampFields: new Set()
        };
      }
    }
  }

  private tableExists(tableName: string): boolean {
    const row = getDatabase().prepare(`
      SELECT name
      FROM sqlite_master
      WHERE type = 'table' AND name = ?
      LIMIT 1
    `).get(tableName);

    return Boolean(row);
  }

  private requireTableFields(tableName: string): string[] {
    if (!tableName || !this.tableExists(tableName)) {
      throw new Error(`琛ㄤ笉瀛樺湪: ${tableName}`);
    }

    return this.getAvailableFields(tableName);
  }

  private getTableDisplayName(tableName: string): string {
    const row = getDatabase().prepare(`
      SELECT cn_name
      FROM api_metadata
      WHERE table_name = ?
      LIMIT 1
    `).get(tableName) as { cn_name?: string } | undefined;

    return row?.cn_name?.trim() || tableName;
  }

  private getRecommendedCodeFields(fields: string[]): string[] {
    const exactMatches = ['f12', 'code', 'scode', 'security_code', 'stock_code', 'symbol'];

    const exact = exactMatches.filter((field) => fields.includes(field));
    const fuzzy = fields.filter((field) => /(^|_)(code|symbol)$|^f12$/i.test(field) && !exact.includes(field));
    return uniqueFields([...exact, ...fuzzy]);
  }

  private generateComboId(): string {
    const db = getDatabase();

    while (true) {
      const id = `combo_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
      const existing = db.prepare(`
        SELECT id
        FROM ${COMBO_TABLE_NAME}
        WHERE id = ?
        LIMIT 1
      `).get(id);

      if (!existing) {
        return id;
      }
    }
  }

  private generateUniqueChildKey(
    label: string,
    tableName: string,
    usedKeys: Set<string>,
    index: number
  ): string {
    const seed = sanitizeKey(label) || sanitizeKey(tableName) || `child_${index + 1}`;
    let key = seed;
    let counter = 2;

    while (!key || usedKeys.has(key)) {
      key = `${seed || 'child'}_${counter}`;
      counter += 1;
    }

    usedKeys.add(key);
    return key;
  }
}

export const dataComboService = new DataComboService();
