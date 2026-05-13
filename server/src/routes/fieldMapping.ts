/**
 * 字段映射 API 路由
 * 处理原始字段名到中文名的映射
 * 
 * 【重要】field_mapping 表存储的数据非常重要！
 * 这是原始字段（f1, f2, ...f300）到中文名的对应关系。
 * 任何系统更新、数据清空操作都不应该影响这个表的数据。
 */
import { Router, Request, Response } from 'express';
import { getDatabase, saveDatabase } from '../services/database';

const router = Router();

let fieldMappingSchemaEnsured = false;

function ensureFieldMappingSchema(db: any): void {
  if (fieldMappingSchemaEnsured) {
    return;
  }

  const columns = db.prepare("PRAGMA table_info(field_mapping)").all() as Array<{ name: string }>;
  const columnNames = new Set(columns.map((col) => col.name));

  if (!columnNames.has('text_clamp')) {
    db.exec('ALTER TABLE field_mapping ADD COLUMN text_clamp INTEGER DEFAULT 0');
    console.log('[FieldMapping] 已添加列: field_mapping.text_clamp');
  }

  fieldMappingSchemaEnsured = true;
}

/**
 * GET /api/field-mapping/export/all - 导出所有字段映射
 * 【重要】这个路由必须放在 /:tableName 之前，否则会被错误匹配
 */
router.get('/export/all', async (_req: Request, res: Response) => {
  try {
    const db = getDatabase();
    ensureFieldMappingSchema(db);

    // 导出所有字段，包括没有中文名但有排序顺序的
    const mappings = db.prepare(`
      SELECT table_name, field_name, cn_name, sort_order, hidden, format_as_wan_yi, text_clamp, updated_at
      FROM field_mapping
      ORDER BY table_name, sort_order, field_name
    `).all() as Array<{
      table_name: string;
      field_name: string;
      cn_name: string | null;
      sort_order: number;
      hidden: number;
      format_as_wan_yi: number;
      text_clamp: number;
      updated_at: string;
    }>;

    // 按表名分组
    const grouped: Record<string, Record<string, { cn_name: string; sort_order: number; hidden: number; format_as_wan_yi: number; text_clamp: number }>> = {};
    let mappingCount = 0;  // 有中文名的数量
    let orderCount = 0;    // 有排序顺序的数量
    let hiddenCount = 0;   // 隐藏的数量
    let formatCount = 0;   // 启用万/亿格式化的数量
    let textClampCount = 0; // 启用长文本折叠的数量
    
    for (const item of mappings) {
      if (!grouped[item.table_name]) {
        grouped[item.table_name] = {};
      }
      grouped[item.table_name][item.field_name] = {
        cn_name: item.cn_name || '',
        sort_order: item.sort_order,
        hidden: item.hidden,
        format_as_wan_yi: item.format_as_wan_yi,
        text_clamp: item.text_clamp
      };
      if (item.cn_name) mappingCount++;
      if (item.sort_order > 0) orderCount++;
      if (item.hidden) hiddenCount++;
      if (item.format_as_wan_yi) formatCount++;
      if (item.text_clamp) textClampCount++;
    }
    
    res.json({
      success: true,
      data: {
        exportTime: new Date().toISOString(),
        version: '1.4',
        totalCount: mappings.length,
        mappingCount,  // 有中文名的数量
        orderCount,    // 有排序顺序的数量
        hiddenCount,   // 隐藏的数量
        formatCount,   // 启用万/亿格式化的数量
        textClampCount, // 启用长文本折叠的数量
        mappings: grouped
      }
    });
  } catch (error) {
    console.error('导出字段映射失败:', error);
    res.status(500).json({
      success: false,
      message: '导出字段映射失败',
      error: (error as Error).message
    });
  }
});

/**
 * POST /api/field-mapping/import/all - 导入字段映射
 * 【重要】这个路由必须放在 /:tableName 之前，否则会被错误匹配
 */
router.post('/import/all', async (req: Request, res: Response) => {
  try {
    const { mappings, mode = 'merge' } = req.body;
    
    if (!mappings || typeof mappings !== 'object') {
      res.status(400).json({
        success: false,
        message: '缺少 mappings 参数或格式不正确'
      });
      return;
    }
    
    const db = getDatabase();
    ensureFieldMappingSchema(db);
    let importedCount = 0;
    let skippedCount = 0;
    
    // 如果是覆盖模式，先清空所有映射
    if (mode === 'overwrite') {
      db.run('DELETE FROM field_mapping');
    }
    
    const sql = `
      INSERT INTO field_mapping (table_name, field_name, cn_name, sort_order, hidden, format_as_wan_yi, text_clamp, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
      ON CONFLICT(table_name, field_name) 
      DO UPDATE SET 
        cn_name = CASE WHEN excluded.cn_name != '' THEN excluded.cn_name ELSE field_mapping.cn_name END,
        sort_order = CASE WHEN excluded.sort_order > 0 THEN excluded.sort_order ELSE field_mapping.sort_order END,
        hidden = excluded.hidden,
        format_as_wan_yi = excluded.format_as_wan_yi,
        text_clamp = excluded.text_clamp,
        updated_at = datetime('now')
    `;
    
    for (const [tableName, fields] of Object.entries(mappings)) {
      if (typeof fields !== 'object') continue;
      
      for (const [fieldName, value] of Object.entries(fields as Record<string, any>)) {
        // 支持多种格式：
        // 1. { cn_name: '中文名', sort_order: 1, hidden: 0, format_as_wan_yi: 1 }
        // 2. { cn_name: '中文名', sort_order: 1 } (旧格式)
        // 3. '中文名' (简化格式)
        let cnName = '';
        let sortOrder = 0;
        let hidden = 0;
        let formatAsWanYi = 0;
        let textClamp = 0;
        
        if (typeof value === 'string') {
          cnName = value;
        } else if (typeof value === 'object' && value !== null) {
          cnName = value.cn_name || '';
          sortOrder = value.sort_order || 0;
          hidden = value.hidden || 0;
          formatAsWanYi = value.format_as_wan_yi || 0;
          textClamp = value.text_clamp || 0;
        }
        
        // 只要有中文名、排序顺序、隐藏状态或格式化设置，就导入
        if (cnName || sortOrder > 0 || hidden || formatAsWanYi || textClamp) {
          db.run(sql, [tableName, fieldName, cnName, sortOrder, hidden, formatAsWanYi, textClamp]);
          importedCount++;
        } else {
          skippedCount++;
        }
      }
    }
    
    saveDatabase();
    
    res.json({
      success: true,
      message: `导入完成：成功 ${importedCount} 条，跳过 ${skippedCount} 条`,
      data: {
        importedCount,
        skippedCount,
        mode
      }
    });
  } catch (error) {
    console.error('导入字段映射失败:', error);
    res.status(500).json({
      success: false,
      message: '导入字段映射失败',
      error: (error as Error).message
    });
  }
});

/**
 * DELETE /api/field-mapping/delete/all - 删除所有字段映射和排序
 * 【重要】这个路由必须放在 /:tableName 之前，否则会被错误匹配
 */
router.delete('/delete/all', async (_req: Request, res: Response) => {
  try {
    const db = getDatabase();

    // 统计要删除的数量
    const mappingResult = db.prepare('SELECT COUNT(*) as count FROM field_mapping WHERE cn_name IS NOT NULL AND cn_name != ""').get() as { count: number };
    const mappingCount = mappingResult.count;

    const orderResult = db.prepare('SELECT COUNT(*) as count FROM field_mapping WHERE sort_order > 0').get() as { count: number };
    const orderCount = orderResult.count;

    // 删除所有映射和排序（直接删除整个表的数据）
    db.run('DELETE FROM field_mapping');
    saveDatabase();
    
    res.json({
      success: true,
      message: `已删除 ${mappingCount} 条字段映射和 ${orderCount} 条排序设置`,
      data: {
        mappingCount,
        orderCount
      }
    });
  } catch (error) {
    console.error('删除字段映射失败:', error);
    res.status(500).json({
      success: false,
      message: '删除字段映射失败',
      error: (error as Error).message
    });
  }
});

/**
 * GET /api/field-mapping/:tableName/column-order - 获取列顺序
 * 【重要】这个路由必须放在 /:tableName 和 /:tableName/:fieldName 之前，否则会被错误匹配
 */
router.get('/:tableName/column-order', async (req: Request, res: Response) => {
  try {
    const { tableName } = req.params;
    const db = getDatabase();

    const order = db.prepare(`
      SELECT field_name, sort_order
      FROM field_mapping
      WHERE table_name = ? AND sort_order > 0
      ORDER BY sort_order ASC
    `).all(tableName) as Array<{ field_name: string; sort_order: number }>;

    res.json({
      success: true,
      data: order.map(row => row.field_name)
    });
  } catch (error) {
    console.error('获取列顺序失败:', error);
    res.status(500).json({
      success: false,
      message: '获取列顺序失败',
      error: (error as Error).message
    });
  }
});

/**
 * GET /api/field-mapping/:tableName/hidden-columns - 获取隐藏的列
 * 【重要】这个路由必须放在 /:tableName/:fieldName 之前，否则会被错误匹配
 */
router.get('/:tableName/hidden-columns', async (req: Request, res: Response) => {
  try {
    const { tableName } = req.params;
    const db = getDatabase();

    const hiddenColumns = db.prepare(`
      SELECT field_name
      FROM field_mapping
      WHERE table_name = ? AND hidden = 1
    `).all(tableName) as Array<{ field_name: string }>;

    res.json({
      success: true,
      data: hiddenColumns.map(row => row.field_name)
    });
  } catch (error) {
    console.error('获取隐藏列失败:', error);
    res.status(500).json({
      success: false,
      message: '获取隐藏列失败',
      error: (error as Error).message
    });
  }
});

/**
 * GET /api/field-mapping/:tableName/hidden-columns-detail - 获取隐藏列的详细信息
 * 【重要】这个路由必须放在 /:tableName/:fieldName 之前，否则会被错误匹配
 */
router.get('/:tableName/hidden-columns-detail', async (req: Request, res: Response) => {
  try {
    const { tableName } = req.params;
    const db = getDatabase();

    // 获取隐藏的列及其中文名
    const rows = db.prepare(`
      SELECT field_name, cn_name
      FROM field_mapping
      WHERE table_name = ? AND hidden = 1
      ORDER BY field_name
    `).all(tableName) as Array<{ field_name: string; cn_name: string }>;

    const hiddenColumns: Array<{
      fieldName: string;
      cnName: string;
      uniqueCount: number;
      topValues: Array<{ value: any; count: number }>;
    }> = [];

    for (const row of rows) {
      // 获取该字段的值分布
      let uniqueCount = 0;
      let topValues: Array<{ value: any; count: number }> = [];

      try {
        // 获取唯一值数量
        const uniqueResult = db.exec(`
          SELECT COUNT(DISTINCT ${row.field_name})
          FROM ${tableName}
          WHERE ${row.field_name} IS NOT NULL AND ${row.field_name} != ''
        `);
        uniqueCount = uniqueResult[0]?.values[0]?.[0] as number || 0;

        // 获取前3个最常见的值
        const valuesResult = db.exec(`
          SELECT ${row.field_name} as value, COUNT(*) as count
          FROM ${tableName}
          WHERE ${row.field_name} IS NOT NULL AND ${row.field_name} != ''
          GROUP BY ${row.field_name}
          ORDER BY count DESC
          LIMIT 3
        `);

        topValues = valuesResult[0]?.values.map((v: any) => ({
          value: v[0],
          count: v[1]
        })) || [];
      } catch (e) {
        // 字段可能不存在于表中，忽略错误
      }

      hiddenColumns.push({
        fieldName: row.field_name,
        cnName: row.cn_name || '',
        uniqueCount,
        topValues
      });
    }
    res.json({
      success: true,
      data: hiddenColumns
    });
  } catch (error) {
    console.error('获取隐藏列详情失败:', error);
    res.status(500).json({
      success: false,
      message: '获取隐藏列详情失败',
      error: (error as Error).message
    });
  }
});

/**
 * PUT /api/field-mapping/:tableName/hide-column - 设置列的隐藏状态
 * 【重要】这个路由必须放在 /:tableName/:fieldName 之前，否则会被错误匹配
 */
router.put('/:tableName/hide-column', async (req: Request, res: Response) => {
  try {
    const { tableName } = req.params;
    const { fieldName, hidden } = req.body;
    
    if (!fieldName) {
      res.status(400).json({
        success: false,
        message: '缺少 fieldName 参数'
      });
      return;
    }
    
    const db = getDatabase();
    
    // 使用 INSERT OR REPLACE 来插入或更新
    const sql = `
      INSERT INTO field_mapping (table_name, field_name, cn_name, hidden, updated_at)
      VALUES (?, ?, '', ?, datetime('now'))
      ON CONFLICT(table_name, field_name) 
      DO UPDATE SET hidden = excluded.hidden, updated_at = datetime('now')
    `;
    
    db.run(sql, [tableName, fieldName, hidden ? 1 : 0]);
    saveDatabase();
    
    res.json({
      success: true,
      message: hidden ? '已隐藏该列' : '已显示该列',
      data: {
        tableName,
        fieldName,
        hidden
      }
    });
  } catch (error) {
    console.error('设置列隐藏状态失败:', error);
    res.status(500).json({
      success: false,
      message: '设置列隐藏状态失败',
      error: (error as Error).message
    });
  }
});

/**
 * PUT /api/field-mapping/:tableName/column-order - 保存列顺序
 * 【重要】这个路由必须放在 /:tableName/:fieldName 之前，否则会被错误匹配
 */
router.put('/:tableName/column-order', async (req: Request, res: Response) => {
  try {
    const { tableName } = req.params;
    const { order } = req.body;
    
    if (!Array.isArray(order)) {
      res.status(400).json({
        success: false,
        message: '缺少 order 参数或格式不正确'
      });
      return;
    }
    
    const db = getDatabase();
    
    // 先将该表所有字段的 sort_order 重置为 0
    db.run('UPDATE field_mapping SET sort_order = 0 WHERE table_name = ?', [tableName]);
    
    // 更新每个字段的排序顺序
    const sql = `
      INSERT INTO field_mapping (table_name, field_name, cn_name, sort_order, updated_at)
      VALUES (?, ?, '', ?, datetime('now'))
      ON CONFLICT(table_name, field_name) 
      DO UPDATE SET sort_order = excluded.sort_order, updated_at = datetime('now')
    `;
    
    order.forEach((fieldName: string, index: number) => {
      db.run(sql, [tableName, fieldName, index + 1]);
    });
    
    saveDatabase();
    
    res.json({
      success: true,
      message: '列顺序保存成功',
      data: {
        tableName,
        count: order.length
      }
    });
  } catch (error) {
    console.error('保存列顺序失败:', error);
    res.status(500).json({
      success: false,
      message: '保存列顺序失败',
      error: (error as Error).message
    });
  }
});

/**
 * GET /api/field-mapping/:tableName - 获取指定表的所有字段映射
 */
router.get('/:tableName', async (req: Request, res: Response) => {
  try {
    const { tableName } = req.params;
    const db = getDatabase();
    ensureFieldMappingSchema(db);
    
    const stmt = db.prepare('SELECT field_name, cn_name, format_as_wan_yi, format_as_timestamp, value_mapping, unit, text_clamp FROM field_mapping WHERE table_name = ?');
    const rows = stmt.all(tableName) as Array<{ field_name: string; cn_name: string; format_as_wan_yi: number; format_as_timestamp: number; value_mapping: string | null; unit: string | null; text_clamp: number }>;

    const mappings: Record<string, string> = {};
    const formatSettings: Record<string, boolean> = {};
    const timestampSettings: Record<string, boolean> = {};
    const valueMappings: Record<string, Record<string, string>> = {};
    const unitSettings: Record<string, string> = {};
    const textClampSettings: Record<string, boolean> = {};
    for (const row of rows) {
      if (row.cn_name) {
        mappings[row.field_name] = row.cn_name;
      }
      if (row.format_as_wan_yi) {
        formatSettings[row.field_name] = true;
      }
      if (row.format_as_timestamp) {
        timestampSettings[row.field_name] = true;
      }
      // 解析值映射 JSON
      if (row.value_mapping) {
        try {
          valueMappings[row.field_name] = JSON.parse(row.value_mapping);
        } catch (e) {
          console.error(`解析值映射失败: ${row.field_name}`, e);
        }
      }
      // 单位设置
      if (row.unit) {
        unitSettings[row.field_name] = row.unit;
      }
      if (row.text_clamp) {
        textClampSettings[row.field_name] = true;
      }
    }

    res.json({
      success: true,
      data: mappings,
      formatSettings,
      timestampSettings,
      valueMappings,
      unitSettings,
      textClampSettings
    });
  } catch (error) {
    console.error('获取字段映射失败:', error);
    res.status(500).json({
      success: false,
      message: '获取字段映射失败',
      error: (error as Error).message
    });
  }
});

/**
 * PUT /api/field-mapping/:tableName/:fieldName - 更新单个字段的中文名、格式化设置、值映射和单位
 */
router.put('/:tableName/:fieldName', async (req: Request, res: Response) => {
  try {
    const { tableName, fieldName } = req.params;
    const { cnName, formatAsWanYi, formatAsTimestamp, valueMapping, unit, textClamp } = req.body;
    
    if (cnName === undefined && formatAsWanYi === undefined && formatAsTimestamp === undefined && valueMapping === undefined && unit === undefined && textClamp === undefined) {
      res.status(400).json({
        success: false,
        message: '缺少 cnName、formatAsWanYi、formatAsTimestamp、valueMapping、unit 或 textClamp 参数'
      });
      return;
    }
    
    const db = getDatabase();
    ensureFieldMappingSchema(db);
    
    // 使用 INSERT OR REPLACE 来插入或更新
    const sql = `
      INSERT INTO field_mapping (table_name, field_name, cn_name, format_as_wan_yi, format_as_timestamp, value_mapping, unit, text_clamp, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
      ON CONFLICT(table_name, field_name) 
      DO UPDATE SET 
        cn_name = CASE WHEN ? IS NOT NULL THEN ? ELSE field_mapping.cn_name END,
        format_as_wan_yi = CASE WHEN ? IS NOT NULL THEN ? ELSE field_mapping.format_as_wan_yi END,
        format_as_timestamp = CASE WHEN ? IS NOT NULL THEN ? ELSE field_mapping.format_as_timestamp END,
        value_mapping = CASE WHEN ? IS NOT NULL THEN ? ELSE field_mapping.value_mapping END,
        unit = CASE WHEN ? IS NOT NULL THEN ? ELSE field_mapping.unit END,
        text_clamp = CASE WHEN ? IS NOT NULL THEN ? ELSE field_mapping.text_clamp END,
        updated_at = datetime('now')
    `;
    
    const cnNameValue = cnName !== undefined ? (cnName || null) : null;
    const formatValue = formatAsWanYi !== undefined ? (formatAsWanYi ? 1 : 0) : null;
    const timestampValue = formatAsTimestamp !== undefined ? (formatAsTimestamp ? 1 : 0) : null;
    // 值映射：如果是空对象则存 null，否则存 JSON 字符串
    const valueMappingValue = valueMapping !== undefined 
      ? (valueMapping && Object.keys(valueMapping).length > 0 ? JSON.stringify(valueMapping) : null) 
      : null;
    const unitValue = unit !== undefined ? (unit || null) : null;
    const textClampValue = textClamp !== undefined ? (textClamp ? 1 : 0) : null;
    
    db.run(sql, [
      tableName, fieldName, cnNameValue, formatValue, timestampValue, valueMappingValue, unitValue, textClampValue,
      cnName !== undefined ? 1 : null, cnNameValue,
      formatAsWanYi !== undefined ? 1 : null, formatValue,
      formatAsTimestamp !== undefined ? 1 : null, timestampValue,
      valueMapping !== undefined ? 1 : null, valueMappingValue,
      unit !== undefined ? 1 : null, unitValue,
      textClamp !== undefined ? 1 : null, textClampValue
    ]);
    saveDatabase();
    
    res.json({
      success: true,
      message: '保存成功',
      data: {
        tableName,
        fieldName,
        cnName,
        formatAsWanYi,
        formatAsTimestamp,
        valueMapping,
        unit,
        textClamp
      }
    });
  } catch (error) {
    console.error('保存字段映射失败:', error);
    res.status(500).json({
      success: false,
      message: '保存字段映射失败',
      error: (error as Error).message
    });
  }
});

/**
 * POST /api/field-mapping/:tableName/batch - 批量更新字段映射
 */
router.post('/:tableName/batch', async (req: Request, res: Response) => {
  try {
    const { tableName } = req.params;
    const { mappings } = req.body;
    
    if (!mappings || typeof mappings !== 'object') {
      res.status(400).json({
        success: false,
        message: '缺少 mappings 参数'
      });
      return;
    }
    
    const db = getDatabase();
    
    const sql = `
      INSERT INTO field_mapping (table_name, field_name, cn_name, updated_at)
      VALUES (?, ?, ?, datetime('now'))
      ON CONFLICT(table_name, field_name) 
      DO UPDATE SET cn_name = excluded.cn_name, updated_at = datetime('now')
    `;
    
    for (const [fieldName, cnName] of Object.entries(mappings)) {
      db.run(sql, [tableName, fieldName, cnName || null]);
    }
    
    saveDatabase();
    
    res.json({
      success: true,
      message: '批量保存成功',
      data: {
        tableName,
        count: Object.keys(mappings).length
      }
    });
  } catch (error) {
    console.error('批量保存字段映射失败:', error);
    res.status(500).json({
      success: false,
      message: '批量保存字段映射失败',
      error: (error as Error).message
    });
  }
});

/**
 * DELETE /api/field-mapping/:tableName/:fieldName - 删除单个字段映射
 */
router.delete('/:tableName/:fieldName', async (req: Request, res: Response) => {
  try {
    const { tableName, fieldName } = req.params;
    const db = getDatabase();
    
    db.run('DELETE FROM field_mapping WHERE table_name = ? AND field_name = ?', [tableName, fieldName]);
    saveDatabase();
    
    res.json({
      success: true,
      message: '删除成功'
    });
  } catch (error) {
    console.error('删除字段映射失败:', error);
    res.status(500).json({
      success: false,
      message: '删除字段映射失败',
      error: (error as Error).message
    });
  }
});

export default router;
