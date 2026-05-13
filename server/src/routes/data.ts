/**
 * 业务数据 API 路由
 * 处理业务数据的查询操作
 */
import { Router, Request, Response } from 'express';
import axios from 'axios';
import { metadataService } from '../services/metadataService';
import { tableService } from '../services/tableService';
import { OutputConfig } from '../models/types';
import { getDatabase } from '../services/database';

const router = Router();

type DataQueryParams = {
  keyword: string;
  sortField: string;
  sortOrder: 'asc' | 'desc' | '';
  searchColumn: string;
  exactMatch: boolean;
};

type ExportColumn = {
  name: string;
  header: string;
  type: string;
  description: string;
  cnName: string;
};

function parseDataQueryParams(req: Request): DataQueryParams {
  return {
    keyword: req.query.keyword as string || '',
    sortField: req.query.sortField as string || '',
    sortOrder: req.query.sortOrder as 'asc' | 'desc' | '' || '',
    searchColumn: req.query.searchColumn as string || '',
    exactMatch: req.query.exactMatch === 'true'
  };
}

function getTableFields(tableName: string, outputConfig: OutputConfig[]): Array<{ name: string; type: string; description: string }> {
  const db = getDatabase();
  const outputConfigMap = new Map(outputConfig.map(c => [c.name, c]));
  const result = db.exec(`PRAGMA table_info(${tableName})`);

  if (result.length === 0) {
    return outputConfig.map(config => ({
      name: config.name,
      type: config.type || 'TEXT',
      description: config.description || config.name
    }));
  }

  return result[0].values
    .filter((row: any) => !['id', 'metadata_id', 'created_at', 'collected_at'].includes(row[1] as string))
    .map((row: any) => {
      const fieldName = row[1] as string;
      const fieldType = row[2] as string;
      const config = outputConfigMap.get(fieldName);
      return {
        name: fieldName,
        type: fieldType || 'TEXT',
        description: config?.description || fieldName
      };
    });
}

function getExportColumns(
  tableName: string,
  outputConfig: OutputConfig[],
  visibleOnly: boolean
): ExportColumn[] {
  const tableFields = getTableFields(tableName, outputConfig);
  const baseColumns = tableFields.map(field => ({
    name: field.name,
    header: field.name,
    type: field.type,
    description: field.description,
    cnName: ''
  }));

  if (!visibleOnly) {
    return baseColumns;
  }

  const db = getDatabase();
  let mappingRows: Array<{ field_name: string; cn_name: string | null; sort_order: number | null; hidden: number | null }> = [];

  try {
    mappingRows = db.prepare(`
      SELECT field_name, cn_name, sort_order, hidden
      FROM field_mapping
      WHERE table_name = ?
    `).all(tableName) as Array<{ field_name: string; cn_name: string | null; sort_order: number | null; hidden: number | null }>;
  } catch (_error) {
    return baseColumns;
  }

  const hiddenColumns = new Set<string>();
  const orderMap = new Map<string, number>();
  const cnNameMap = new Map<string, string>();

  for (const row of mappingRows) {
    if (row.hidden) {
      hiddenColumns.add(row.field_name);
    }
    if (typeof row.sort_order === 'number' && row.sort_order > 0) {
      orderMap.set(row.field_name, row.sort_order);
    }
    if (row.cn_name && row.cn_name.trim()) {
      cnNameMap.set(row.field_name, row.cn_name.trim());
    }
  }

  const visibleColumns = baseColumns
    .filter(column => !hiddenColumns.has(column.name))
    .map(column => {
      const cnName = cnNameMap.get(column.name) || '';
      return {
        ...column,
        cnName,
        header: cnName ? `${cnName} (${column.name})` : column.name
      };
    });

  if (orderMap.size === 0) {
    return visibleColumns;
  }

  return [...visibleColumns].sort((a, b) => {
    const orderA = orderMap.get(a.name) ?? Number.MAX_SAFE_INTEGER;
    const orderB = orderMap.get(b.name) ?? Number.MAX_SAFE_INTEGER;
    return orderA - orderB;
  });
}

function escapeCsvCell(value: any): string {
  if (value === null || value === undefined) {
    return '';
  }

  const text = String(value);
  if (/[",\r\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }

  return text;
}

/**
 * GET /api/data/raw-fields/:metadataId - 获取接口对应的官方API的所有原始字段
 * 实时请求官方API，返回所有字段（包括空值字段）
 */
router.get('/raw-fields/:metadataId', async (req: Request, res: Response) => {
  try {
    const metadataId = parseInt(req.params.metadataId, 10);
    
    if (isNaN(metadataId)) {
      res.status(400).json({
        success: false,
        message: '无效的元数据 ID'
      });
      return;
    }
    
    // 获取元数据
    const metadata = await metadataService.getMetadataById(metadataId);

    // 根据不同的接口类型，调用不同的方法获取原始字段
    let rawFields: { name: string; value: any; description: string }[] = [];

    // 解析 datacenter_config 获取 apiType 和 baseUrl
    let apiType: string | null = null;
    let baseUrl: string | null = null;
    if (metadata.datacenter_config) {
      try {
        const config = JSON.parse(metadata.datacenter_config);
        apiType = config.apiType || null;
        baseUrl = config.baseUrl || null;
      } catch (e) {
        // 解析失败
      }
    }

    // 东方财富股票列表接口
    if (apiType === 'stock_jsonp') {
      rawFields = await fetchEastmoneyStockRawFields();
    }
    // 东方财富数据中心接口（股东户数、公告等）
    else if (apiType === 'datacenter' && baseUrl) {
      rawFields = await fetchEastmoneyDatacenterRawFields(baseUrl);
    }
    // 其他接口暂不支持
    else {
      res.json({
        success: true,
        data: {
          supported: false,
          message: '该接口暂不支持查看原始字段',
          fields: []
        }
      });
      return;
    }
    
    res.json({
      success: true,
      data: {
        supported: true,
        tableName: metadata.table_name,
        cnName: metadata.cn_name,
        totalFields: rawFields.length,
        fields: rawFields
      }
    });
    
  } catch (error) {
    console.error('获取原始字段失败:', error);
    res.status(500).json({
      success: false,
      message: '获取原始字段失败',
      error: (error as Error).message
    });
  }
});

/**
 * 智能推断字段含义（方案1）
 * 根据字段值的特征推断其含义
 */
function inferFieldMeaning(_fieldName: string, value: any): string {
  if (value === null || value === undefined || value === '-') {
    return '';
  }
  
  const strValue = String(value);
  
  // 1. 股票代码：6位数字字符串
  if (typeof value === 'string' && /^\d{6}$/.test(value)) {
    return '(推断)股票代码';
  }
  
  // 2. 市场代码：0或1
  if (typeof value === 'number' && (value === 0 || value === 1)) {
    return '(推断)市场代码(0深/1沪)';
  }
  
  // 3. 日期格式：8位数字如20251207
  if (typeof value === 'number' && value > 19900101 && value < 20991231) {
    const dateStr = String(value);
    if (dateStr.length === 8) {
      return '(推断)日期(YYYYMMDD)';
    }
  }
  
  // 4. 时间戳：10位或13位数字
  if (typeof value === 'number') {
    if (value > 1000000000 && value < 2000000000) {
      return '(推断)时间戳(秒)';
    }
    if (value > 1000000000000 && value < 2000000000000) {
      return '(推断)时间戳(毫秒)';
    }
  }
  
  // 5. 百分比：-100到100之间的小数
  if (typeof value === 'number' && !Number.isInteger(value) && value >= -100 && value <= 100) {
    // 检查是否有小数点后两位
    const decimalPlaces = (strValue.split('.')[1] || '').length;
    if (decimalPlaces === 2) {
      return '(推断)百分比/比率';
    }
  }
  
  // 6. 价格：正数，通常有2位小数
  if (typeof value === 'number' && value > 0 && value < 10000) {
    const decimalPlaces = (strValue.split('.')[1] || '').length;
    if (decimalPlaces === 2) {
      return '(推断)价格';
    }
  }
  
  // 7. 大数值：可能是市值、成交额等
  if (typeof value === 'number' && value > 100000000) {
    return '(推断)大数值(市值/成交额等)';
  }
  
  // 8. 中等数值：可能是成交量
  if (typeof value === 'number' && value > 10000 && value < 100000000) {
    return '(推断)中等数值(成交量等)';
  }
  
  // 9. 中文字符串：可能是名称、行业等
  if (typeof value === 'string' && /[\u4e00-\u9fa5]/.test(value)) {
    if (value.length <= 4) {
      return '(推断)股票名称';
    }
    if (value.length <= 10) {
      return '(推断)行业/板块名称';
    }
    return '(推断)中文文本';
  }
  
  return '';
}

/**
 * 获取东方财富股票列表API的所有原始字段
 * 结合方案1（智能推断）和方案2（已知映射）
 */
async function fetchEastmoneyStockRawFields(): Promise<{ name: string; value: any; description: string }[]> {
  // 生成 f1 到 f300 的所有字段
  const allFields = Array.from({ length: 300 }, (_, i) => `f${i + 1}`);
  
  const url = `https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1&fs=m:0+t:6&fields=${allFields.join(',')}`;
  
  const response = await axios.get(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      'Referer': 'https://quote.eastmoney.com/'
    },
    timeout: 30000
  });
  
  if (!response.data?.data?.diff) {
    throw new Error('API返回数据格式错误');
  }
  
  const item = Object.values(response.data.data.diff)[0] as Record<string, any>;
  
  // 方案2：已知字段含义映射（仅保留通过爬取行情中心表头真正验证过的字段）
  // 数据来源：https://quote.eastmoney.com/center/gridlist.html 表头
  const fieldMeanings: Record<string, string> = {
    // 基础信息
    f1: '市场类型(0深/1沪)',
    f12: '股票代码',
    f13: '市场代码',
    f14: '股票名称',
    // 价格相关
    f2: '最新价',
    f4: '涨跌额',
    f15: '最高价',
    f16: '最低价',
    f17: '开盘价(今开)',
    f18: '昨收价',
    // 涨跌幅相关
    f3: '涨跌幅(%)',
    f7: '振幅(%)',
    // 成交相关
    f5: '成交量(手)',
    f6: '成交额',
    f8: '换手率(%)',
    f10: '量比',
    // 估值相关
    f9: '市盈率(动态)',
    f23: '市净率',
  };
  
  // 构建结果数组
  const result: { name: string; value: any; description: string }[] = [];
  
  for (const field of allFields) {
    const value = item[field];
    // 优先使用已知映射，否则使用智能推断
    let description = fieldMeanings[field] || '';
    
    // 如果没有已知映射，尝试智能推断
    if (!description && value !== undefined && value !== null && value !== '-') {
      description = inferFieldMeaning(field, value);
    }
    
    result.push({
      name: field,
      value: value !== undefined && value !== null ? value : '(空)',
      description
    });
  }
  
  return result;
}

/**
 * 获取东方财富数据中心API的所有原始字段
 */
async function fetchEastmoneyDatacenterRawFields(requestUrl: string): Promise<{ name: string; value: any; description: string }[]> {
  // 修改请求URL，使用 columns=ALL 获取所有字段
  let url = requestUrl;
  if (url.includes('columns=')) {
    url = url.replace(/columns=[^&]*/, 'columns=ALL');
  } else {
    url += (url.includes('?') ? '&' : '?') + 'columns=ALL';
  }
  
  // 添加分页参数，只获取一条数据
  if (!url.includes('pageSize=')) {
    url += '&pageSize=1';
  }
  if (!url.includes('pageNumber=')) {
    url += '&pageNumber=1';
  }
  
  const response = await axios.get(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      'Referer': 'https://data.eastmoney.com/'
    },
    timeout: 30000
  });
  
  if (!response.data?.result?.data || response.data.result.data.length === 0) {
    throw new Error('API返回数据为空');
  }
  
  const item = response.data.result.data[0] as Record<string, any>;
  
  // 构建结果数组
  const result: { name: string; value: any; description: string }[] = [];
  
  for (const [key, value] of Object.entries(item)) {
    result.push({
      name: key,
      value: value !== undefined && value !== null ? value : '(空)',
      description: '' // 数据中心接口字段含义需要根据具体接口确定
    });
  }
  
  // 按字段名排序
  result.sort((a, b) => a.name.localeCompare(b.name));
  
  return result;
}

/**
 * GET /api/data/field-values/:tableName/:fieldName - 获取指定字段的唯一值分布
 * 用于帮助用户理解字段含义
 */
router.get('/field-values/:tableName/:fieldName', async (req: Request, res: Response) => {
  try {
    const { tableName, fieldName } = req.params;
    
    // 验证表名和字段名（防止SQL注入）
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(tableName) || !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(fieldName)) {
      res.status(400).json({
        success: false,
        message: '无效的表名或字段名'
      });
      return;
    }
    
    const db = getDatabase();
    
    // 检查表是否存在
    const tableCheck = db.exec(`SELECT name FROM sqlite_master WHERE type='table' AND name='${tableName}'`);
    if (tableCheck.length === 0 || tableCheck[0].values.length === 0) {
      res.status(404).json({
        success: false,
        message: '表不存在'
      });
      return;
    }
    
    // 检查字段是否存在
    const columnCheck = db.exec(`PRAGMA table_info(${tableName})`);
    const columns = columnCheck[0]?.values.map((row: any) => row[1]) || [];
    if (!columns.includes(fieldName)) {
      res.status(404).json({
        success: false,
        message: '字段不存在'
      });
      return;
    }
    
    // 获取唯一值及其数量（限制前10个最常见的值）
    const result = db.exec(`
      SELECT ${fieldName} as value, COUNT(*) as count 
      FROM ${tableName} 
      WHERE ${fieldName} IS NOT NULL AND ${fieldName} != ''
      GROUP BY ${fieldName} 
      ORDER BY count DESC 
      LIMIT 10
    `);
    
    // 获取总记录数
    const totalResult = db.exec(`SELECT COUNT(*) FROM ${tableName}`);
    const totalRows = totalResult[0]?.values[0]?.[0] as number || 0;
    
    // 获取唯一值总数
    const uniqueResult = db.exec(`SELECT COUNT(DISTINCT ${fieldName}) FROM ${tableName} WHERE ${fieldName} IS NOT NULL AND ${fieldName} != ''`);
    const uniqueCount = uniqueResult[0]?.values[0]?.[0] as number || 0;
    
    // 解析结果
    const values = result[0]?.values.map((row: any) => ({
      value: row[0],
      count: row[1]
    })) || [];
    
    res.json({
      success: true,
      data: {
        tableName,
        fieldName,
        totalRows,
        uniqueCount,
        topValues: values
      }
    });
    
  } catch (error) {
    console.error('获取字段值分布失败:', error);
    res.status(500).json({
      success: false,
      message: '获取字段值分布失败',
      error: (error as Error).message
    });
  }
});

/**
 * GET /api/data/check-fields - 检查所有接口的实际返回字段与数据库表结构是否一致
 * 使用最近一次采集时记录的接口返回字段（last_api_fields）与数据库表列对比
 * 注意：此路由必须放在 /:metadataId 之前，否则会被错误匹配
 */
router.get('/check-fields', async (req: Request, res: Response) => {
  try {
    const metadataList = await metadataService.getMetadataList();
    const results: any[] = [];
    
    for (const metadata of metadataList) {
      // 获取最近一次采集时接口实际返回的字段
      let apiFields: string[] = [];
      if ((metadata as any).last_api_fields) {
        try {
          apiFields = JSON.parse((metadata as any).last_api_fields);
        } catch (e) {
          // 解析失败，保持空数组
        }
      }
      
      // 如果没有记录过接口字段，使用配置字段作为备选
      let configFields: string[] = [];
      try {
        const outputConfig = JSON.parse(metadata.output_config);
        configFields = outputConfig.map((c: any) => c.name);
      } catch (e) {
        // 解析失败
      }
      
      // 优先使用接口实际返回的字段，如果没有则使用配置字段
      const compareFields = apiFields.length > 0 ? apiFields : configFields;
      const fieldSource = apiFields.length > 0 ? 'api' : 'config';
      
      // 获取数据库表的实际列
      let tableFields: string[] = [];
      try {
        const db = getDatabase();
        const result = db.exec(`PRAGMA table_info(${metadata.table_name})`);
        if (result.length > 0) {
          // 排除系统字段 id, metadata_id, created_at, collected_at
          tableFields = result[0].values
            .map((row: any) => row[1] as string)
            .filter((name: string) => !['id', 'metadata_id', 'created_at', 'collected_at'].includes(name));
        }
      } catch (e) {
        results.push({
          id: metadata.id,
          cn_name: metadata.cn_name,
          table_name: metadata.table_name,
          status: 'error',
          message: '获取表结构失败',
          apiFields: compareFields,
          apiFieldCount: compareFields.length,
          tableFields: [],
          tableFieldCount: 0,
          missingInTable: [],
          extraInTable: [],
          fieldSource
        });
        continue;
      }
      
      // 对比字段
      const missingInTable = compareFields.filter(f => !tableFields.includes(f));
      const extraInTable = tableFields.filter(f => !compareFields.includes(f));
      
      const status = missingInTable.length === 0 && extraInTable.length === 0 ? 'ok' : 'mismatch';
      
      results.push({
        id: metadata.id,
        cn_name: metadata.cn_name,
        table_name: metadata.table_name,
        status,
        message: status === 'ok' ? '字段一致' : '字段不一致',
        apiFields: compareFields,
        apiFieldCount: compareFields.length,
        tableFields,
        tableFieldCount: tableFields.length,
        missingInTable,  // 接口返回但表中没有的字段
        extraInTable,    // 表中有但接口没返回的字段
        fieldSource      // 字段来源：api（实际采集）或 config（配置）
      });
    }
    
    // 统计
    const okCount = results.filter(r => r.status === 'ok').length;
    const mismatchCount = results.filter(r => r.status === 'mismatch').length;
    const errorCount = results.filter(r => r.status === 'error').length;
    
    res.json({
      success: true,
      data: {
        summary: {
          total: results.length,
          ok: okCount,
          mismatch: mismatchCount,
          error: errorCount
        },
        details: results
      }
    });
    
  } catch (error) {
    console.error('检查字段配置失败:', error);
    res.status(500).json({
      success: false,
      message: '检查字段配置失败',
      error: (error as Error).message
    });
  }
});

/**
 * GET /api/data/:metadataId - 获取指定接口的业务数据（分页）
 */
router.get('/:metadataId/export', async (req: Request, res: Response) => {
  try {
    const metadataId = parseInt(req.params.metadataId, 10);

    if (isNaN(metadataId)) {
      res.status(400).json({
        success: false,
        message: '无效的元数据 ID'
      });
      return;
    }

    const format = ((req.query.format as string) || 'csv').toLowerCase();
    if (!['csv', 'json'].includes(format)) {
      res.status(400).json({
        success: false,
        message: '仅支持 csv 或 json 导出格式'
      });
      return;
    }

    const visibleOnly = req.query.visibleOnly !== 'false';
    const { keyword, sortField, sortOrder, searchColumn, exactMatch } = parseDataQueryParams(req);
    const metadata = await metadataService.getMetadataById(metadataId);
    const outputConfig: OutputConfig[] = JSON.parse(metadata.output_config);
    const exportColumns = getExportColumns(metadata.table_name, outputConfig, visibleOnly);
    const result = tableService.queryAllData(
      metadata.table_name,
      keyword,
      sortField || undefined,
      (sortOrder as 'asc' | 'desc') || undefined,
      searchColumn || undefined,
      exactMatch,
      exportColumns.map(column => column.name)
    );

    if (format === 'json') {
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      res.send(JSON.stringify({
        metadata: {
          id: metadata.id,
          cnName: metadata.cn_name,
          tableName: metadata.table_name,
          visibleOnly,
          exportedAt: new Date().toISOString(),
          total: result.total
        },
        columns: exportColumns.map(column => ({
          field: column.name,
          header: column.header,
          cnName: column.cnName,
          description: column.description,
          type: column.type
        })),
        rows: result.data
      }, null, 2));
      return;
    }

    const headerRow = exportColumns.map(column => escapeCsvCell(column.header)).join(',');
    const dataRows = result.data.map(row =>
      exportColumns.map(column => escapeCsvCell(row[column.name])).join(',')
    );
    const csvContent = `\uFEFF${[headerRow, ...dataRows].join('\r\n')}`;

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.send(csvContent);
  } catch (error) {
    console.error('导出业务数据失败:', error);
    res.status(500).json({
      success: false,
      message: '导出业务数据失败',
      error: (error as Error).message
    });
  }
});

router.get('/:metadataId', async (req: Request, res: Response) => {
  try {
    const metadataId = parseInt(req.params.metadataId, 10);
    
    if (isNaN(metadataId)) {
      res.status(400).json({
        success: false,
        message: '无效的元数据 ID'
      });
      return;
    }
    
    // 获取分页参数
    const page = parseInt(req.query.page as string, 10) || 1;
    const pageSize = parseInt(req.query.pageSize as string, 10) || 20;
    const keyword = req.query.keyword as string || '';
    let sortField = req.query.sortField as string || '';
    let sortOrder = req.query.sortOrder as 'asc' | 'desc' | '' || '';
    const searchColumn = req.query.searchColumn as string || '';
    const exactMatch = req.query.exactMatch === 'true';  // 精确匹配模式
    
    // 验证分页参数
    if (page < 1 || pageSize < 1 || pageSize > 100) {
      res.status(400).json({
        success: false,
        message: '无效的分页参数'
      });
      return;
    }
    
    // 获取元数据
    const metadata = await metadataService.getMetadataById(metadataId);

    // 如果没有传入排序参数，使用元数据中的默认排序
    if (!sortField && (metadata as any).default_sort_field) {
      sortField = (metadata as any).default_sort_field;
      sortOrder = ((metadata as any).default_sort_order as 'asc' | 'desc') || 'desc';
    }

    // 解析出参配置
    const outputConfig: OutputConfig[] = JSON.parse(metadata.output_config);
    
    // 获取接口实际返回的字段（优先使用 last_api_fields，但需要从 output_config 获取完整信息）
    let apiFieldNames: string[] = outputConfig.map(c => c.name);
    if ((metadata as any).last_api_fields) {
      try {
        apiFieldNames = JSON.parse((metadata as any).last_api_fields);
      } catch (e) {
        // 解析失败，使用配置字段
      }
    }
    
    // 构建接口字段的完整信息（从 output_config 中查找对应的 type 和 description）
    const outputConfigMap = new Map(outputConfig.map(c => [c.name, c]));
    const apiFields = apiFieldNames.map(name => {
      const config = outputConfigMap.get(name);
      return {
        name,
        type: config?.type || 'TEXT',
        description: config?.description || name
      };
    });
    
    // 获取数据库表的实际列（包含类型信息）
    let tableFields: { name: string; type: string; description: string }[] = [];
    try {
      const db = getDatabase();
      const result = db.exec(`PRAGMA table_info(${metadata.table_name})`);
      if (result.length > 0) {
        // 排除系统字段 id, metadata_id, created_at, collected_at
        tableFields = result[0].values
          .filter((row: any) => !['id', 'metadata_id', 'created_at', 'collected_at'].includes(row[1] as string))
          .map((row: any) => {
            const fieldName = row[1] as string;
            const fieldType = row[2] as string;
            // 从 output_config 中查找描述
            const config = outputConfigMap.get(fieldName);
            return {
              name: fieldName,
              type: fieldType || 'TEXT',
              description: config?.description || fieldName
            };
          });
      }
    } catch (e) {
      // 获取失败
    }
    
    // 查询数据（支持搜索和排序）
    const result = tableService.queryData(
      metadata.table_name,
      page,
      pageSize,
      keyword,
      sortField || undefined,
      (sortOrder as 'asc' | 'desc') || undefined,
      searchColumn || undefined,
      exactMatch
    );
    
    res.json({
      success: true,
      data: {
        list: result.data,
        total: result.total,
        page,
        pageSize,
        totalPages: Math.ceil(result.total / pageSize),
        columns: outputConfig,  // 返回列配置信息
        apiFields,              // 接口字段列表（完整信息）
        apiFieldCount: apiFields.length,
        tableFields,            // 表字段列表（完整信息）
        tableFieldCount: tableFields.length,
        defaultSortField: (metadata as any).default_sort_field || null,
        defaultSortOrder: (metadata as any).default_sort_order || null
      }
    });
    
  } catch (error) {
    console.error('获取业务数据失败:', error);
    
    if ((error as Error).message.includes('不存在')) {
      res.status(404).json({
        success: false,
        message: (error as Error).message
      });
    } else {
      res.status(500).json({
        success: false,
        message: '获取业务数据失败',
        error: (error as Error).message
      });
    }
  }
});

/**
 * GET /api/data/:metadataId/daily-stats - 获取每日数据统计
 */
router.get('/:metadataId/daily-stats', async (req: Request, res: Response) => {
  try {
    const metadataId = parseInt(req.params.metadataId, 10);
    
    if (isNaN(metadataId)) {
      res.status(400).json({
        success: false,
        message: '无效的元数据 ID'
      });
      return;
    }
    
    // 获取元数据
    const metadata = await metadataService.getMetadataById(metadataId);
    const tableName = metadata.table_name;
    
    // 根据表名确定日期字段
    let dateField: string;
    let dateFormat: string;
    
    if (tableName === 'hudongyi') {
      // 互动易：使用 answerTime 字段，格式为 "2025-12-26 10:30:00"
      dateField = 'answerTime';
      dateFormat = 'substr(%s, 1, 10)'; // 取前10个字符 YYYY-MM-DD
    } else if (tableName === 'sse_ehudong') {
      // 上证e互动：使用 answerTime 字段，格式为 "2025年12月26日 10:30"
      dateField = 'answerTime';
      dateFormat = 'substr(%s, 1, 11)'; // 取前11个字符 "2025年12月26日"
    } else {
      // 其他表：尝试使用 tdate 或 created_at
      dateField = 'tdate';
      dateFormat = 'substr(%s, 1, 10)';
    }
    
    // 获取每日统计
    const stats = tableService.getDailyStats(tableName, dateField, dateFormat);
    
    res.json({
      success: true,
      data: {
        tableName,
        dateField,
        stats
      }
    });
    
  } catch (error) {
    console.error('获取每日统计失败:', error);
    res.status(500).json({
      success: false,
      message: '获取每日统计失败',
      error: (error as Error).message
    });
  }
});

/**
 * GET /api/data/:metadataId/detail - 获取大宗交易细项数据
 * 代理请求巨潮资讯的细项接口
 */
router.get('/:metadataId/detail', async (req: Request, res: Response) => {
  try {
    const metadataId = parseInt(req.params.metadataId, 10);
    const { tdate, scode } = req.query;
    
    if (isNaN(metadataId)) {
      res.status(400).json({
        success: false,
        message: '无效的元数据 ID'
      });
      return;
    }
    
    if (!tdate || !scode) {
      res.status(400).json({
        success: false,
        message: '缺少必要参数: tdate, scode'
      });
      return;
    }
    
    // 获取元数据，验证是否是大宗交易明细接口
    const metadata = await metadataService.getMetadataById(metadataId);
    
    // 只有大宗交易明细接口才支持细项查询
    if (metadata.table_name !== 'dzjy_detail') {
      res.status(400).json({
        success: false,
        message: '该接口不支持细项查询'
      });
      return;
    }
    
    // 代理请求巨潮资讯的细项接口
    const response = await axios.post(
      'https://www.cninfo.com.cn/data20/ints/detail',
      `tdate=${tdate}&scode=${scode}`,
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Referer': 'https://www.cninfo.com.cn/new/commonUrl?url=data/dzjy'
        },
        timeout: 30000
      }
    );
    
    if (response.data.code === 200 && response.data.data.resultMsg === 'success') {
      res.json({
        success: true,
        data: response.data.data.records || []
      });
    } else {
      res.json({
        success: true,
        data: []
      });
    }
    
  } catch (error) {
    console.error('获取细项数据失败:', error);
    res.status(500).json({
      success: false,
      message: '获取细项数据失败',
      error: (error as Error).message
    });
  }
});

/**
 * DELETE /api/data/clear-all - 清空所有接口的业务数据和更新日志
 */
router.delete('/clear-all', async (req: Request, res: Response) => {
  try {
    // 获取所有元数据
    const metadataList = await metadataService.getMetadataList();
    
    let clearedTables = 0;
    let clearedLogs = 0;
    
    for (const metadata of metadataList) {
      // 清空业务数据表
      try {
        tableService.truncateTable(metadata.table_name);
        clearedTables++;
      } catch (e) {
        console.warn(`清空表 ${metadata.table_name} 失败:`, e);
      }
      
      // 清空更新日志
      try {
        const { run } = require('../services/database');
        run('DELETE FROM update_logs WHERE metadata_id = ?', [metadata.id]);
        clearedLogs++;
      } catch (e) {
        console.warn(`清空接口 ${metadata.id} 的更新日志失败:`, e);
      }
      
      // 重置最后更新时间
      try {
        const { run } = require('../services/database');
        run('UPDATE api_metadata SET last_update_time = NULL WHERE id = ?', [metadata.id]);
      } catch (e) {
        console.warn(`重置接口 ${metadata.id} 的更新时间失败:`, e);
      }
    }
    
    res.json({
      success: true,
      message: `已清空 ${clearedTables} 个数据表和 ${clearedLogs} 个接口的更新日志`
    });
    
  } catch (error) {
    console.error('清空所有数据失败:', error);
    res.status(500).json({
      success: false,
      message: '清空所有数据失败',
      error: (error as Error).message
    });
  }
});

export default router;
