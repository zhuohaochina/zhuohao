/**
 * 数据迁移服务模块
 * 负责将旧的 update_rule 字符串迁移到新的结构化字段
 */
import { UpdateMode } from '../models/types';

/**
 * 解析结果接口
 */
export interface ParsedUpdateRule {
  updateMode: UpdateMode;
  dateField: string | null;
  dateRange: number | null;
}

/**
 * 日期字段映射表
 * 根据接口类型后缀确定日期字段
 */
const DATE_FIELD_MAPPING: Record<string, string> = {
  '（股东增减持）': 'notice_date',
  '（高管增减持）': 'notice_date',
  '（互动易）': 'pubDate',
  '(互动易)': 'pubDate',
  '（上证e互动）': 'eitime',
  '(上证e互动)': 'eitime',
};

/**
 * 解析 update_rule 字符串，提取结构化信息
 * @param updateRule 原始更新规则字符串
 * @returns 解析后的结构化数据
 */
export function parseUpdateRule(updateRule: string | null): ParsedUpdateRule {
  // 默认值
  const result: ParsedUpdateRule = {
    updateMode: 'full',
    dateField: null,
    dateRange: null,
  };

  if (!updateRule) {
    return result;
  }

  // 1. 解析更新模式
  if (updateRule.startsWith('【增量更新】')) {
    result.updateMode = 'incremental';
  } else if (updateRule.startsWith('【全量更新】')) {
    result.updateMode = 'full';
  }

  // 2. 解析日期字段（根据后缀）
  for (const [suffix, field] of Object.entries(DATE_FIELD_MAPPING)) {
    if (updateRule.includes(suffix)) {
      result.dateField = field;
      break;
    }
  }

  // 3. 解析天数范围
  // 匹配各种格式：取最近N天、取最近N个交易日等
  const dayPatterns = [
    /取最近(\d+)天的数据/,
    /取最近(\d+)个交易日的数据/,
    /取最近(\d+)天的互动易数据/,
  ];

  for (const pattern of dayPatterns) {
    const match = updateRule.match(pattern);
    if (match) {
      result.dateRange = parseInt(match[1], 10);
      break;
    }
  }

  // 4. 特殊规则处理
  if (updateRule.includes('取近一周的数据')) {
    result.dateRange = 7;
    result.dateField = result.dateField || 'tdate';
  } else if (updateRule.includes('取最近30天的数据') && !result.dateRange) {
    result.dateRange = 30;
    result.dateField = result.dateField || 'tdate';
  }

  return result;
}

/**
 * 获取日期字段的中文标签
 * @param dateField 日期字段名
 * @returns 中文标签
 */
export function getDateFieldLabel(dateField: string | null): string {
  if (!dateField) return '';
  
  const labels: Record<string, string> = {
    'notice_date': '公告日期',
    'change_date': '变动日期',
    'tdate': '交易日期',
    'pubDate': '发布日期',
    'eitime': '回复时间',
    'ann_date': '公告日期',
    'trade_date': '交易日期',
  };
  
  return labels[dateField] || dateField;
}

/**
 * 获取更新模式的中文标签
 * @param updateMode 更新模式
 * @returns 中文标签
 */
export function getUpdateModeLabel(updateMode: UpdateMode): string {
  return updateMode === 'incremental' ? '增量' : '全量';
}

export const migrationService = {
  parseUpdateRule,
  getDateFieldLabel,
  getUpdateModeLabel,
};
