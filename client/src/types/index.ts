/**
 * 前端类型定义
 */

// 出参配置接口
export interface OutputConfig {
  name: string
  type: string
  description?: string
}

// 请求方法类型
export type RequestMethod = 'GET' | 'POST'

// 更新模式类型
export type UpdateMode = 'incremental' | 'full'

// 循环策略类型
export type LoopStrategyType = 'single' | 'double'

// 日期批次配置
export interface DateBatchConfig {
  batchDays: number
  beginDateParam: string
  endDateParam: string
  useSingleDate?: boolean
  dateFormat?: string
  dateField?: string              // 循环日期对应的数据字段名（如 notice_date）
  dateFieldLabel?: string         // 日期字段的中文名（如 公告日期）
}

// 请求延迟配置
export interface RequestDelayConfig {
  betweenPages: number
  betweenBatches?: number
}

// 循环策略配置
export interface LoopStrategyConfig {
  type: LoopStrategyType
  dateBatch?: DateBatchConfig
  requestDelay?: RequestDelayConfig
}

// 前端显示配置
export interface DisplayConfig {
  batchSizeLabel?: string
  fixedDateRange?: {
    begin: string
    end: string
  }
}

export interface DateConfig {
  type?: 'filter' | 'param' | 'daily'
  field?: string
  beginParam?: string
  endParam?: string
  fieldSyncDays?: number
  crawlerDays?: number
  dateRangeMode?: 'offset_days' | 'count_including_today'
  dateUnit?: 'calendar_day' | 'trading_day'
  closedDateRanges?: Array<{ begin: string; end: string }>
}

// API 请求配置（简化版，用于前端显示）
export interface ApiRequestConfig {
  apiType: string
  dateConfig?: DateConfig
  loopStrategy?: LoopStrategyConfig
  displayConfig?: DisplayConfig
}

// API 元数据接口
export interface ApiMetadata {
  id: number
  cn_name: string
  source_url: string
  request_method: RequestMethod
  output_config: string
  table_name: string
  update_mode: UpdateMode
  date_field: string | null
  date_range: number | null
  future_days: number | null
  fields_verified?: number
  row_bg_color?: string | null
  is_active?: number
  last_update_time: string | null
  created_at: string
  updated_at: string
  record_count?: number
  last_duration?: number | null  // 最新一次更新耗时（毫秒）
  datacenter_config?: string  // JSON 字符串，包含 ApiRequestConfig
}

// 分页结果接口
export interface PaginatedResult<T = any> {
  data: T[]
  total: number
  page: number
  pageSize: number
  totalPages: number
}

// WebSocket 进度消息接口
export interface ProgressMessage {
  type: 'progress' | 'complete' | 'error'
  metadataId: number
  progress: number
  message: string
  timestamp: number
}

export interface DataComboColumn {
  field: string
  label: string
  description: string
  type: string
  display: {
    formatAsWanYi: boolean
    formatAsTimestamp: boolean
    valueMapping: Record<string, string>
    unit: string
    textClamp: boolean
  }
}

export interface DataComboChildSummary {
  key: string
  label: string
}

export interface DataComboSummary {
  id: string
  name: string
  description: string
  mainTableName: string
  mainTableLabel: string
  isBuiltIn: boolean
  children: DataComboChildSummary[]
}

export interface DataComboChildCollection extends DataComboChildSummary {
  tableName: string
  codeField: string
  dataField: string
  countField: string
  columns: DataComboColumn[]
}

export interface DataComboRowViewLayoutConfig {
  cols: number
  rows: number
}

export interface DataComboRowViewMainLayout extends DataComboRowViewLayoutConfig {
  slots: Array<string | null>
}

export interface DataComboRowViewLayout {
  main: DataComboRowViewMainLayout
  children: Record<string, DataComboRowViewLayoutConfig>
}

export interface DataComboDetail extends DataComboSummary {
  mainCodeField: string
  mainColumns: DataComboColumn[]
  childCollections: DataComboChildCollection[]
  rowViewLayout: DataComboRowViewLayout
}

export interface DataComboRowResult {
  combo: DataComboDetail
  row: Record<string, any>
}

export interface DataComboMetadataOption {
  metadataId: number
  name: string
  tableName: string
  fields: DataComboColumn[]
  defaultDisplayFields: string[]
  recommendedCodeFields: string[]
}

export interface CreateDataComboChildInput {
  tableName: string
  codeField: string
  displayFields: string[]
  sortField?: string
  sortOrder?: 'asc' | 'desc'
}

export interface CreateDataComboInput {
  name: string
  description?: string
  mainTableName: string
  mainCodeField: string
  mainDisplayFields: string[]
  children: CreateDataComboChildInput[]
}
