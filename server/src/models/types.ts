/**
 * 数据模型类型定义
 */

// 请求方法类型
export type RequestMethod = 'GET' | 'POST';

// 出参配置接口
export interface OutputConfig {
  name: string;        // 字段名
  type: string;        // 字段类型 (TEXT, INTEGER, REAL)
  description?: string; // 字段描述
}

// 更新模式类型
export type UpdateMode = 'incremental' | 'full' | 'delete_range_insert';

// API 元数据接口
export interface ApiMetadata {
  id: number;
  cn_name: string;           // 接口中文名
  source_url: string;        // 来源页面 URL
  request_method: RequestMethod; // 请求方法 (GET/POST)
  output_config: string;     // 出参配置 (JSON 字符串)
  table_name: string;        // 对应的业务表名
  update_mode: UpdateMode;   // 更新模式：incremental（增量）或 full（全量）
  date_field: string | null; // 日期字段名，用于过滤数据
  date_range: number | null; // 天数范围，如10表示最近10天（过去）
  future_days: number | null; // 未来天数，如2表示包含未来2天
  fixed_begin_date: string | null; // 固定开始日期，如 '2025-01-01'
  datacenter_config?: string; // 东方财富 datacenter API 配置（JSON 字符串）
  fields_verified?: number; // 是否已在“更新接口&字段”人工确认（0/1）
  row_bg_color?: string | null; // 首页接口行背景色
  last_update_time: string | null;  // 最后更新时间
  created_at: string;
  is_active?: number; // 1=active, 0=disabled
  updated_at: string;
}

// 带记录计数的元数据接口
export interface ApiMetadataWithCount extends ApiMetadata {
  record_count: number;      // 业务表记录数
  last_duration: number | null;  // 最新一次更新耗时（毫秒）
}

// 创建元数据 DTO
export interface CreateMetadataDto {
  cn_name: string;
  source_url: string;
  request_method?: RequestMethod; // 请求方法，默认 GET
  output_config: OutputConfig[];
  table_name: string;
}

// 更新元数据 DTO
export interface UpdateMetadataDto {
  cn_name?: string;
  source_url?: string;
  request_method?: RequestMethod;
  output_config?: OutputConfig[];
  fields_verified?: number | boolean;
  row_bg_color?: string | null;
  is_active?: number | boolean;
}

// 分页结果接口
export interface PaginatedResult<T = any> {
  data: T[];
  total: number;
  page: number;
  pageSize: number;
  totalPages: number;
}

// 更新日志接口
export interface UpdateLog {
  id: number;
  metadata_id: number;
  update_time: string;
  total_count: number;
  daily_stats: string | null;  // JSON 字符串，格式: [{date: string, count: number}]
  status: 'success' | 'error';
  error_message: string | null;
  created_at: string;
  duration: number | null;     // 更新耗时（毫秒）
}

// 每日统计项
export interface DailyStatItem {
  date: string;
  count: number;
}

// WebSocket 进度消息接口
export interface ProgressMessage {
  type: 'progress' | 'complete' | 'error' | 'batch_start' | 'batch_progress' | 'batch_complete';
  metadataId: number;
  progress: number;      // 0-100 的百分比
  message: string;       // 状态描述
  timestamp?: number;    // 时间戳
  total?: number;        // 批量更新总数
  current?: number;      // 批量更新当前进度
}

// 列配置接口（用于动态表）
export interface ColumnConfig {
  name: string;
  type: string;
}

// API 类型
export type ApiType =
  | 'datacenter'      // 东方财富 datacenter API
  | 'report'          // 东方财富研报 API
  | 'fund'            // 东方财富基金持仓 API
  | 'stock_jsonp'     // 东方财富 A股列表 JSONP
  | 'announcement'    // 东方财富公告 API
  | 'hudongyi'        // 互动易 API
  | 'sse_hudong';     // 上证e互动 API

// 循环策略类型
export type LoopStrategyType = 'single' | 'double';

// 日期批次配置
export interface DateBatchConfig {
  batchDays: number;                    // 每批天数（如10天）
  beginDateParam: string;               // 开始日期参数名
  endDateParam: string;                 // 结束日期参数名
  useSingleDate?: boolean;              // 是否使用同一天（如互动易按天循环）
  dateFormat?: string;                  // 日期格式，默认 'YYYY-MM-DD'
  dateField?: string;                   // 循环日期对应的数据字段名（如 notice_date）
  dateFieldLabel?: string;              // 日期字段的中文名（如 公告日期）
}

// 请求延迟配置
export interface RequestDelayConfig {
  betweenPages: number;                 // 分页请求间隔(ms)
  betweenBatches?: number;              // 批次请求间隔(ms)
}

// 循环策略配置
export interface LoopStrategyConfig {
  type: LoopStrategyType;               // single=仅分页, double=日期分批+分页
  dateBatch?: DateBatchConfig;          // 日期批次配置（仅 double 类型需要）
  requestDelay?: RequestDelayConfig;    // 请求延迟配置
}

// 前端显示配置
export interface DisplayConfig {
  batchSizeLabel?: string;              // 批次大小显示文本（如"每10天"）
  fixedDateRange?: {                    // 固定日期范围显示
    begin: string;
    end: string;
  };
}

export interface RelativeDateRule {
  mode: 'relative_weekday';
  weekday: number;                      // JS getDay 口径：0=周日，1=周一 ... 6=周六
  weeksAgo?: number;                    // 往前推几周，默认 1
  weekStartsOn?: number;                // 每周起始日，默认 1=周一
}

// 通用 API 请求配置接口
export interface ApiRequestConfig {
  // API 类型，决定使用哪个处理器
  apiType: ApiType;

  // 请求配置
  baseUrl: string;                      // API 地址
  method?: 'GET' | 'POST';              // 请求方法，默认 GET
  params: Record<string, any>;          // 核心业务参数

  // 响应配置
  dataPath: string;                     // 数据路径，如 "result.data"
  totalPagesPath?: string;              // 总页数路径，如 "result.pages"
  totalCountPath?: string;              // 总数路径，如 "result.count"

  // 分页配置
  pagination: {
    pageNumParam: string;               // 页码参数名，如 "pageNumber"
    pageSizeParam: string;              // 每页数量参数名，如 "pageSize"
    fieldSyncPageSize: number;          // 字段检查时的每页数量（通常是1）
    crawlerPageSize: number;            // 数据采集时的每页数量（通常是500）
  };

  // 日期配置（可选，用于需要动态日期的接口）
  dateConfig?: {
    type: 'filter' | 'param' | 'daily'; // 日期过滤类型：filter=过滤条件, param=参数, daily=按天循环
    field?: string;                     // 日期字段名（用于 filter 类型）
    filterTemplate?: string;            // 过滤条件模板，如 "({field}>='{start}')({field}<='{end}')"
    beginParam?: string;                // 开始日期参数名（用于 param 类型）
    endParam?: string;                  // 结束日期参数名（用于 param 类型）
    fieldSyncDays?: number;             // 字段检查时的日期范围（天），如 30
    crawlerDays?: number;               // 数据采集时的日期范围（天），如 365
    dateRangeMode?: 'offset_days' | 'count_including_today'; // date_range 口径
    dateUnit?: 'calendar_day' | 'trading_day'; // 日期范围单位：自然日或交易日（工作日口径）
    closedDateRanges?: Array<{ begin: string; end: string }>; // 休市日期区间
    relativeDateRule?: RelativeDateRule; // 动态相对日期规则
  };

  // 循环策略配置（新增）
  loopStrategy?: LoopStrategyConfig;

  // 前端显示配置（新增）
  displayConfig?: DisplayConfig;
}

// 保留旧的 DatacenterConfig 用于兼容（已废弃，请使用 ApiRequestConfig）
export interface DatacenterConfig {
  reportName: string;
  sortColumns: string;
  sortTypes?: string;
  columns?: string;
  quoteColumns?: string;
  filter?: string;
  filterDateField?: string;
  source?: string;
  client?: string;
}
