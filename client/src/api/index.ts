/**
 * API 请求封装
 */
import axios from 'axios'

type RequestAbortOptions = {
  signal?: AbortSignal
}

// 创建 axios 实例
const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache',
    'Expires': '0'
  }
})

// 请求拦截器 - 为每个请求添加时间戳防止缓存
api.interceptors.request.use(
  config => {
    // 为 GET 请求添加时间戳参数，防止浏览器缓存
    if (config.method === 'get') {
      config.params = {
        ...config.params,
        _t: Date.now()
      }
    }
    return config
  },
  error => {
    return Promise.reject(error)
  }
)

// 响应拦截器
api.interceptors.response.use(
  response => response.data,
  error => {
    console.error('API 请求错误:', error)
    return Promise.reject(error)
  }
)

export default api

// 元数据相关 API
export const metadataApi = {
  // 获取所有元数据列表
  getList: (includeDisabled = false) => api.get('/metadata', { params: { includeDisabled } }),

  // 获取单个元数据详情
  getById: (id: number, options: RequestAbortOptions = {}) =>
    api.get(`/metadata/${id}`, { signal: options.signal }),

  // 创建元数据
  create: (data: any) => api.post('/metadata', data),

  // 更新元数据
  update: (id: number, data: any) => api.put(`/metadata/${id}`, data),

  // 删除元数据
  delete: (id: number) => api.delete(`/metadata/${id}`),

  // 更新排序
  updateSort: (orders: { id: number; sort_order: number }[]) => api.put('/metadata/sort', { orders }),

  // 检查所有接口的字段差异（不执行同步，超时时间延长）
  checkFields: () => api.get('/metadata/check-fields', { timeout: 300000 }),

  // 检查单个接口的字段差异（超时时间延长到 120 秒，因为需要测试 API 最大返回数量）
  checkSingleField: (id: number, options: RequestAbortOptions = {}) =>
    api.get(`/metadata/${id}/check-fields`, { timeout: 120000, signal: options.signal }),

  // 同步单个接口的字段结构
  syncSingleField: (id: number) => api.post(`/metadata/${id}/sync-fields`),

  // 同步所有接口的字段结构
  syncFields: () => api.post('/metadata/sync-fields'),

  // 检测单个接口的实际最大返回数量
  testMaxPageSize: (id: number, options: RequestAbortOptions = {}) =>
    api.post(`/metadata/${id}/test-max-page-size`, {}, { timeout: 60000, signal: options.signal })
}

// 业务数据相关 API
export const dataApi = {
  // 获取业务数据（分页，支持搜索和排序）
  getData: (
    metadataId: number, 
    page: number = 1, 
    pageSize: number = 20, 
    keyword: string = '',
    sortField: string = '',
    sortOrder: string = '',
    searchColumn: string = '',
    exactMatch: boolean = false
  ) => 
    api.get(`/data/${metadataId}`, { params: { page, pageSize, keyword, sortField, sortOrder, searchColumn, exactMatch } }),
  
  // 获取大宗交易细项数据
  getDetail: (metadataId: number, tdate: string, scode: string) =>
    api.get(`/data/${metadataId}/detail`, { params: { tdate, scode } }),
  
  // 获取每日数据统计
  getDailyStats: (metadataId: number) =>
    api.get(`/data/${metadataId}/daily-stats`),
  
  // 清空所有数据和更新日志
  clearAll: () => api.delete('/data/clear-all'),
  
  // 检查所有接口的字段配置
  checkFields: () => api.get('/data/check-fields'),
  
  // 获取接口对应的官方API的所有原始字段
  getRawFields: (metadataId: number) => api.get(`/data/raw-fields/${metadataId}`),
  
  // 获取指定字段的唯一值分布
  getFieldValues: (tableName: string, fieldName: string) => 
    api.get(`/data/field-values/${tableName}/${fieldName}`),

  getExportData: (
    metadataId: number,
    visibleOnly: boolean = true,
    keyword: string = '',
    sortField: string = '',
    sortOrder: string = '',
    searchColumn: string = '',
    exactMatch: boolean = false
  ) =>
    api.get(`/data/${metadataId}/export`, {
      params: { format: 'json', visibleOnly, keyword, sortField, sortOrder, searchColumn, exactMatch },
      timeout: 300000
    })
}

// 更新相关 API
export const updateApi = {
  // 预览更新数据量
  previewUpdate: (metadataId: number) => api.get(`/update/${metadataId}/preview`),

  // 触发增量更新
  triggerUpdate: (metadataId: number, options: RequestAbortOptions = {}) =>
    api.post(`/update/${metadataId}`, undefined, { signal: options.signal }),

  // 触发机构调研统计汇总基础采集（跳过 main_content 详情补充）
  triggerJgdyBasicUpdate: (metadataId: number, options: RequestAbortOptions = {}) =>
    api.post(`/update/${metadataId}?skip_jgdy_main_content=1`, undefined, { signal: options.signal }),

  // 单独补充机构调研统计汇总 main_content
  triggerJgdyMainContentUpdate: (metadataId: number) =>
    api.post(`/update/${metadataId}/jgdy-main-content`),

  // 触发10年全量更新
  triggerFullUpdate: (metadataId: number) => api.post(`/update/${metadataId}?mode=full`),

  // 触发真正全量更新（不带日期过滤器，获取所有历史数据）
  triggerTrueFullUpdate: (metadataId: number) => api.post(`/update/${metadataId}?mode=truefull`),

  // 触发全部更新
  triggerUpdateAll: () => api.post('/update/all'),

  // 停止指定接口的采集
  stopUpdate: (metadataId: number) => api.post(`/update/stop/${metadataId}`),

  // 停止所有采集（包括批量更新）
  stopAll: () => api.post('/update/stop-all')
}

// 更新日志相关 API
export const updateLogApi = {
  // 获取指定接口的更新日志列表
  getLogs: (metadataId: number, page: number = 1, pageSize: number = 20) =>
    api.get(`/update-logs/${metadataId}`, { params: { page, pageSize } }),
  
  // 获取单条更新日志详情
  getLogDetail: (logId: number) =>
    api.get(`/update-logs/detail/${logId}`)
}

export const fieldMappingApi = {
  // 获取指定表的所有字段映射
  getMappings: (tableName: string) => api.get(`/field-mapping/${tableName}`),
  
  // 更新单个字段的中文名、格式化设置、值映射、单位和长文本折叠设置
  updateMapping: (
    tableName: string,
    fieldName: string,
    cnName: string,
    formatAsWanYi?: boolean,
    formatAsTimestamp?: boolean,
    valueMapping?: Record<string, string>,
    unit?: string,
    textClamp?: boolean
  ) =>
    api.put(`/field-mapping/${tableName}/${fieldName}`, {
      cnName,
      formatAsWanYi,
      formatAsTimestamp,
      valueMapping,
      unit,
      textClamp
    }),
  
  // 批量更新字段映射
  batchUpdate: (tableName: string, mappings: Record<string, string>) =>
    api.post(`/field-mapping/${tableName}/batch`, { mappings }),
  
  // 删除单个字段映射
  deleteMapping: (tableName: string, fieldName: string) =>
    api.delete(`/field-mapping/${tableName}/${fieldName}`),
  
  // 获取列顺序
  getColumnOrder: (tableName: string) =>
    api.get(`/field-mapping/${tableName}/column-order`),
  
  // 保存列顺序
  saveColumnOrder: (tableName: string, order: string[]) =>
    api.put(`/field-mapping/${tableName}/column-order`, { order }),
  
  // 获取隐藏的列
  getHiddenColumns: (tableName: string) =>
    api.get(`/field-mapping/${tableName}/hidden-columns`),
  
  // 获取隐藏列的详细信息
  getHiddenColumnsDetail: (tableName: string) =>
    api.get(`/field-mapping/${tableName}/hidden-columns-detail`),
  
  // 设置列的隐藏状态
  setColumnHidden: (tableName: string, fieldName: string, hidden: boolean) =>
    api.put(`/field-mapping/${tableName}/hide-column`, { fieldName, hidden }),
  
  // 导出所有字段映射
  exportAll: () => api.get('/field-mapping/export/all'),
  
  // 导入字段映射
  importAll: (mappings: any, mode: 'merge' | 'overwrite' = 'merge') =>
    api.post('/field-mapping/import/all', { mappings, mode }),
  
  // 删除所有字段映射
  deleteAll: () => api.delete('/field-mapping/delete/all')
}

export const dataComboApi = {
  getList: () => api.get('/data-combo'),

  getMetadataOptions: () => api.get('/data-combo/options/metadata'),

  getById: (id: string) => api.get(`/data-combo/${id}`),

  getData: (
    id: string,
    page: number = 1,
    pageSize: number = 20,
    keyword: string = '',
    sortField: string = '',
    sortOrder: 'asc' | 'desc' | '' = ''
  ) => api.get(`/data-combo/${id}/data`, { params: { page, pageSize, keyword, sortField, sortOrder } }),

  getRow: (id: string, code: string) => api.get(`/data-combo/${id}/row/${encodeURIComponent(code)}`),

  create: (data: any) => api.post('/data-combo', data),

  delete: (id: string) => api.delete(`/data-combo/${id}`)
}
