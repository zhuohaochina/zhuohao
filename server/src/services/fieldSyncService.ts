/**
 * 字段同步服务
 * 负责检查和同步 API 返回字段与数据库表结构
 */
import axios from 'axios';
import * as cheerio from 'cheerio';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { HttpProxyAgent } from 'http-proxy-agent';
import { metadataService } from './metadataService';
import { tableService } from './tableService';
import { getDatabase, saveDatabase } from './database';
import { ApiMetadata, ApiRequestConfig, RelativeDateRule } from '../models/types';
import { calculateDateScope, formatDateOnly } from '../utils/dateScope';

interface DateScopeOptions {
  dateRange?: number;
  dateRangeMode?: 'offset_days' | 'count_including_today';
  dateUnit?: 'calendar_day' | 'trading_day';
  closedDateRanges?: Array<{ begin: string; end: string }>;
  futureDays?: number;
  fixedBeginDate?: string | null;
  relativeDateRule?: RelativeDateRule | null;
}

/**
 * 代理配置
 * 使用 Clash Verge 的本地代理
 */
const PROXY_CONFIG = {
  enabled: true,  // 是否启用代理
  host: '127.0.0.1',
  port: 7897
};

// 创建代理 agent
const proxyUrl = `http://${PROXY_CONFIG.host}:${PROXY_CONFIG.port}`;
const httpsAgent = PROXY_CONFIG.enabled ? new HttpsProxyAgent(proxyUrl) : undefined;
const httpAgent = PROXY_CONFIG.enabled ? new HttpProxyAgent(proxyUrl) : undefined;

interface FieldCheckResult {
  id: number;
  cn_name: string;
  table_name: string;
  request_method: string;   // 请求方法
  actualParams: Record<string, any>;  // 字段同步时的请求参数（取1条）
  crawlerParams: Record<string, any>; // 实际采集时的请求参数（取几百条）
  actualUrl: string;        // 实际请求的URL
  maxPageSize: number | null; // API 实际最大返回数量
  status: 'ok' | 'diff' | 'skip' | 'error';
  message: string;
  apiFields: string[];
  dbFields: string[];
  addedFields: string[];    // API 有但数据库没有
  removedFields: string[];  // 数据库有但 API 没有
  synced?: boolean;         // 是否已同步
  // 采集统计信息
  crawlStats?: {
    totalBatches: number;      // 日期批次总数
    dateRange: string;         // 日期范围
    apiTotalRecord: number | null;  // API 返回的总记录数（单批次）
    hasMorePages: boolean;     // 是否有更多页（数据可能不完整）
    estimatedTotal: number | null;  // 预估总数据量
  };
}

interface SyncResult {
  summary: {
    total: number;
    ok: number;
    diff: number;
    skip: number;
    synced: number;
    failed: number;
  };
  details: FieldCheckResult[];
}

interface FetchResult {
  data: any[];
  actualParams: Record<string, any>;
  crawlerParams: Record<string, any>;
  actualUrl: string;
  maxPageSize: number | null; // API 实际最大返回数量
  // 采集统计信息
  crawlStats?: {
    totalBatches: number;      // 日期批次总数
    dateRange: string;         // 日期范围
    apiTotalRecord: number | null;  // API 返回的总记录数（单批次）
    hasMorePages: boolean;     // 是否有更多页（数据可能不完整）
    estimatedTotal: number | null;  // 预估总数据量
  };
}

/**
 * 字段同步服务类
 */
class FieldSyncService {
  private getManagedExtraFields(metadata: ApiMetadata): string[] {
    if (metadata.table_name === 'biz_eastmoney_jgdy_summary') {
      return ['main_content'];
    }

    return [];
  }

  /**
   * 检查所有接口的字段差异（不执行同步）
   */
  async checkAllFields(): Promise<SyncResult> {
    const metadataList = await metadataService.getMetadataList();
    const results: FieldCheckResult[] = [];

    for (const metadata of metadataList) {
      try {
        const result = await this.checkSingleInterface(metadata);
        results.push(result);
      } catch (error) {
        results.push({
          id: metadata.id,
          cn_name: metadata.cn_name,
          table_name: metadata.table_name,
          request_method: metadata.request_method || 'GET',
          actualParams: {},
          crawlerParams: {},
          actualUrl: '',
          maxPageSize: null,
          status: 'error',
          message: `检查失败: ${(error as Error).message}`,
          apiFields: [],
          dbFields: [],
          addedFields: [],
          removedFields: []
        });
      }
    }

    const okCount = results.filter(r => r.status === 'ok').length;
    const diffCount = results.filter(r => r.status === 'diff').length;
    const skipCount = results.filter(r => r.status === 'skip').length;
    const errorCount = results.filter(r => r.status === 'error').length;

    return {
      summary: {
        total: results.length,
        ok: okCount,
        diff: diffCount,
        skip: skipCount,
        synced: 0,
        failed: errorCount
      },
      details: results
    };
  }

  /**
   * 检查并同步所有接口的字段结构
   */
  async syncAllFields(): Promise<SyncResult> {
    const metadataList = await metadataService.getMetadataList();
    const results: FieldCheckResult[] = [];

    for (const metadata of metadataList) {
      try {
        const checkResult = await this.checkSingleInterface(metadata);

        if (checkResult.status === 'diff') {
          // 需要同步：重建表
          await this.rebuildTable(metadata, checkResult.apiFields);
          checkResult.synced = true;
          checkResult.message = `已同步：删除 ${checkResult.removedFields.length} 个字段，新增 ${checkResult.addedFields.length} 个字段`;
        }

        results.push(checkResult);
      } catch (error) {
        results.push({
          id: metadata.id,
          cn_name: metadata.cn_name,
          table_name: metadata.table_name,
          request_method: metadata.request_method || 'GET',
          actualParams: {},
          crawlerParams: {},
          actualUrl: '',
          maxPageSize: null,
          status: 'error',
          message: `同步失败: ${(error as Error).message}`,
          apiFields: [],
          dbFields: [],
          addedFields: [],
          removedFields: [],
          synced: false
        });
      }
    }

    const okCount = results.filter(r => r.status === 'ok').length;
    const diffCount = results.filter(r => r.status === 'diff').length;
    const skipCount = results.filter(r => r.status === 'skip').length;
    const syncedCount = results.filter(r => r.synced === true).length;
    const errorCount = results.filter(r => r.status === 'error').length;

    return {
      summary: {
        total: results.length,
        ok: okCount,
        diff: diffCount,
        skip: skipCount,
        synced: syncedCount,
        failed: errorCount
      },
      details: results
    };
  }

  /**
   * 检查单个接口的字段差异（公开方法，供外部调用）
   */
  async checkSingleField(id: number): Promise<FieldCheckResult & { sampleData?: any }> {
    const metadata = await metadataService.getMetadataById(id);
    if (!metadata) {
      throw new Error(`接口 ID ${id} 不存在`);
    }
    const result = await this.checkSingleInterface(metadata);
    return result;
  }

  /**
   * 同步单个接口的字段结构（公开方法，供外部调用）
   */
  async syncSingleField(id: number): Promise<FieldCheckResult & { sampleData?: any }> {
    const metadata = await metadataService.getMetadataById(id);
    if (!metadata) {
      throw new Error(`接口 ID ${id} 不存在`);
    }

    const checkResult = await this.checkSingleInterface(metadata);

    if (checkResult.status === 'diff') {
      // 需要同步：重建表
      await this.rebuildTable(metadata, checkResult.apiFields);
      checkResult.synced = true;
      checkResult.status = 'ok';
      checkResult.message = `已同步：删除 ${checkResult.removedFields.length} 个字段，新增 ${checkResult.addedFields.length} 个字段`;

      // 同步后重新获取数据库字段（按字母顺序排序）
      const managedExtraFieldSet = new Set(this.getManagedExtraFields(metadata).map(field => field.toLowerCase()));
      checkResult.dbFields = tableService.getTableColumns(metadata.table_name)
        .filter(col => !['id', 'metadata_id', 'created_at', 'collected_at'].includes(col))
        .filter(col => !managedExtraFieldSet.has(col.toLowerCase()))
        .sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
      checkResult.addedFields = [];
      checkResult.removedFields = [];
    }

    return checkResult;
  }

  /**
   * 检查单个接口的字段差异
   * 注意：字段比较时忽略大小写，因为 API 返回的字段可能是大写，而数据库存储的是小写
   */
  private async checkSingleInterface(metadata: ApiMetadata): Promise<FieldCheckResult & { sampleData?: any }> {
    const managedExtraFields = this.getManagedExtraFields(metadata);
    const managedExtraFieldSet = new Set(managedExtraFields.map(field => field.toLowerCase()));

    // 获取数据库表的字段（排除系统字段）
    const dbFields = tableService.getTableColumns(metadata.table_name)
      .filter(col => !['id', 'metadata_id', 'created_at', 'collected_at'].includes(col))
      .filter(col => !managedExtraFieldSet.has(col.toLowerCase()))
      .sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));  // 按字母顺序排序

    // 尝试获取 API 返回的字段
    let apiFields: string[];
    let actualParams: Record<string, any> = {};
    let crawlerParams: Record<string, any> = {};
    let actualUrl: string = '';
    let maxPageSize: number | null = null;
    let sampleData: any = null;
    let crawlStats: FieldCheckResult['crawlStats'] = undefined;
    try {
      const fetchResult = await this.fetchApiFields(metadata);
      apiFields = fetchResult.data && fetchResult.data.length > 0 && typeof fetchResult.data[0] === 'object'
        ? Object.keys(fetchResult.data[0]).sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()))  // 按字母顺序排序
        : [];
      actualParams = fetchResult.actualParams;
      crawlerParams = fetchResult.crawlerParams;
      actualUrl = fetchResult.actualUrl;
      maxPageSize = fetchResult.maxPageSize;
      sampleData = fetchResult.data && fetchResult.data.length > 0 ? fetchResult.data[0] : null;
      crawlStats = fetchResult.crawlStats;

      if (apiFields.length === 0) {
        throw new Error('API 返回数据为空');
      }
      console.log(`[FieldSync] ${metadata.cn_name} 获取到 ${apiFields.length} 个字段`);
    } catch (error) {
      // 如果无法获取 API 字段，标记为跳过
      return {
        id: metadata.id,
        cn_name: metadata.cn_name,
        table_name: metadata.table_name,
        request_method: metadata.request_method || 'GET',
        actualParams,
        crawlerParams,
        actualUrl,
        maxPageSize: null,
        status: 'skip',
        message: `跳过：${(error as Error).message}`,
        apiFields: [],
        dbFields,
        addedFields: [],
        removedFields: [],
        crawlStats
      };
    }

    // 比较差异（忽略大小写）
    // 注意：API 返回的 id 字段会被重命名为 api_id
    // 将数据库字段转为小写集合，用于快速查找
    const dbFieldsLower = new Set(dbFields.map(f => f.toLowerCase()));
    // 将 API 字段转为小写集合，用于快速查找（id -> api_id）
    const apiFieldsLower = new Set(apiFields.map(f => f.toLowerCase() === 'id' ? 'api_id' : f.toLowerCase()));

    // 找出 API 有但数据库没有的字段（忽略大小写，考虑 id -> api_id 映射）
    const addedFields = apiFields.filter(f => {
      const mappedName = f.toLowerCase() === 'id' ? 'api_id' : f.toLowerCase();
      return !dbFieldsLower.has(mappedName);
    });
    // 找出数据库有但 API 没有的字段（忽略大小写，考虑 id -> api_id 映射）
    const removedFields = dbFields.filter(f => !apiFieldsLower.has(f.toLowerCase()));

    // 检查是否为特殊接口（API返回的字段经过处理/展开）
    const tableName = metadata.table_name;
    const isProcessedApi = tableName.includes('announcement') ||
                           tableName === 'sse_ehudong' ||
                           tableName === 'sse_ehudong2';

    if (addedFields.length === 0 && removedFields.length === 0) {
      let message = '字段一致';
      if (isProcessedApi) {
        message = '字段一致（注：API返回字段经过解析展开，非原始字段）';
      }
      if (managedExtraFields.length > 0) {
        message += `（已忽略本地派生字段 ${managedExtraFields.join(', ')}）`;
      }
      return {
        id: metadata.id,
        cn_name: metadata.cn_name,
        table_name: metadata.table_name,
        request_method: metadata.request_method || 'GET',
        actualParams,
        crawlerParams,
        actualUrl,
        maxPageSize,
        status: 'ok',
        message,
        apiFields,
        dbFields,
        addedFields: [],
        removedFields: [],
        sampleData,
        crawlStats
      };
    }

    let message = `发现差异：新增 ${addedFields.length} 个，删除 ${removedFields.length} 个`;
    if (isProcessedApi) {
      message += '（注：API返回字段经过解析展开，非原始字段）';
    }
    if (managedExtraFields.length > 0) {
      message += `（已忽略本地派生字段 ${managedExtraFields.join(', ')}）`;
    }

    return {
      id: metadata.id,
      cn_name: metadata.cn_name,
      table_name: metadata.table_name,
      request_method: metadata.request_method || 'GET',
      actualParams,
      crawlerParams,
      actualUrl,
      maxPageSize,
      status: 'diff',
      message,
      apiFields,
      dbFields,
      addedFields,
      removedFields,
      sampleData,
      crawlStats
    };
  }

  /**
   * 从数据库获取 API 请求配置
   */
  private getApiRequestConfig(metadata: ApiMetadata): ApiRequestConfig | null {
    if (!metadata.datacenter_config) return null;
    try {
      return JSON.parse(metadata.datacenter_config);
    } catch {
      return null;
    }
  }

  private getDateScopeOptions(metadata?: ApiMetadata, config?: ApiRequestConfig): DateScopeOptions {
    return {
      dateRange: metadata?.date_range ?? undefined,
      dateRangeMode: config?.dateConfig?.dateRangeMode,
      dateUnit: config?.dateConfig?.dateUnit,
      closedDateRanges: config?.dateConfig?.closedDateRanges,
      futureDays: (metadata as any)?.future_days ?? undefined,
      fixedBeginDate: (metadata as any)?.fixed_begin_date ?? null,
      relativeDateRule: config?.dateConfig?.relativeDateRule ?? null
    };
  }

  private calculateDateRangeForMetadata(metadata?: ApiMetadata, config?: ApiRequestConfig): { begin: string; end: string } | null {
    if (!metadata) {
      return null;
    }

    const scope = calculateDateScope(this.getDateScopeOptions(metadata, config));
    if (scope.noDateFilter) {
      return null;
    }

    return {
      begin: formatDateOnly(scope.startDate),
      end: formatDateOnly(scope.endDate)
    };
  }

  private shouldUseConfiguredPageSizeForSensitiveApi(config: ApiRequestConfig): boolean {
    const reportName = String(config.params?.reportName || '').toUpperCase();
    return (
      config.apiType === 'announcement' ||
      config.apiType === 'hudongyi' ||
      config.apiType === 'report' ||
      config.apiType === 'fund' ||
      reportName === 'RPTA_WEB_RZRQ_GGMX' ||
      reportName === 'RPT_SHARE_HOLDER_INCREASE' ||
      reportName === 'RPT_EXECUTIVE_HOLD_DETAILS' ||
      reportName === 'RPT_SEO_DETAIL' ||
      reportName === 'RPT_HOLDERNUMLATEST'
    );
  }

  private isJgdySummaryConfig(config: ApiRequestConfig): boolean {
    return String(config.params?.reportName || '').toUpperCase() === 'RPT_ORG_SURVEYNEW';
  }

  /**
   * 计算采集统计信息
   */
  private calculateCrawlStats(
    metadata: ApiMetadata,
    config: ApiRequestConfig,
    maxPageSize: number | null,
    apiTotalRecord: number | null
  ): FetchResult['crawlStats'] {
    const loopStrategy = config.loopStrategy;
    const dateBatch = loopStrategy?.dateBatch;
    const batchDays = dateBatch?.batchDays || 1;
    const crawlerPageSize = maxPageSize || config.pagination?.crawlerPageSize || 500;

    // 计算日期范围
    let dateRangeStr = '-';
    let totalBatches = 1;

    // 从 displayConfig 或 metadata 获取日期范围
    const displayConfig = config.displayConfig;
    const configuredRange = this.calculateDateRangeForMetadata(metadata, config);

    if (configuredRange) {
      const startDate = new Date(configuredRange.begin);
      const endDate = new Date(configuredRange.end);
      dateRangeStr = `${configuredRange.begin} ~ ${configuredRange.end}`;
      const days = Math.ceil((endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24)) + 1;
      totalBatches = Math.ceil(days / batchDays);
    } else if (displayConfig?.fixedDateRange) {
      // metadata 未配置日期时，降级使用 displayConfig 的固定日期范围
      const { begin, end } = displayConfig.fixedDateRange;
      dateRangeStr = `${begin} ~ ${end}`;
      const startDate = new Date(begin);
      const endDate = new Date(end);
      const days = Math.ceil((endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24)) + 1;
      totalBatches = Math.ceil(days / batchDays);
    }

    // 判断是否有更多页（测试批次数据可能不完整）
    const hasMorePages = apiTotalRecord !== null && apiTotalRecord > crawlerPageSize;

    return {
      totalBatches,
      dateRange: dateRangeStr,
      apiTotalRecord,
      hasMorePages,
      estimatedTotal: null  // 不再预估，因为每批数据量差异大
    };
  }

  /**
   * 通用 API 请求方法
   * 根据数据库配置发送请求，获取1条数据用于字段检查
   */
  private async fetchByConfig(
    metadata: ApiMetadata,
    headers: Record<string, string>,
    config: ApiRequestConfig
  ): Promise<FetchResult> {
    const apiType = config.apiType;

    switch (apiType) {
      case 'datacenter':
        return this.fetchDatacenterByConfig(metadata, headers, config);
      case 'report':
        return this.fetchReportByConfig(metadata, headers, config);
      case 'fund':
        return this.fetchFundByConfig(metadata, headers, config);
      case 'stock_jsonp':
        return this.fetchStockJsonpByConfig(metadata, headers, config);
      case 'announcement':
        return this.fetchAnnouncementByConfig(metadata, headers, config);
      case 'hudongyi':
        return this.fetchHudongyiByConfig(metadata, headers, config);
      case 'sse_hudong':
        return this.fetchSseHudongByConfig(metadata, headers, config);
      default:
        throw new Error(`未知的 API 类型: ${apiType}`);
    }
  }

  /**
   * 测试 API 实际最大返回数量
   * 通过请求不同的 pageSize 来检测 API 的实际限制
   * requestFn 返回 { data: any[], total: number }
   * 优化：默认只测试 5000，减少请求次数加快速度
   *
   * 返回值：
   * - maxPageSize: 检测到的最大返回数量
   * - isConfirmed: 是否是确定的限制（true=API有明确限制，false=数据量不足无法确定）
   * - total: API 返回的总数
   */
  private async testMaxPageSize(
    requestFn: (pageSize: number) => Promise<{ data: any[], total: number }>,
    testSizes: number[] = [5000]
  ): Promise<{ maxPageSize: number; isConfirmed: boolean; total: number } | null> {
    let maxSize = 0;
    let lastTotal = 0;
    let successCount = 0;
    let maxSuccessfulRequestedSize = 0;
    const errors: string[] = [];

    for (const size of testSizes) {
      try {
        const result = await requestFn(size);
        const actualCount = result.data.length;
        const total = result.total;
        lastTotal = total;
        successCount += 1;
        maxSuccessfulRequestedSize = Math.max(maxSuccessfulRequestedSize, size);

        if (actualCount > maxSize) {
          maxSize = actualCount;
        }

        // 如果返回数量小于请求数量，且总数大于返回数量，说明是 API 限制
        // 如果返回数量小于请求数量，但总数等于返回数量，说明是数据本身就少
        if (actualCount < size) {
          if (total > actualCount) {
            // API 有限制，这是确定的
            console.log(`[FieldSync] API 最大返回数量: ${maxSize} (总数: ${total}) - 已确认`);
            return { maxPageSize: maxSize, isConfirmed: true, total };
          } else {
            // 数据本身就少，无法确定真实限制
            console.log(`[FieldSync] 数据量较少 (${actualCount}/${total})，无法确定真实限制`);
          }
        }
      } catch (error) {
        const message = (error as Error).message;
        errors.push(`[size=${size}] ${message}`);
        console.warn(`[FieldSync] 测试 pageSize=${size} 失败: ${message}`);
      }

      // 短暂延迟避免请求过快
      await new Promise(r => setTimeout(r, 200));
    }

    if (successCount === 0) {
      const reason = errors.length > 0 ? errors.join('; ') : '未知错误';
      console.log(`[FieldSync] 测试最大返回数量失败: ${reason}`);
      return null;
    }

    // 如果所有测试都通过（返回数量 >= 请求数量），说明限制至少是测试的最大值
    // 如果数据量不足，返回未确认的结果
    if (maxSize < maxSuccessfulRequestedSize) {
      // 数据量不足，无法确定
      console.log(`[FieldSync] 数据量不足 (${maxSize}/${lastTotal})，无法确定真实限制`);
      return { maxPageSize: maxSize, isConfirmed: false, total: lastTotal };
    }

    console.log(`[FieldSync] API 最大返回数量: >=${maxSuccessfulRequestedSize}`);
    return { maxPageSize: maxSize, isConfirmed: true, total: lastTotal };
  }

  /**
   * 东方财富 datacenter API
   */
  private async fetchDatacenterByConfig(
    metadata: ApiMetadata,
    headers: Record<string, string>,
    config: ApiRequestConfig
  ): Promise<FetchResult> {
    const actualUrl = config.baseUrl;
    const fieldSyncPageSize = config.pagination.fieldSyncPageSize;
    const requestTimeoutMs = 60000;

    const params: Record<string, any> = {
      ...config.params,
      [config.pagination.pageSizeParam]: fieldSyncPageSize,
      [config.pagination.pageNumParam]: 1
    };

    const response = await axios.get(actualUrl, { headers, params, timeout: 30000, httpsAgent: httpsAgent, httpAgent: httpAgent });
    const data = this.extractData(response.data, config.dataPath);

    // 测试最大返回数量（使用完整参数，但不带 filter）
    const maxPageSizeResult = await this.testMaxPageSize(async (pageSize: number) => {
      const testParams: Record<string, any> = {
        ...config.params,
        [config.pagination.pageSizeParam]: pageSize,
        [config.pagination.pageNumParam]: 1
      };
      // 移除 filter 参数，确保有足够数据来测试
      delete testParams.filter;
      const testResponse = await axios.get(actualUrl, { headers, params: testParams, timeout: 30000, httpsAgent: httpsAgent, httpAgent: httpAgent });
      const testData = this.extractData(testResponse.data, config.dataPath);
      const total = testResponse.data?.result?.count || 0;
      return { data: testData, total };
    });
    const maxPageSize = maxPageSizeResult?.maxPageSize ?? null;

    // 实际采集时的参数（使用检测到的 maxPageSize）
    const crawlerParams: Record<string, any> = {
      ...config.params,
      [config.pagination.pageSizeParam]: maxPageSize || config.pagination.crawlerPageSize || 500,
      [config.pagination.pageNumParam]: 1
    };

    return { data, actualParams: params, crawlerParams, actualUrl, maxPageSize };
  }

  /**
   * 东方财富研报 API
   */
  private async fetchReportByConfig(
    metadata: ApiMetadata,
    headers: Record<string, string>,
    config: ApiRequestConfig
  ): Promise<FetchResult> {
    const actualUrl = config.baseUrl;
    const fieldSyncPageSize = config.pagination.fieldSyncPageSize;
    const requestTimeoutMs = 60000;

    // 字段同步用的日期范围
    const now = new Date();
    const fieldSyncDays = config.dateConfig?.fieldSyncDays || 30;
    const fieldSyncEndTime = now.toISOString().split('T')[0];
    const fieldSyncStartDate = new Date(now);
    fieldSyncStartDate.setDate(fieldSyncStartDate.getDate() - fieldSyncDays);
    const fieldSyncBeginTime = fieldSyncStartDate.toISOString().split('T')[0];

    // 实际采集用的日期范围（使用 metadata.date_range）
    const crawlerDays = metadata.date_range || 30;
    const crawlerEndDate = new Date(now);
    crawlerEndDate.setDate(crawlerEndDate.getDate() + (metadata.future_days || 0));
    const crawlerEndTime = crawlerEndDate.toISOString().split('T')[0];
    const crawlerStartDate = new Date(now);
    crawlerStartDate.setDate(crawlerStartDate.getDate() - crawlerDays);
    const crawlerBeginTime = crawlerStartDate.toISOString().split('T')[0];

    const params: Record<string, any> = {
      ...config.params,
      [config.dateConfig?.beginParam || 'beginTime']: fieldSyncBeginTime,
      [config.dateConfig?.endParam || 'endTime']: fieldSyncEndTime,
      [config.pagination.pageNumParam]: 1,
      [config.pagination.pageSizeParam]: fieldSyncPageSize,
      _: Date.now()
    };

    const response = await axios.get(actualUrl, { headers, params, timeout: requestTimeoutMs });
    const data = this.extractData(response.data, config.dataPath);

    const maxPageSize = config.pagination.crawlerPageSize || 100;

    // 实际采集时的参数（使用检测到的 maxPageSize）
    const crawlerParams: Record<string, any> = {
      ...config.params,
      [config.dateConfig?.beginParam || 'beginTime']: crawlerBeginTime,
      [config.dateConfig?.endParam || 'endTime']: crawlerEndTime,
      [config.pagination.pageNumParam]: 1,
      [config.pagination.pageSizeParam]: maxPageSize || config.pagination.crawlerPageSize || 100,
      _: Date.now()
    };

    return { data, actualParams: params, crawlerParams, actualUrl, maxPageSize };
  }

  /**
   * 东方财富基金持仓 API
   */
  private async fetchFundByConfig(
    metadata: ApiMetadata,
    headers: Record<string, string>,
    config: ApiRequestConfig
  ): Promise<FetchResult> {
    const actualUrl = config.baseUrl;
    const fieldSyncPageSize = config.pagination.fieldSyncPageSize;

    const params: Record<string, any> = {
      ...config.params,
      [config.pagination.pageNumParam]: 1,
      [config.pagination.pageSizeParam]: fieldSyncPageSize
    };

    const response = await axios.get(actualUrl, { headers, params, timeout: 30000, httpsAgent: httpsAgent, httpAgent: httpAgent });
    const data = this.extractData(response.data, config.dataPath);

    // 测试最大返回数量
    const maxPageSizeResult = await this.testMaxPageSize(async (pageSize: number) => {
      const testParams = {
        ...config.params,
        [config.pagination.pageNumParam]: 1,
        [config.pagination.pageSizeParam]: pageSize
      };
      const testResponse = await axios.get(actualUrl, { headers, params: testParams, timeout: 30000, httpsAgent: httpsAgent, httpAgent: httpAgent });
      const testData = this.extractData(testResponse.data, config.dataPath);
      // 基金持仓 API 返回 pages（总页数），需要计算总数
      const pages = testResponse.data?.pages || 0;
      const total = pages > 0 ? pages * pageSize : (testResponse.data?.TotalCount || testResponse.data?.result?.count || 0);
      return { data: testData, total };
    });
    const maxPageSize = maxPageSizeResult?.maxPageSize ?? null;

    // 实际采集时的参数（使用检测到的 maxPageSize）
    const crawlerParams: Record<string, any> = {
      ...config.params,
      [config.pagination.pageNumParam]: 1,
      [config.pagination.pageSizeParam]: maxPageSize || config.pagination.crawlerPageSize || 500
    };

    return { data, actualParams: params, crawlerParams, actualUrl, maxPageSize };
  }

  /**
   * 东方财富 A股列表 JSONP
   * 使用 Puppeteer 浏览器采集，绕过 TLS 指纹检测
   */
  private async fetchStockJsonpByConfig(
    metadata: ApiMetadata,
    headers: Record<string, string>,
    config: ApiRequestConfig
  ): Promise<FetchResult> {
    const actualUrl = config.baseUrl;
    const fieldSyncPageSize = config.pagination.fieldSyncPageSize;

    // 动态导入浏览器采集服务
    const { browserCrawlerService } = await import('./browserCrawler');

    const params: Record<string, any> = {
      ...config.params,
      [config.pagination.pageNumParam]: 1,
      [config.pagination.pageSizeParam]: fieldSyncPageSize
    };

    // 使用浏览器采集
    const result = await browserCrawlerService.fetchStockList(config.params || {}, 1, fieldSyncPageSize);
    const data = result.data;

    // 测试最大返回数量
    const maxPageSizeResult = await this.testMaxPageSize(async (pageSize: number) => {
      const testResult = await browserCrawlerService.fetchStockList(config.params || {}, 1, pageSize);
      return { data: testResult.data, total: testResult.total };
    });
    const maxPageSize = maxPageSizeResult?.maxPageSize ?? null;

    // 实际采集时的参数（使用检测到的 maxPageSize）
    const crawlerParams: Record<string, any> = {
      ...config.params,
      cb: 'jQuery',
      [config.pagination.pageNumParam]: 1,
      [config.pagination.pageSizeParam]: maxPageSize || config.pagination.crawlerPageSize || 5000
    };

    return { data, actualParams: params, crawlerParams, actualUrl, maxPageSize };
  }

  /**
   * 东方财富公告 API
   */
  private async fetchAnnouncementByConfig(
    metadata: ApiMetadata,
    headers: Record<string, string>,
    config: ApiRequestConfig
  ): Promise<FetchResult> {
    const actualUrl = config.baseUrl;
    const fieldSyncPageSize = config.pagination.fieldSyncPageSize;

    // 字段同步用的日期范围（只取1天）
    const now = new Date();
    const fieldSyncDays = config.dateConfig?.fieldSyncDays || 1;
    const fieldSyncEndDate = new Date(now);
    const fieldSyncStartDate = new Date(now);
    fieldSyncStartDate.setDate(fieldSyncStartDate.getDate() - fieldSyncDays + 1);

    // 获取循环策略配置
    const loopStrategy = config.loopStrategy;
    const isDoubleLoop = loopStrategy?.type === 'double';
    const batchDays = loopStrategy?.dateBatch?.batchDays || 10;

    // 实际采集用的日期范围
    // 如果是双层循环，显示每批的日期范围（batchDays）
    // 如果是单层循环，显示整个采集范围（date_range）
    const crawlerEndDate = new Date(now);
    crawlerEndDate.setDate(crawlerEndDate.getDate() + (metadata.future_days || 0));
    const crawlerStartDate = new Date(crawlerEndDate);
    if (isDoubleLoop) {
      // 双层循环：显示每批的日期范围
      crawlerStartDate.setDate(crawlerStartDate.getDate() - batchDays + 1);
    } else {
      // 单层循环：显示整个采集范围
      const crawlerDays = metadata.date_range || 7;
      crawlerStartDate.setDate(crawlerStartDate.getDate() - crawlerDays);
    }

    const params: Record<string, any> = {
      ...config.params,
      [config.pagination.pageNumParam]: 1,
      [config.pagination.pageSizeParam]: fieldSyncPageSize,
      [config.dateConfig?.beginParam || 'begin_time']: fieldSyncStartDate.toISOString().split('T')[0],
      [config.dateConfig?.endParam || 'end_time']: fieldSyncEndDate.toISOString().split('T')[0]
    };

    const response = await axios.get(actualUrl, { headers, params, timeout: 30000, httpsAgent: httpsAgent, httpAgent: httpAgent });

    let data: any[] = [];
    const list = this.extractData(response.data, config.dataPath || 'data.list');
    if (Array.isArray(list) && list.length > 0) {
      // 转换数据格式
      data = list.map((item: any) => ({
        art_code: item.art_code,
        title: item.title || '',
        title_ch: item.title_ch || '',
        title_en: item.title_en || '',
        notice_date: item.notice_date?.split(' ')[0] || '',
        display_time: item.display_time || '',
        ei_time: item.eiTime || '',
        sort_date: item.sort_date || '',
        stock_code: item.codes?.[0]?.stock_code || '',
        stock_name: item.codes?.[0]?.short_name || '',
        market_code: item.codes?.[0]?.market_code || '',
        ann_type: item.codes?.[0]?.ann_type || '',
        column_code: item.columns?.[0]?.column_code || '',
        column_name: item.columns?.[0]?.column_name || '',
        source_type: item.source_type || '',
        language: item.language || '',
        listing_state: item.listing_state || '',
        product_code: item.product_code || '',
        pdf_url: item.art_code ? `https://pdf.dfcfw.com/pdf/H2_${item.art_code}_1.pdf` : ''
      }));
    }

    // 公告接口对大页/宽日期范围很敏感；字段比对阶段使用采集配置值，避免额外触发限流。
    const maxPageSize = config.pagination?.crawlerPageSize || 100;

    // 实际采集时的参数（使用检测到的 maxPageSize）
    const crawlerParams: Record<string, any> = {
      ...config.params,
      [config.pagination.pageNumParam]: 1,
      [config.pagination.pageSizeParam]: maxPageSize || config.pagination.crawlerPageSize || 100,
      [config.dateConfig?.beginParam || 'begin_time']: crawlerStartDate.toISOString().split('T')[0],
      [config.dateConfig?.endParam || 'end_time']: crawlerEndDate.toISOString().split('T')[0]
    };

    return { data, actualParams: params, crawlerParams, actualUrl, maxPageSize };
  }

  /**
   * 检测指定接口的实际最大返回数量（公开方法，供 API 调用）
   */
  async testMaxPageSizeForMetadata(id: number): Promise<{
    maxPageSize: number | null;
    isConfirmed: boolean;
    total: number;
    message: string;
  }> {
    const metadata = await metadataService.getMetadataById(id);
    if (!metadata) {
      throw new Error(`接口 ID ${id} 不存在`);
    }

    const config = this.getApiRequestConfig(metadata);
    if (!config) {
      throw new Error('缺少 datacenter_config 配置');
    }

    const headers: Record<string, string> = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      'Accept': 'application/json, text/plain, */*',
      'Referer': metadata.source_url || ''
    };

    const result = await this.getMaxPageSizeForConfig(metadata, headers, config);

    if (result === null) {
      return {
        maxPageSize: null,
        isConfirmed: false,
        total: 0,
        message: '检测失败：可能触发风控/限流或网络抖动，请稍后重试'
      };
    }

    if (result.isConfirmed) {
      return {
        maxPageSize: result.maxPageSize,
        isConfirmed: true,
        total: result.total,
        message: `${result.maxPageSize} 条/页`
      };
    } else {
      // 数据量不足，无法确定真实限制
      return {
        maxPageSize: result.maxPageSize,
        isConfirmed: false,
        total: result.total,
        message: `数据量不足 (${result.total}条)，无法确定`
      };
    }
  }

  /**
   * 获取指定配置的实际最大返回数量
   */
  private async getMaxPageSizeForConfig(
    metadata: ApiMetadata,
    headers: Record<string, string>,
    config: ApiRequestConfig
  ): Promise<{ maxPageSize: number; isConfirmed: boolean; total: number } | null> {
    if (config.apiType === 'stock_jsonp') {
      const { browserCrawlerService } = await import('./browserCrawler');
      return this.testMaxPageSize(async (pageSize: number) => {
        const result = await browserCrawlerService.fetchStockList(config.params || {}, 1, pageSize);
        return { data: result.data, total: result.total };
      });
    }

    if (this.shouldUseConfiguredPageSizeForSensitiveApi(config)) {
      return {
        maxPageSize: config.pagination?.crawlerPageSize || config.pagination?.fieldSyncPageSize || 100,
        isConfirmed: true,
        total: 0
      };
    }

    if (this.isJgdySummaryConfig(config)) {
      return {
        maxPageSize: config.pagination?.crawlerPageSize || config.pagination?.fieldSyncPageSize || 500,
        isConfirmed: true,
        total: 0
      };
    }

    if (config.apiType === 'sse_hudong') {
      return {
        maxPageSize: 10,
        isConfirmed: true,
        total: 0
      };
    }

    return this.testMaxPageSize(async (pageSize: number) => {
      const testParams = this.buildTestParamsWithDateScope(config, pageSize, 1, metadata, false);
      const response = await axios.get(config.baseUrl, { headers, params: testParams, timeout: 30000, httpsAgent: httpsAgent, httpAgent: httpAgent });
      const data = this.extractData(response.data, config.dataPath || 'data.list');
      const total = this.extractTotal(response.data, config, pageSize);
      return { data, total };
    });
  }

  /**
   * 构建测试参数（可选择使用宽口径日期范围或当前接口实际日期范围）
   */
  private buildTestParamsWithDateScope(
    config: ApiRequestConfig,
    pageSize: number,
    page: number,
    metadata?: ApiMetadata,
    useWideDateRange: boolean = true
  ): Record<string, any> {
    const params: Record<string, any> = {
      ...config.params,
      [config.pagination.pageNumParam]: page
    };
    if (config.apiType !== 'sse_hudong') {
      params[config.pagination.pageSizeParam] = pageSize;
    }

    // 添加日期参数
    if (config.dateConfig) {
      const getCurrentRange = (): { begin: string; end: string } => {
        if (useWideDateRange || !metadata) {
          const wideScope = calculateDateScope({ dateRange: -1 });
          return {
            begin: '2020-01-01',
            end: formatDateOnly(wideScope.endDate)
          };
        }

        return this.calculateDateRangeForMetadata(metadata, config) || {
          begin: '2020-01-01',
          end: formatDateOnly(calculateDateScope({ futureDays: 0 }).endDate)
        };
      };

      const { begin, end } = getCurrentRange();

      if (config.dateConfig.type === 'param') {
        params[config.dateConfig.beginParam || 'begin_time'] = begin;
        params[config.dateConfig.endParam || 'end_time'] = end;
      } else if (config.dateConfig.type === 'filter') {
        const field = config.dateConfig.field;
        if (field) {
          const template = config.dateConfig.filterTemplate || `({field}>='{start}')({field}<='{end}')`;
          const dateFilter = template
            .replace(/{field}/g, field)
            .replace(/{start}/g, begin)
            .replace(/{end}/g, end);
          params.filter = params.filter ? `${params.filter}${dateFilter}` : dateFilter;
        }
      }
    }

    return params;
  }

  /**
   * 提取总数
   */
  private extractTotal(responseData: any, config: ApiRequestConfig, pageSize?: number): number {
    // 不同 API 的总数字段不同
    // 注意：hits 要在 size 之前检查，因为有些 API 的 size 是当前页数量而不是总数

    // 优先使用明确的总数字段
    if (responseData?.result?.count !== undefined) {
      return responseData.result.count;
    }

    // 如果有 pages 字段（总页数），需要结合实际返回的数据量来估算
    // 注意：有些 API 的 pages 是基于固定的内部分页大小计算的，而不是请求的 pageSize
    if (responseData?.pages !== undefined && responseData?.data?.length !== undefined) {
      const actualDataLength = responseData.data.length;
      const pages = responseData.pages;
      // 使用实际返回的数据量作为每页大小来估算总数
      // 如果是第一页且数据量小于请求的 pageSize，说明 API 有内部限制
      if (pageSize && actualDataLength < pageSize && actualDataLength > 0) {
        return pages * actualDataLength;
      } else if (pageSize) {
        return pages * pageSize;
      }
    }

    return responseData?.data?.total_hits ||
           responseData?.data?.total ||
           responseData?.TotalCount ||
           responseData?.hits ||
           responseData?.total ||
           0;
  }

  /**
   * 互动易 API
   */
  private async fetchHudongyiByConfig(
    metadata: ApiMetadata,
    headers: Record<string, string>,
    config: ApiRequestConfig
  ): Promise<FetchResult> {
    const actualUrl = config.baseUrl;
    const fieldSyncPageSize = config.pagination.fieldSyncPageSize;
    headers['Content-Type'] = 'application/x-www-form-urlencoded';

    // 获取循环策略配置
    const loopStrategy = config.loopStrategy;
    const dateBatch = loopStrategy?.dateBatch;
    const batchDays = dateBatch?.batchDays || 1;
    const dateFormat = dateBatch?.dateFormat || 'YYYY-MM-DD HH:mm:ss';

    // 字段同步只取最近的轻量日期窗口，避免互动易探测请求过重
    const fieldSyncScope = calculateDateScope({
      dateRange: config.dateConfig?.fieldSyncDays || batchDays,
      dateRangeMode: config.dateConfig?.dateRangeMode || 'count_including_today',
      futureDays: 0
    });

    // 格式化日期
    const formatDate = (date: Date, withTime: boolean) => {
      const dateStr = formatDateOnly(date);
      return withTime ? `${dateStr} 00:00:00` : dateStr;
    };
    const formatEndDate = (date: Date, withTime: boolean) => {
      const dateStr = formatDateOnly(date);
      return withTime ? `${dateStr} 23:59:59` : dateStr;
    };

    const withTime = dateFormat.includes('HH:mm:ss');
    const beginDateParam = dateBatch?.beginDateParam || 'beginDate';
    const endDateParam = dateBatch?.endDateParam || 'endDate';

    const buildParamsForScope = (scope: { startDate: Date; endDate: Date }, pageSize: number): Record<string, any> => ({
      ...config.params,
      [config.pagination.pageNumParam]: 1,
      [config.pagination.pageSizeParam]: pageSize,
      [beginDateParam]: formatDate(scope.startDate, withTime),
      [endDateParam]: formatEndDate(scope.endDate, withTime)
    });

    let params: Record<string, any> = buildParamsForScope(fieldSyncScope, fieldSyncPageSize);

    let response = await axios.post(actualUrl, new URLSearchParams(params).toString(), {
      headers,
      timeout: 30000,
      httpsAgent: httpsAgent,
      httpAgent: httpAgent
    });

    const maxPageSize = config.pagination.crawlerPageSize || config.pagination.fieldSyncPageSize || 1000;
    let data = this.extractData(response.data, config.dataPath);
    let apiTotalRecord = response.data?.totalRecord || response.data?.TotalCount || null;

    if ((!data || data.length === 0) && (metadata.date_range || config.dateConfig?.crawlerDays || 0) > (config.dateConfig?.fieldSyncDays || batchDays)) {
      const fallbackScope = calculateDateScope({
        dateRange: metadata.date_range || config.dateConfig?.crawlerDays || 5,
        dateRangeMode: config.dateConfig?.dateRangeMode || 'count_including_today',
        futureDays: (metadata as any).future_days || 0,
        fixedBeginDate: (metadata as any).fixed_begin_date || null
      });
      params = buildParamsForScope(fallbackScope, fieldSyncPageSize);
      response = await axios.post(actualUrl, new URLSearchParams(params).toString(), {
        headers,
        timeout: 30000,
        httpsAgent: httpsAgent,
        httpAgent: httpAgent
      });
      data = this.extractData(response.data, config.dataPath);
      apiTotalRecord = response.data?.totalRecord || response.data?.TotalCount || null;
    }

    // 实际采集时的参数（显示每批的日期范围）
    const crawlerParams: Record<string, any> = buildParamsForScope(fieldSyncScope, maxPageSize || config.pagination.crawlerPageSize || 1000);

    // 计算采集统计信息
    const crawlStats = this.calculateCrawlStats(metadata, config, maxPageSize, apiTotalRecord);

    return { data, actualParams: params, crawlerParams, actualUrl, maxPageSize, crawlStats };
  }

  /**
   * 上证e互动 API (HTML)
   */
  private async fetchSseHudongByConfig(
    metadata: ApiMetadata,
    headers: Record<string, string>,
    config: ApiRequestConfig
  ): Promise<FetchResult> {
    const timeoutEnv = Number(process.env.SSE_HUDONG_REQUEST_TIMEOUT_MS);
    const sseHudongRequestTimeoutMs = Number.isFinite(timeoutEnv) && timeoutEnv > 0
      ? Math.floor(timeoutEnv)
      : 60000;
    const actualUrl = config.baseUrl;
    headers['Content-Type'] = 'application/x-www-form-urlencoded';
    headers['Referer'] = 'https://sns.sseinfo.com/qa.do';
    headers['X-Requested-With'] = 'XMLHttpRequest';

    // 获取循环策略配置
    const loopStrategy = config.loopStrategy;
    const dateBatch = loopStrategy?.dateBatch;
    const batchDays = dateBatch?.batchDays || 1;
    const beginDateParam = dateBatch?.beginDateParam || config.dateConfig?.beginParam || 'sdate';
    const endDateParam = dateBatch?.endDateParam || config.dateConfig?.endParam || 'edate';

    // 字段同步用的日期范围（固定7天，确保能获取到示例数据）
    const now = new Date();
    const fieldSyncEndDate = now.toISOString().split('T')[0];
    const fieldSyncStartDate = new Date(now);
    fieldSyncStartDate.setDate(fieldSyncStartDate.getDate() - 7);
    const fieldSyncSdate = fieldSyncStartDate.toISOString().split('T')[0];

    // 实际采集用的日期范围（显示每批的日期范围）
    const crawlerEndDate = new Date(now);
    const crawlerStartDate = new Date(now);
    crawlerStartDate.setDate(crawlerStartDate.getDate() - batchDays + 1);
    const crawlerSdate = crawlerStartDate.toISOString().split('T')[0];
    const crawlerEdate = crawlerEndDate.toISOString().split('T')[0];

    const params: Record<string, any> = {
      ...config.params,
      [beginDateParam]: fieldSyncSdate,
      [endDateParam]: fieldSyncEndDate,
      [config.pagination.pageNumParam]: 1
    };

    // 上证e互动没有分页大小参数，显示每批的日期范围
    const crawlerParams: Record<string, any> = {
      ...config.params,
      [beginDateParam]: crawlerSdate,
      [endDateParam]: crawlerEdate,
      [config.pagination.pageNumParam]: 1
    };

    const response = await axios.post(actualUrl, new URLSearchParams(params).toString(), {
      headers,
      timeout: sseHudongRequestTimeoutMs,
      httpsAgent: httpsAgent,
      httpAgent: httpAgent
    });

    // 解析 HTML
    const $ = cheerio.load(response.data);
    const items: any[] = [];

    $('.m_feed_item').each((i, el) => {
      if (i > 0) return;

      const $item = $(el);
      const qaId = $item.attr('id')?.replace('item-', '') || '';
      const $askDetail = $item.find('.m_qa_detail').length > 0
        ? $item.find('.m_qa_detail')
        : $item.find('.m_feed_detail');

      const askerName = $askDetail.find('.m_feed_face p').text().trim();
      const askTime = $askDetail.find('.m_feed_from span').text().trim();
      const question = $askDetail.find('.m_feed_txt').text().trim();
      const askSource = $askDetail.find('.m_feed_from a').text().trim();
      const answererName = $item.find('.m_qa .m_feed_face p').text().trim();
      const answerTime = $item.find('.m_qa .m_feed_from span').text().trim();
      const answer = $item.find('.m_qa .m_feed_txt').text().trim();
      const answerSource = $item.find('.m_qa .m_feed_from a').text().trim();

      const stockMatch = question.match(/:([^(]+)\((\d+)\)/);
      const stockName = stockMatch ? stockMatch[1].trim() : '';
      const stockCode = stockMatch ? stockMatch[2] : '';
      const cleanQuestion = question.replace(/^[^:]*:[^)]+\)/, '').trim();

      items.push({
        qaId, stockCode, stockName, askerName, askTime, askSource,
        question: cleanQuestion || question,
        answererName, answerTime, answerSource, answer, eitime: fieldSyncEndDate
      });
    });

    const data = items.length === 0
      ? [{
          qaId: '', stockCode: '', stockName: '', askerName: '', askTime: '', askSource: '',
          question: '', answererName: '', answerTime: '', answerSource: '', answer: '', eitime: ''
        }]
      : items;

    // 计算采集统计信息
    const crawlStats = this.calculateCrawlStats(metadata, config, null, null);

    // 上证e互动是 HTML 解析，没有分页大小参数
    return { data, actualParams: params, crawlerParams, actualUrl, maxPageSize: null, crawlStats };
  }

  /**
   * 从响应数据中提取数据数组
   */
  private extractData(responseData: any, dataPath: string): any[] {
    const paths = dataPath.split('.');
    let data = responseData;
    for (const p of paths) {
      if (data && typeof data === 'object') {
        data = data[p];
      } else {
        return [];
      }
    }
    return Array.isArray(data) ? data : [];
  }

  /**
   * 获取 API 返回的字段列表
   * 只请求1条数据，快速获取字段结构
   */
  private async fetchApiFields(metadata: ApiMetadata): Promise<FetchResult> {
    const tableName = metadata.table_name;
    console.log(`[FieldSync] 请求 ${metadata.cn_name} (${tableName})`);

    const headers: Record<string, string> = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept': 'application/json, text/plain, */*',
      'Accept-Language': 'zh-CN,zh;q=0.9',
      'Referer': metadata.source_url || ''
    };

    try {
      // 优先使用数据库中的配置
      const config = this.getApiRequestConfig(metadata);
      if (config && config.apiType) {
        console.log(`[FieldSync] 使用数据库配置: apiType=${config.apiType}`);
        return this.fetchByConfig(metadata, headers, config);
      }

      // 没有配置，报错
      throw new Error('缺少 datacenter_config 配置');
    } catch (error: any) {
      throw new Error(`${error.message}`);
    }
  }

  /**
   * 重建数据库表
   */
  private async rebuildTable(metadata: ApiMetadata, apiFields: string[]): Promise<void> {
    const db = getDatabase();
    const tableName = metadata.table_name;
    const managedExtraFields = this.getManagedExtraFields(metadata);

    console.log(`[FieldSync] 重建表 ${tableName}，字段数: ${apiFields.length}`);

    // 处理字段名冲突：将 API 返回的 id 字段重命名为 api_id
    const processedFields = apiFields.map(col => {
      if (col.toLowerCase() === 'id') {
        return 'api_id';
      }
      return col;
    });
    for (const field of managedExtraFields) {
      if (!processedFields.some(col => col.toLowerCase() === field.toLowerCase())) {
        processedFields.push(field);
      }
    }

    // 删除旧表
    db.run(`DROP TABLE IF EXISTS ${tableName}`);

    // 创建新表（所有字段都用 TEXT 类型，因为我们要保存原始数据）
    const columns = processedFields.map(col => `"${col}" TEXT`).join(', ');
    const sql = `
      CREATE TABLE ${tableName} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metadata_id INTEGER NOT NULL,
        ${columns},
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (metadata_id) REFERENCES api_metadata(id)
      )
    `;

    db.run(sql);

    // 更新 output_config（保留原始字段名，但标记 id -> api_id 的映射）
    const outputConfig = apiFields.map(name => ({
      name: name.toLowerCase() === 'id' ? 'api_id' : name,
      type: 'TEXT',
      originalName: name
    }));
    db.run(
      'UPDATE api_metadata SET output_config = ?, updated_at = ? WHERE id = ?',
      [JSON.stringify(outputConfig), new Date().toISOString(), metadata.id]
    );

    saveDatabase();

    console.log(`[FieldSync] 表 ${tableName} 重建完成`);
  }
}

export const fieldSyncService = new FieldSyncService();
