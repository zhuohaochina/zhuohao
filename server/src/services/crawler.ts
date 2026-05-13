/**
 * 数据采集服务模块
 * 负责从 API 采集数据
 */
import axios from 'axios';
import * as cheerio from 'cheerio';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { HttpProxyAgent } from 'http-proxy-agent';
import { ApiMetadata, OutputConfig, ApiRequestConfig, LoopStrategyConfig, RelativeDateRule } from '../models/types';
import { broadcastProgress, createProgressMessage } from '../websocket';
import { calculateDateScope, formatDateOnly, hasDateScope } from '../utils/dateScope';

interface DateScopeOptions {
  dateRange?: number;
  dateRangeMode?: 'offset_days' | 'count_including_today';
  dateUnit?: 'calendar_day' | 'trading_day';
  closedDateRanges?: Array<{ begin: string; end: string }>;
  futureDays?: number;
  fixedBeginDate?: string | null;
  relativeDateRule?: RelativeDateRule | null;
}

export interface JgdyMainContentUpdate {
  securityCode: string;
  receiveStartDate: string;
  noticeDate: string;
  mainContent: string;
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

/**
 * 采集被取消的错误类
 */
export class CrawlerCancelledError extends Error {
  constructor(message: string = '采集已被用户取消') {
    super(message);
    this.name = 'CrawlerCancelledError';
  }
}

/**
 * 数据采集服务类
 */
export class CrawlerService {
  // 存储最后一次采集时接口返回的原始字段列表
  private lastRawFields: string[] = [];

  // 取消标记：存储需要取消的 metadataId
  private cancelledIds: Set<number> = new Set();

  /**
   * 请求取消指定接口的采集
   * @param metadataId 元数据 ID
   */
  requestCancel(metadataId: number): void {
    this.cancelledIds.add(metadataId);
    console.log(`[Crawler] 收到取消请求: metadataId=${metadataId}`);
  }

  /**
   * 请求取消所有采集
   */
  requestCancelAll(): void {
    console.log(`[Crawler] 收到取消所有采集请求`);
    // 设置一个特殊标记表示取消所有
    this.cancelledIds.add(-1);
  }

  /**
   * 检查是否已被取消
   * @param metadataId 元数据 ID
   */
  isCancelled(metadataId: number): boolean {
    return this.cancelledIds.has(metadataId) || this.cancelledIds.has(-1);
  }

  /**
   * 清除取消标记
   * @param metadataId 元数据 ID
   */
  clearCancel(metadataId: number): void {
    this.cancelledIds.delete(metadataId);
  }

  /**
   * 清除所有取消标记
   */
  clearAllCancels(): void {
    this.cancelledIds.clear();
  }

  /**
   * 检查取消状态，如果已取消则抛出错误
   * @param metadataId 元数据 ID
   */
  private checkCancelled(metadataId: number): void {
    if (this.isCancelled(metadataId)) {
      throw new CrawlerCancelledError();
    }
  }

  /**
   * 获取最后一次采集的原始字段列表
   */
  getLastRawFields(): string[] {
    return this.lastRawFields;
  }

  /**
   * 设置原始字段列表（在解析响应时调用）
   */
  private setLastRawFields(fields: string[]): void {
    if (fields.length > 0) {
      this.lastRawFields = fields;
    }
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

  /**
   * 构建日期过滤条件
   */
  private buildDateFilter(
    field: string,
    dateRange: number,
    futureDays: number = 0,
    template?: string,
    fixedBeginDate?: string | null,
    relativeDateRule?: RelativeDateRule | null
  ): string {
    const { startDate, endDate } = this.calculateDateRange({
      dateRange,
      futureDays,
      fixedBeginDate,
      relativeDateRule
    });

    const startDateStr = formatDateOnly(startDate);
    const endDateStr = formatDateOnly(endDate);

    if (template) {
      return template
        .replace(/{field}/g, field)
        .replace(/{start}/g, startDateStr)
        .replace(/{end}/g, endDateStr);
    }

    return `(${field}>='${startDateStr}')(${field}<='${endDateStr}')`;
  }

  /**
   * 计算采集的日期范围
   * @param options 日期选项
   * @returns { startDate, endDate } 开始和结束日期
   */
  private calculateDateRange(options?: {
    dateRange?: number;
    dateRangeMode?: 'offset_days' | 'count_including_today';
    dateUnit?: 'calendar_day' | 'trading_day';
    closedDateRanges?: Array<{ begin: string; end: string }>;
    futureDays?: number;
    fixedBeginDate?: string | null;
    relativeDateRule?: RelativeDateRule | null;
  }): { startDate: Date; endDate: Date; noDateFilter: boolean } {
    return calculateDateScope(options);
  }

  private getDateScopeOptions(config?: ApiRequestConfig, options?: DateScopeOptions): DateScopeOptions {
    return {
      ...options,
      dateRangeMode: config?.dateConfig?.dateRangeMode ?? options?.dateRangeMode,
      dateUnit: config?.dateConfig?.dateUnit ?? options?.dateUnit,
      closedDateRanges: config?.dateConfig?.closedDateRanges ?? options?.closedDateRanges,
      relativeDateRule: config?.dateConfig?.relativeDateRule ?? options?.relativeDateRule ?? null
    };
  }

  private isTradingDate(date: Date, closedDateRanges: Array<{ begin: string; end: string }> = []): boolean {
    const day = date.getUTCDay();
    const key = date.toISOString().split('T')[0];
    return day >= 1 && day <= 5 && !closedDateRanges.some(range => key >= range.begin && key <= range.end);
  }

  /**
   * 生成日期批次列表
   * @param startDate 开始日期
   * @param endDate 结束日期
   * @param batchDays 每批天数
   * @param useSingleDate 是否使用同一天（如互动易按天循环）
   * @returns 日期批次数组 [{begin, end}, ...]
   */
  private generateDateBatches(
    startDate: Date,
    endDate: Date,
    batchDays: number,
    useSingleDate: boolean = false,
    dateUnit: 'calendar_day' | 'trading_day' = 'calendar_day',
    closedDateRanges: Array<{ begin: string; end: string }> = []
  ): Array<{ begin: string; end: string }> {
    const batches: Array<{ begin: string; end: string }> = [];
    const current = new Date(startDate);

    while (current <= endDate) {
      if (dateUnit === 'trading_day' && useSingleDate && !this.isTradingDate(current, closedDateRanges)) {
        current.setDate(current.getDate() + 1);
        continue;
      }

      const batchStart = new Date(current);
      let batchEnd: Date;

      if (useSingleDate) {
        // 使用同一天
        batchEnd = new Date(current);
      } else {
        // 计算批次结束日期
        batchEnd = new Date(current);
        batchEnd.setDate(batchEnd.getDate() + batchDays - 1);
        if (batchEnd > endDate) {
          batchEnd = new Date(endDate);
        }
      }

      batches.push({
        begin: batchStart.toISOString().split('T')[0],
        end: batchEnd.toISOString().split('T')[0]
      });

      // 移动到下一批次
      current.setDate(current.getDate() + batchDays);
    }

    return batches;
  }

  /**
   * 格式化日期字符串
   * @param dateStr 日期字符串 YYYY-MM-DD
   * @param format 目标格式
   * @returns 格式化后的日期字符串
   */
  private formatDateString(dateStr: string, format?: string): string {
    if (!format || format === 'YYYY-MM-DD') {
      return dateStr;
    }
    if (format === 'YYYY-MM-DD HH:mm:ss') {
      return `${dateStr} 00:00:00`;
    }
    return dateStr;
  }

  /**
   * 获取循环策略配置
   * 如果配置中没有 loopStrategy，根据 apiType 返回默认配置
   */
  private getLoopStrategy(config: ApiRequestConfig): LoopStrategyConfig {
    if (config.loopStrategy) {
      return config.loopStrategy;
    }

    // 根据 apiType 返回默认配置
    switch (config.apiType) {
      case 'hudongyi':
        return {
          type: 'double',
          dateBatch: {
            batchDays: 1,
            beginDateParam: 'beginDate',
            endDateParam: 'endDate',
            useSingleDate: true,
            dateFormat: 'YYYY-MM-DD HH:mm:ss'
          },
          requestDelay: { betweenPages: 100, betweenBatches: 200 }
        };
      case 'sse_hudong':
        return {
          type: 'double',
          dateBatch: {
            batchDays: 1,
            beginDateParam: 'sdate',
            endDateParam: 'edate',
            useSingleDate: true,
            dateFormat: 'YYYY-MM-DD'
          },
          requestDelay: { betweenPages: 100, betweenBatches: 200 }
        };
      default:
        return {
          type: 'single',
          requestDelay: { betweenPages: 100 }
        };
    }
  }

  private getPositiveIntegerEnv(name: string, fallback: number): number {
    const value = Number(process.env[name]);
    return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
  }

  private getAnnouncementRequestTimeoutMs(): number {
    return this.getPositiveIntegerEnv('ANNOUNCEMENT_REQUEST_TIMEOUT_MS', 90000);
  }

  private getAnnouncementPageDelayMs(configuredDelay?: number): number {
    const minDelay = this.getPositiveIntegerEnv('ANNOUNCEMENT_MIN_PAGE_DELAY_MS', 1000);
    return Math.max(configuredDelay || 0, minDelay);
  }

  private getAnnouncementBatchDelayMs(configuredDelay?: number): number {
    const minDelay = this.getPositiveIntegerEnv('ANNOUNCEMENT_MIN_BATCH_DELAY_MS', 1500);
    return Math.max(configuredDelay || 0, minDelay);
  }

  private shouldReportAnnouncementRetry(attempt: number): boolean {
    return attempt === 1 || attempt === 3 || attempt === 5;
  }

  private async fetchAnnouncementPage(
    url: string,
    params: Record<string, any>,
    headers: Record<string, string>,
    timeoutMs: number,
    onRetry?: (context: { attempt: number; delayMs: number; error: any }) => void
  ): Promise<any> {
    const maxRetries = this.getPositiveIntegerEnv('ANNOUNCEMENT_MAX_RETRIES', 5);
    const retryDelayMs = this.getPositiveIntegerEnv('ANNOUNCEMENT_RETRY_DELAY_MS', 3000);
    const maxRetryDelayMs = this.getPositiveIntegerEnv('ANNOUNCEMENT_MAX_RETRY_DELAY_MS', 45000);

    const useProxy = ['1', 'true', 'yes'].includes(String(process.env.ANNOUNCEMENT_USE_PROXY || '').toLowerCase());
    const requestConfig: any = {
      params,
      headers,
      timeout: timeoutMs
    };

    if (useProxy) {
      requestConfig.httpsAgent = httpsAgent;
      requestConfig.httpAgent = httpAgent;
    }

    return this.makeRequestWithRetry(
      () => axios.get(url, requestConfig),
      maxRetries,
      retryDelayMs,
      onRetry,
      maxRetryDelayMs
    );
  }

  /**
   * 通用 API 采集方法
   * 根据数据库配置进行分页采集
   */
  async fetchByApiConfig(
    metadata: ApiMetadata,
    options?: { dateRange?: number; futureDays?: number; fixedBeginDate?: string | null; skipJgdyMainContent?: boolean }
  ): Promise<any[]> {
    const config = this.getApiRequestConfig(metadata);
    if (!config || !config.apiType) throw new Error('缺少 datacenter_config 配置');

    switch (config.apiType) {
      case 'datacenter':
        return this.fetchDatacenterByConfig(metadata, config, options);
      case 'report':
        return this.fetchReportByConfig(metadata, config, options);
      case 'fund':
        return this.fetchFundByConfig(metadata, config, options);
      case 'stock_jsonp':
        return this.fetchStockJsonpByConfig(metadata, config);
      case 'announcement':
        return this.fetchAnnouncementByConfig(metadata, config, options);
      case 'hudongyi':
        return this.fetchHudongyiByConfig(metadata, config, options);
      case 'sse_hudong':
        return this.fetchSseHudongByConfig(metadata, config, options);
      default:
        throw new Error(`未知的 API 类型: ${config.apiType}`);
    }
  }

  /**
   * 东方财富 datacenter API 采集
   */
  private async fetchDatacenterByConfig(
    metadata: ApiMetadata,
    config: ApiRequestConfig,
    options?: { dateRange?: number; futureDays?: number; fixedBeginDate?: string | null; skipJgdyMainContent?: boolean }
  ): Promise<any[]> {
    const metadataId = metadata.id;
    const allData: any[] = [];
    const pagination = config.pagination || {
      pageNumParam: 'pageNumber',
      pageSizeParam: 'pageSize',
      fieldSyncPageSize: 1,
      crawlerPageSize: 500
    };
    const pageSize = pagination.crawlerPageSize || 500;

    this.reportProgress(metadataId, 0, `开始采集 ${metadata.cn_name} 数据...`);

    const headers: Record<string, string> = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Referer': metadata.source_url || 'https://data.eastmoney.com/',
      'Accept': 'application/json, text/plain, */*'
    };

    let page = 1;
    let totalPages = 1;
    let totalCount = 0;

    try {
      while (page <= totalPages) {
        // 检查是否被取消
        this.checkCancelled(metadataId);

        const url = config.baseUrl || 'https://datacenter-web.eastmoney.com/api/data/v1/get';
        const params: Record<string, any> = {
          ...config.params,
          [pagination.pageSizeParam]: pageSize,
          [pagination.pageNumParam]: page
        };

        // 处理日期过滤条件
        const dateScopeOptions = this.getDateScopeOptions(config, options);

        if (config.dateConfig && config.dateConfig.type === 'filter' && hasDateScope(dateScopeOptions)) {
          const filterStr = this.buildDateFilter(
            config.dateConfig.field!,
            dateScopeOptions.dateRange || 0,
            dateScopeOptions.futureDays || 0,
            config.dateConfig.filterTemplate,
            dateScopeOptions.fixedBeginDate,
            dateScopeOptions.relativeDateRule
          );
          params.filter = params.filter ? `${params.filter}${filterStr}` : filterStr;
        }

        const response = await this.makeRequestWithRetry(() =>
          axios.get(url, { params, headers, timeout: 30000, httpsAgent: httpsAgent, httpAgent: httpAgent })
        );

        const list = this.extractData(response.data, config.dataPath || 'result.data');
        if (!list || list.length === 0) break;

        totalPages = this.extractValue(response.data, config.totalPagesPath || 'result.pages') || 1;
        totalCount = this.extractValue(response.data, 'result.count') || 0;

        const records = this.convertRawItems(list);
        allData.push(...records);

        // 报告进度（在请求完成后，此时 totalPages 已经是正确的值）
        const progress = Math.min(Math.round((page / Math.max(totalPages, 1)) * 80) + 10, 90);
        this.reportProgress(metadataId, progress, `正在获取第 ${page}/${totalPages} 页数据，本页 ${records.length} 条，累计 ${allData.length} 条`);

        console.log(`[Crawler] 第 ${page}/${totalPages} 页: ${records.length} 条，累计 ${allData.length}/${totalCount}`);

        page++;
        await this.sleep(100);
      }

      let finalData = allData;

      if (this.isJgdySummaryTable(metadata) && allData.length > 0 && !options?.skipJgdyMainContent) {
        finalData = await this.enrichJgdySummaryWithDetail(metadataId, allData, headers);
      }

      console.log(`[Crawler] ${metadata.cn_name} 采集完成，共 ${finalData.length} 条`);
      this.reportProgress(metadataId, 95, `采集完成，共 ${finalData.length} 条数据`);

      if (finalData.length > 0) {
        this.setLastRawFields(Object.keys(finalData[0]));
      }

      return finalData;
    } catch (error) {
      console.error(`[Crawler] ${metadata.cn_name} 采集失败:`, (error as Error).message);
      this.reportError(metadataId, `采集失败: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * 东方财富研报 API 采集
   * 自动检测 API 最大返回数量
   */
  private async fetchReportByConfig(
    metadata: ApiMetadata,
    config: ApiRequestConfig,
    options?: { dateRange?: number; futureDays?: number; fixedBeginDate?: string | null }
  ): Promise<any[]> {
    const metadataId = metadata.id;
    const allData: any[] = [];
    let pageSize = config.pagination?.crawlerPageSize || 100;
    let actualPageSize = 0;  // API 实际返回的每页数量

    this.reportProgress(metadataId, 0, `开始采集 ${metadata.cn_name} 数据...`);

    const headers: Record<string, string> = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Referer': metadata.source_url || 'https://data.eastmoney.com/',
      'Accept': 'application/json, text/plain, */*'
    };

    // 计算日期范围
    // report API 默认使用近 10 年；若显式传入 fixedBeginDate/dateRange，则按显式配置
    const hasExplicitDateControl = Boolean(options?.fixedBeginDate) || typeof options?.dateRange === 'number';
    const effectiveDateOptions = hasExplicitDateControl
      ? options
      : {
          dateRange: 3650,
          futureDays: options?.futureDays || 0
        };
    const { startDate, endDate } = this.calculateDateRange(this.getDateScopeOptions(config, effectiveDateOptions));
    const beginTime = formatDateOnly(startDate);
    const endTime = formatDateOnly(endDate);

    const beginParam = config.dateConfig?.beginParam || 'beginTime';
    const endParam = config.dateConfig?.endParam || 'endTime';
    const pageNumParam = config.pagination?.pageNumParam || 'pageNo';
    const pageSizeParam = config.pagination?.pageSizeParam || 'pageSize';
    const requestTimeoutMs = this.getPositiveIntegerEnv('REPORT_REQUEST_TIMEOUT_MS', 60000);
    const reportPageDelayMs = Math.max(
      Number(config.loopStrategy?.requestDelay?.betweenPages || 0),
      this.getPositiveIntegerEnv('REPORT_MIN_PAGE_DELAY_MS', 300)
    );

    console.log(`[Crawler] ${metadata.cn_name} 日期范围: ${beginTime} ~ ${endTime}`);

    let page = 1;
    let totalPages = 1;

    try {
      while (page <= totalPages) {
        // 检查是否被取消
        this.checkCancelled(metadataId);

        const url = config.baseUrl || 'https://reportapi.eastmoney.com/report/list';
        const params: Record<string, any> = {
          ...config.params,
          [beginParam]: beginTime,
          [endParam]: endTime,
          [pageNumParam]: page,
          [pageSizeParam]: pageSize,
          _: Date.now()
        };

        const response = await this.makeRequestWithRetry(
          () => axios.get(url, { params, headers, timeout: requestTimeoutMs }),
          8,
          1500,
          undefined,
          30000
        );

        const list = this.extractData(response.data, config.dataPath || 'data');
        if (!list || list.length === 0) break;

        // 第一次请求时，检测 API 实际返回的每页数量
        if (page === 1) {
          actualPageSize = list.length;
          if (actualPageSize < pageSize && actualPageSize > 0 && (this.extractValue(response.data, 'TotalPage') || this.extractValue(response.data, 'pages') || 1) > 1) {
            console.log(`[Crawler] API 限制每页最多 ${actualPageSize} 条，调整分页`);
            pageSize = actualPageSize;
          }
        }

        totalPages = this.extractValue(response.data, 'TotalPage') || 1;

        const records = this.convertRawItems(list);
        allData.push(...records);

        // 报告进度（在请求完成后，此时 totalPages 已经是正确的值）
        const progress = Math.min(Math.round((page / Math.max(totalPages, 1)) * 80) + 10, 90);
        this.reportProgress(metadataId, progress, `正在获取第 ${page}/${totalPages} 页数据，本页 ${records.length} 条，累计 ${allData.length} 条`);

        console.log(`[Crawler] 第 ${page}/${totalPages} 页: ${records.length} 条，累计 ${allData.length}`);

        page++;
        await this.sleep(reportPageDelayMs);
      }

      console.log(`[Crawler] ${metadata.cn_name} 采集完成，共 ${allData.length} 条`);
      this.reportProgress(metadataId, 95, `采集完成，共 ${allData.length} 条数据`);

      if (allData.length > 0) {
        this.setLastRawFields(Object.keys(allData[0]));
      }

      return allData;
    } catch (error) {
      console.error(`[Crawler] ${metadata.cn_name} 采集失败:`, (error as Error).message);
      this.reportError(metadataId, `采集失败: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * 东方财富基金持仓 API 采集
   * 自动检测 API 最大返回数量
   */
  private async fetchFundByConfig(
    metadata: ApiMetadata,
    config: ApiRequestConfig,
    options?: { dateRange?: number; futureDays?: number; fixedBeginDate?: string | null }
  ): Promise<any[]> {
    const metadataId = metadata.id;
    const allData: any[] = [];
    let pageSize = 5000;  // 先请求大数量，让 API 返回实际限制
    let actualPageSize = 0;

    this.reportProgress(metadataId, 0, `开始采集 ${metadata.cn_name} 数据...`);

    const headers: Record<string, string> = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Referer': metadata.source_url || 'https://data.eastmoney.com/',
      'Accept': 'application/json, text/plain, */*'
    };

    let page = 1;
    let totalPages = 1;

    try {
      while (page <= totalPages) {
        // 检查是否被取消
        this.checkCancelled(metadataId);

        const url = config.baseUrl || 'https://data.eastmoney.com/dataapi/zlsj/list';
        const params: Record<string, any> = {
          ...config.params,
          pageNum: page,
          pageSize
        };

        const response = await this.makeRequestWithRetry(() =>
          axios.get(url, { params, headers, timeout: 30000, httpsAgent: httpsAgent, httpAgent: httpAgent })
        );

        const list = this.extractData(response.data, config.dataPath || 'data');
        if (!list || list.length === 0) break;

        // 第一次请求时，检测 API 实际返回的每页数量
        if (page === 1) {
          actualPageSize = list.length;
          if (actualPageSize < pageSize && actualPageSize > 0 && (this.extractValue(response.data, 'TotalPage') || this.extractValue(response.data, 'pages') || 1) > 1) {
            console.log(`[Crawler] API 限制每页最多 ${actualPageSize} 条，调整分页`);
            pageSize = actualPageSize;
          }
        }

        totalPages = this.extractValue(response.data, 'pages') || 1;

        const records = this.convertRawItems(list);
        allData.push(...records);

        // 报告进度（在请求完成后，此时 totalPages 已经是正确的值）
        const progress = Math.min(Math.round((page / Math.max(totalPages, 1)) * 80) + 10, 90);
        this.reportProgress(metadataId, progress, `正在获取第 ${page}/${totalPages} 页数据，本页 ${records.length} 条，累计 ${allData.length} 条`);

        console.log(`[Crawler] 第 ${page}/${totalPages} 页: ${records.length} 条，累计 ${allData.length}`);

        page++;
        await this.sleep(100);
      }

      console.log(`[Crawler] ${metadata.cn_name} 采集完成，共 ${allData.length} 条`);
      this.reportProgress(metadataId, 95, `采集完成，共 ${allData.length} 条数据`);

      if (allData.length > 0) {
        this.setLastRawFields(Object.keys(allData[0]));
      }

      return allData;
    } catch (error) {
      console.error(`[Crawler] ${metadata.cn_name} 采集失败:`, (error as Error).message);
      this.reportError(metadataId, `采集失败: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * 东方财富 A股列表 JSONP 采集
   * 使用 Puppeteer 浏览器采集，绕过 TLS 指纹检测
   */
  private async fetchStockJsonpByConfig(
    metadata: ApiMetadata,
    config: ApiRequestConfig
  ): Promise<any[]> {
    const metadataId = metadata.id;

    this.reportProgress(metadataId, 0, `开始采集 ${metadata.cn_name} 数据（浏览器模式）...`);

    try {
      // 动态导入浏览器采集服务，避免循环依赖
      const { browserCrawlerService } = await import('./browserCrawler');

      const allData = await browserCrawlerService.fetchAllStockList(
        config.params || {},
        (current, total, message) => {
          // 检查是否被取消
          if (this.isCancelled(metadataId)) {
            throw new CrawlerCancelledError();
          }
          const progress = Math.min(Math.round((current / Math.max(total, 1)) * 80) + 10, 90);
          this.reportProgress(metadataId, progress, message);
        }
      );

      // 转换数据格式
      const records = this.convertRawItems(allData);

      console.log(`[Crawler] ${metadata.cn_name} 采集完成，共 ${records.length} 条`);
      this.reportProgress(metadataId, 95, `采集完成，共 ${records.length} 条数据`);

      if (records.length > 0) {
        this.setLastRawFields(Object.keys(records[0]));
      }

      return records;
    } catch (error) {
      if (error instanceof CrawlerCancelledError) {
        throw error;
      }
      console.error(`[Crawler] ${metadata.cn_name} 采集失败:`, (error as Error).message);
      this.reportError(metadataId, `采集失败: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * 东方财富公告 API 采集
   * 使用 loopStrategy 配置控制循环策略（支持日期分批+分页）
   */
  private async fetchAnnouncementByConfig(
    metadata: ApiMetadata,
    config: ApiRequestConfig,
    options?: { dateRange?: number; futureDays?: number; fixedBeginDate?: string | null }
  ): Promise<any[]> {
    const metadataId = metadata.id;
    const allData: any[] = [];
    let pageSize = config.pagination?.crawlerPageSize || 5000;
    let actualPageSize = 0;
    let skippedPageCount = 0;

    // 获取循环策略配置
    const loopStrategy = this.getLoopStrategy(config);
    const dateBatch = loopStrategy.dateBatch;
    const requestDelay = loopStrategy.requestDelay;
    const requestTimeoutMs = this.getAnnouncementRequestTimeoutMs();
    const pageDelayMs = this.getAnnouncementPageDelayMs(requestDelay?.betweenPages);
    const batchDelayMs = this.getAnnouncementBatchDelayMs(requestDelay?.betweenBatches);
    const pageProgressReportInterval = 10;

    this.reportProgress(metadataId, 0, `开始采集 ${metadata.cn_name} 数据...`);

    const headers: Record<string, string> = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Referer': metadata.source_url || 'https://data.eastmoney.com/',
      'Accept': 'application/json, text/plain, */*'
    };

    // 计算日期范围（支持 fixedBeginDate）
    const { startDate, endDate } = this.calculateDateRange(this.getDateScopeOptions(config, options));

    try {
      // 检查是否使用双层循环（日期分批+分页）
      if (loopStrategy.type === 'double' && dateBatch) {
        // 双层循环：外层按日期分批，内层分页
        const batchDays = dateBatch.batchDays || 10;
        const reportPageLevelProgress = batchDays > 1;
        const batches = this.generateDateBatches(
          startDate,
          endDate,
          batchDays,
          false,
          config.dateConfig?.dateUnit || 'calendar_day',
          config.dateConfig?.closedDateRanges || []
        );
        const totalBatches = batches.length;

        for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
          // 检查是否被取消
          this.checkCancelled(metadataId);

          const batch = batches[batchIdx];
          const batchProgress = Math.round(((batchIdx + 1) / totalBatches) * 80) + 10;

          let page = 1;
          let totalPages = 1;
          let batchTotal = 0;
          let fetchedPagesInBatch = 0;
          let skippedCurrentBatch = false;

          while (page <= totalPages) {
            // 检查是否被取消
            this.checkCancelled(metadataId);

            const url = config.baseUrl || 'https://np-anotice-stock.eastmoney.com/api/security/ann';
            const params: Record<string, any> = {
              ...config.params,
              [config.pagination?.pageNumParam || 'page_index']: page,
              [config.pagination?.pageSizeParam || 'page_size']: pageSize,
              [dateBatch.beginDateParam || 'begin_time']: batch.begin,
              [dateBatch.endDateParam || 'end_time']: batch.end
            };

            const requestStartAt = Date.now();
            let response: any;
            try {
              response = await this.fetchAnnouncementPage(
                url,
                params,
                headers,
                requestTimeoutMs,
                ({ attempt, delayMs, error }) => {
                  if (!this.shouldReportAnnouncementRetry(attempt)) return;
                  const errMsg = error?.response?.status
                    ? `HTTP ${error.response.status}`
                    : (error?.code || error?.message || '未知错误');
                  this.reportProgress(metadataId, batchProgress,
                    `${batch.begin}~${batch.end} (${batchIdx + 1}/${totalBatches}) 第${page}页网络波动(${errMsg})，${Math.round(delayMs / 1000)}秒后自动重试(${attempt})...`);
                }
              );
            } catch (error: any) {
              const errMsg = error?.response?.status
                ? `HTTP ${error.response.status}`
                : (error?.code || error?.message || '未知错误');
              skippedPageCount++;
              skippedCurrentBatch = true;
              this.reportProgress(metadataId, batchProgress,
                `${batch.begin}~${batch.end} (${batchIdx + 1}/${totalBatches}) 第${page}页连续重试后仍失败(${errMsg})，跳过该日剩余页并继续下一日`);
              break;
            }
            const requestCostMs = Date.now() - requestStartAt;

            const list = this.extractData(response.data, config.dataPath || 'data.list');
            if (!list || list.length === 0) break;

            // 第一次请求时，检测 API 实际返回的每页数量
            if (page === 1 && batchIdx === 0) {
              actualPageSize = list.length;
              if (actualPageSize < pageSize && actualPageSize > 0 && (response.data?.data?.total_hits || 0) > actualPageSize) {
                console.log(`[Crawler] API 限制每页最多 ${actualPageSize} 条，调整分页`);
                pageSize = actualPageSize;
              }
            }

            batchTotal = response.data?.data?.total_hits || 0;
            totalPages = Math.ceil(batchTotal / pageSize);

            // 转换数据格式
            const records = this.convertAnnouncementRecords(list);
            allData.push(...records);
            fetchedPagesInBatch = page;

            // 批量模式（batchDays=1）下避免逐页刷屏，仅保留批次级别进度
            if (reportPageLevelProgress) {
              const shouldReportPageProgress =
                totalPages <= 3 ||
                page === totalPages ||
                page % pageProgressReportInterval === 0;

              if (shouldReportPageProgress) {
                this.reportProgress(
                  metadataId,
                  batchProgress,
                  `${batch.begin}~${batch.end} (${batchIdx + 1}/${totalBatches}) 第${page}/${totalPages}页，累计 ${allData.length} 条，请求耗时 ${requestCostMs}ms`
                );
              }
            }

            console.log(`[Crawler] ${batch.begin}~${batch.end} 第 ${page}/${totalPages} 页: ${records.length} 条，累计 ${allData.length}`);

            page++;
            await this.sleep(pageDelayMs);
          }

          // 批次完成后更新进度，显示该批次采集结果
          const pagesInBatch = batchTotal > 0 ? totalPages : 0;
          const batchPageText = skippedCurrentBatch
            ? `已采 ${fetchedPagesInBatch}/${pagesInBatch} 页，后续页已跳过`
            : `共 ${pagesInBatch} 页`;
          this.reportProgress(metadataId, batchProgress,
            `${batch.begin}~${batch.end} 完成 (${batchIdx + 1}/${totalBatches})，本批 ${batchTotal} 条，${batchPageText}，累计 ${allData.length} 条`);

          await this.sleep(batchDelayMs);
        }
      } else {
        // 单层循环：仅分页
        const beginTime = startDate.toISOString().split('T')[0];
        const endTime = endDate.toISOString().split('T')[0];

        let page = 1;
        let totalPages = 1;

        while (page <= totalPages) {
          // 检查是否被取消
          this.checkCancelled(metadataId);

          const url = config.baseUrl || 'https://np-anotice-stock.eastmoney.com/api/security/ann';
          const params: Record<string, any> = {
            ...config.params,
            [config.pagination?.pageNumParam || 'page_index']: page,
            [config.pagination?.pageSizeParam || 'page_size']: pageSize,
            begin_time: beginTime,
            end_time: endTime
          };

          const requestStartAt = Date.now();
          let response: any;
          try {
            response = await this.fetchAnnouncementPage(
              url,
              params,
              headers,
              requestTimeoutMs,
              ({ attempt, delayMs, error }) => {
                if (!this.shouldReportAnnouncementRetry(attempt)) return;
                const errMsg = error?.response?.status
                  ? `HTTP ${error.response.status}`
                  : (error?.code || error?.message || '未知错误');
                const progress = Math.min(Math.round((page / Math.max(totalPages, 1)) * 80) + 10, 90);
                this.reportProgress(metadataId, progress,
                  `第 ${page}/${totalPages} 页网络波动(${errMsg})，${Math.round(delayMs / 1000)}秒后自动重试(${attempt})...`);
              }
            );
          } catch (error: any) {
            const errMsg = error?.response?.status
              ? `HTTP ${error.response.status}`
              : (error?.code || error?.message || '未知错误');
            const progress = Math.min(Math.round((page / Math.max(totalPages, 1)) * 80) + 10, 90);
            skippedPageCount++;
            this.reportProgress(metadataId, progress,
              `第 ${page}/${totalPages} 页连续重试后仍失败(${errMsg})，跳过后续页，保留已采集数据`);
            break;
          }
          const requestCostMs = Date.now() - requestStartAt;

          const list = this.extractData(response.data, config.dataPath || 'data.list');
          if (!list || list.length === 0) break;

          // 第一次请求时，检测 API 实际返回的每页数量
          if (page === 1) {
            actualPageSize = list.length;
            if (actualPageSize < pageSize && actualPageSize > 0 && (response.data?.data?.total_hits || 0) > actualPageSize) {
              console.log(`[Crawler] API 限制每页最多 ${actualPageSize} 条，调整分页`);
              pageSize = actualPageSize;
            }
          }

          totalPages = Math.ceil((response.data?.data?.total_hits || 0) / pageSize);

          // 转换数据格式
          const records = this.convertAnnouncementRecords(list);
          allData.push(...records);

          // 报告进度（在请求完成后，此时 totalPages 已经是正确的值）
          const progress = Math.min(Math.round((page / Math.max(totalPages, 1)) * 80) + 10, 90);
          this.reportProgress(metadataId, progress, `正在获取第 ${page}/${totalPages} 页数据，本页 ${records.length} 条，累计 ${allData.length} 条，请求耗时 ${requestCostMs}ms`);

          console.log(`[Crawler] 第 ${page}/${totalPages} 页: ${records.length} 条，累计 ${allData.length}`);

          page++;
          await this.sleep(pageDelayMs);
        }
      }

      const completionMessage = skippedPageCount > 0
        ? `采集完成，共 ${allData.length} 条数据，跳过 ${skippedPageCount} 个连续失败页/批次`
        : `采集完成，共 ${allData.length} 条数据`;
      console.log(`[Crawler] ${metadata.cn_name} ${completionMessage}`);
      this.reportProgress(metadataId, 95, completionMessage);

      if (allData.length > 0) {
        this.setLastRawFields(Object.keys(allData[0]));
      }

      return allData;
    } catch (error) {
      console.error(`[Crawler] ${metadata.cn_name} 采集失败:`, (error as Error).message);
      this.reportError(metadataId, `采集失败: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * 转换公告数据格式
   */
  private convertAnnouncementRecords(list: any[]): any[] {
    return list.map((item: any) => ({
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

  /**
   * 互动易 API 采集
   * 使用 loopStrategy 配置控制循环策略
   */
  private async fetchHudongyiByConfig(
    metadata: ApiMetadata,
    config: ApiRequestConfig,
    options?: { dateRange?: number; futureDays?: number; fixedBeginDate?: string | null }
  ): Promise<any[]> {
    const metadataId = metadata.id;
    const allData: any[] = [];
    const pageSize = config.pagination?.crawlerPageSize || 5000;

    // 获取循环策略配置
    const loopStrategy = this.getLoopStrategy(config);
    const dateBatch = loopStrategy.dateBatch;
    const requestDelay = loopStrategy.requestDelay;

    this.reportProgress(metadataId, 0, `开始采集 ${metadata.cn_name} 数据...`);

    const headers: Record<string, string> = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Referer': metadata.source_url || 'https://irm.cninfo.com.cn/',
      'Accept': 'application/json, text/plain, */*',
      'Content-Type': 'application/x-www-form-urlencoded'
    };

    // 计算日期范围（支持 fixedBeginDate）
    const { startDate, endDate } = this.calculateDateRange(this.getDateScopeOptions(config, options));

    // 生成日期批次
    const batchDays = dateBatch?.batchDays || 1;
    const useSingleDate = dateBatch?.useSingleDate !== false;
    const batches = this.generateDateBatches(
      startDate,
      endDate,
      batchDays,
      useSingleDate,
      config.dateConfig?.dateUnit || 'calendar_day',
      config.dateConfig?.closedDateRanges || []
    );
    const totalBatches = batches.length;

    try {
      for (let idx = 0; idx < batches.length; idx++) {
        // 检查是否被取消
        this.checkCancelled(metadataId);

        const batch = batches[idx];
        const progress = Math.round(((idx + 1) / totalBatches) * 80) + 10;
        this.reportProgress(metadataId, progress, `正在采集 ${batch.begin} (${idx + 1}/${totalBatches})...`);
        let batchCount = 0;

        const url = config.baseUrl || 'https://irm.cninfo.com.cn/newircs/index/search';

        // 格式化日期
        const dateFormat = dateBatch?.dateFormat || 'YYYY-MM-DD HH:mm:ss';
        let beginDateStr = batch.begin;
        let endDateStr = batch.end;
        if (dateFormat === 'YYYY-MM-DD HH:mm:ss') {
          beginDateStr = `${batch.begin} 00:00:00`;
          endDateStr = `${batch.end} 23:59:59`;
        }

        let page = 1;
        let totalPages = 1;
        const pageNumParam = config.pagination?.pageNumParam || 'pageNo';
        const pageSizeParam = config.pagination?.pageSizeParam || 'pageSize';

        while (page <= totalPages) {
          // 检查是否被取消
          this.checkCancelled(metadataId);

          const params: Record<string, any> = {
            ...config.params,
            [pageNumParam]: page,
            [pageSizeParam]: pageSize,
            [dateBatch?.beginDateParam || 'beginDate']: beginDateStr,
            [dateBatch?.endDateParam || 'endDate']: endDateStr
          };

          const response = await this.makeRequestWithRetry(() =>
            axios.post(url, new URLSearchParams(params).toString(), { headers, timeout: 30000, httpsAgent: httpsAgent, httpAgent: httpAgent })
          );

          // 首次请求时确定当天总页数（互动易返回 totalPage / totalRecord）
          if (page === 1) {
            const totalPageFromResponse = Number(this.extractValue(response.data, 'totalPage'));
            if (Number.isFinite(totalPageFromResponse) && totalPageFromResponse > 0) {
              totalPages = totalPageFromResponse;
            } else {
              const totalRecordFromResponse = Number(this.extractValue(response.data, 'totalRecord'));
              if (Number.isFinite(totalRecordFromResponse) && totalRecordFromResponse > 0) {
                totalPages = Math.max(1, Math.ceil(totalRecordFromResponse / pageSize));
              }
            }
          }

          const list = this.extractData(response.data, config.dataPath || 'results');
          if (!list || list.length === 0) {
            break;
          }

          const records = this.convertRawItems(list);
          allData.push(...records);
          batchCount += records.length;

          if (page < totalPages) {
            await this.sleep(requestDelay?.betweenPages || 100);
          }
          page++;
        }

        this.reportProgress(
          metadataId,
          progress,
          `正在采集 ${batch.begin} (${idx + 1}/${totalBatches})，当日 ${batchCount} 条，累计 ${allData.length} 条`
        );

        await this.sleep(requestDelay?.betweenBatches || 200);
      }

      console.log(`[Crawler] ${metadata.cn_name} 采集完成，共 ${allData.length} 条`);
      this.reportProgress(metadataId, 95, `采集完成，共 ${allData.length} 条数据`);

      if (allData.length > 0) {
        this.setLastRawFields(Object.keys(allData[0]));
      }

      return allData;
    } catch (error) {
      console.error(`[Crawler] ${metadata.cn_name} 采集失败:`, (error as Error).message);
      this.reportError(metadataId, `采集失败: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * 上证e互动 API 采集 (HTML)
   * 使用 loopStrategy 配置控制循环策略
   */
  private async fetchSseHudongByConfig(
    metadata: ApiMetadata,
    config: ApiRequestConfig,
    options?: { dateRange?: number; futureDays?: number; fixedBeginDate?: string | null }
  ): Promise<any[]> {
    const metadataId = metadata.id;
    const allData: any[] = [];

    // 获取循环策略配置
    const loopStrategy = this.getLoopStrategy(config);
    const dateBatch = loopStrategy.dateBatch;
    const requestDelay = loopStrategy.requestDelay;

    this.reportProgress(metadataId, 0, `开始采集 ${metadata.cn_name} 数据...`);

    const headers: Record<string, string> = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Referer': 'https://sns.sseinfo.com/qa.do',
      'Accept': '*/*',
      'Content-Type': 'application/x-www-form-urlencoded',
      'X-Requested-With': 'XMLHttpRequest'
    };

    // 计算日期范围（支持 fixedBeginDate）
    const { startDate, endDate } = this.calculateDateRange(this.getDateScopeOptions(config, options));

    // 生成日期批次
    const batchDays = dateBatch?.batchDays || 1;
    const useSingleDate = dateBatch?.useSingleDate !== false;
    const batches = this.generateDateBatches(
      startDate,
      endDate,
      batchDays,
      useSingleDate,
      config.dateConfig?.dateUnit || 'calendar_day',
      config.dateConfig?.closedDateRanges || []
    );
    const totalBatches = batches.length;
    const beginDateParam = dateBatch?.beginDateParam || 'sdate';
    const endDateParam = dateBatch?.endDateParam || 'edate';
    const pageNumParam = config.pagination?.pageNumParam || 'page';
    const pageSizeParam = config.pagination?.pageSizeParam || 'pageSize';
    const includePageSizeEnv = (process.env.SSE_HUDONG_INCLUDE_PAGE_SIZE || '').toLowerCase();
    const includePageSize = includePageSizeEnv === '1' || includePageSizeEnv === 'true';
    const configuredPageSize = Math.max(1, Number(config.pagination?.crawlerPageSize || 100));
    const url = config.baseUrl || 'https://sns.sseinfo.com/getNewDataFullText.do';

    const parseNonNegativeIntEnv = (name: string, fallback: number): number => {
      const value = Number(process.env[name]);
      if (Number.isFinite(value) && value >= 0) {
        return Math.floor(value);
      }
      return fallback;
    };
    const parsePositiveIntEnv = (name: string, fallback: number): number => {
      const value = Number(process.env[name]);
      if (Number.isFinite(value) && value > 0) {
        return Math.floor(value);
      }
      return fallback;
    };
    const sseHudongRequestTimeoutMs = parsePositiveIntEnv('SSE_HUDONG_REQUEST_TIMEOUT_MS', 20000);
    const sseHudongMaxRetries = parsePositiveIntEnv('SSE_HUDONG_MAX_RETRIES', 3);
    const sseHudongRetryDelayMs = parsePositiveIntEnv('SSE_HUDONG_RETRY_DELAY_MS', 1000);
    const sseHudongMaxRetryDelayMs = parsePositiveIntEnv('SSE_HUDONG_MAX_RETRY_DELAY_MS', 15000);
    const useSseHudongProxy = ['1', 'true', 'yes'].includes(String(process.env.SSE_HUDONG_USE_PROXY || '').toLowerCase());
    const toPageItemKey = (item: Record<string, any>): string => {
      if (item.qaId) {
        return `qa:${item.qaId}`;
      }
      // fallback key for rare rows without qaId
      return `fallback:${item.askTime || ''}|${item.answerTime || ''}|${item.question || ''}|${item.answer || ''}`;
    };

    const configuredPageDelayMs = Math.max(0, Number(requestDelay?.betweenPages ?? 80));
    const configuredBatchDelayMs = Math.max(0, Number(requestDelay?.betweenBatches ?? 120));
    const minPageDelayMs = parseNonNegativeIntEnv(
      'SSE_HUDONG_MIN_PAGE_DELAY_MS',
      Math.min(configuredPageDelayMs, 160)
    );
    const minBatchDelayMs = parseNonNegativeIntEnv(
      'SSE_HUDONG_MIN_BATCH_DELAY_MS',
      Math.min(configuredBatchDelayMs, 280)
    );
    const maxPageDelayMs = Math.max(
      configuredPageDelayMs,
      parseNonNegativeIntEnv('SSE_HUDONG_MAX_PAGE_DELAY_MS', 1200)
    );
    const maxBatchDelayMs = Math.max(
      configuredBatchDelayMs,
      parseNonNegativeIntEnv('SSE_HUDONG_MAX_BATCH_DELAY_MS', 2000)
    );
    const cooldownPageStepMs = parsePositiveIntEnv('SSE_HUDONG_COOLDOWN_PAGE_STEP_MS', 160);
    const cooldownBatchStepMs = parsePositiveIntEnv('SSE_HUDONG_COOLDOWN_BATCH_STEP_MS', 220);
    const relaxPageStepMs = parsePositiveIntEnv('SSE_HUDONG_RELAX_PAGE_STEP_MS', 120);
    const relaxBatchStepMs = parsePositiveIntEnv('SSE_HUDONG_RELAX_BATCH_STEP_MS', 180);
    const stableStreakThreshold = Math.max(2, parsePositiveIntEnv('SSE_HUDONG_STABLE_STREAK', 4));

    let dynamicPageDelayMs = configuredPageDelayMs;
    let dynamicBatchDelayMs = configuredBatchDelayMs;
    const emptyBatchDelayMs = Math.min(40, configuredBatchDelayMs);
    let stableRequestStreak = 0;
    let skippedPageCount = 0;

    const withJitter = (baseMs: number, minJitter: number, maxJitter: number): number => {
      if (baseMs <= 0) return 0;
      const jitter = Math.floor(Math.random() * (maxJitter - minJitter + 1)) + minJitter;
      return Math.max(0, baseMs + jitter);
    };

    const applyThrottleCooldown = (reason: string): void => {
      const prevPageDelay = dynamicPageDelayMs;
      const prevBatchDelay = dynamicBatchDelayMs;
      dynamicPageDelayMs = Math.min(
        maxPageDelayMs,
        Math.max(dynamicPageDelayMs, configuredPageDelayMs) + cooldownPageStepMs
      );
      dynamicBatchDelayMs = Math.min(
        maxBatchDelayMs,
        Math.max(dynamicBatchDelayMs, configuredBatchDelayMs) + cooldownBatchStepMs
      );
      stableRequestStreak = 0;

      if (prevPageDelay !== dynamicPageDelayMs || prevBatchDelay !== dynamicBatchDelayMs) {
        console.log(
          `[Crawler][sse_hudong] throttle safeguard activated (${reason}); ` +
          `pageDelay=${dynamicPageDelayMs}ms, batchDelay=${dynamicBatchDelayMs}ms`
        );
      }
    };

    const markRequestStable = (): void => {
      stableRequestStreak += 1;
      if (stableRequestStreak < stableStreakThreshold) return;
      stableRequestStreak = 0;

      const prevPageDelay = dynamicPageDelayMs;
      const prevBatchDelay = dynamicBatchDelayMs;
      dynamicPageDelayMs = Math.max(minPageDelayMs, dynamicPageDelayMs - relaxPageStepMs);
      dynamicBatchDelayMs = Math.max(minBatchDelayMs, dynamicBatchDelayMs - relaxBatchStepMs);

      if (prevPageDelay !== dynamicPageDelayMs || prevBatchDelay !== dynamicBatchDelayMs) {
        console.log(
          `[Crawler][sse_hudong] throttle safeguard relaxed; ` +
          `pageDelay=${dynamicPageDelayMs}ms, batchDelay=${dynamicBatchDelayMs}ms`
        );
      }
    };

    try {
      for (let idx = 0; idx < batches.length; idx++) {
        // 检查是否被取消
        this.checkCancelled(metadataId);

        const batch = batches[idx];
        const progress = Math.round(((idx + 1) / totalBatches) * 80) + 10;
        this.reportProgress(metadataId, progress, `正在采集 ${batch.begin} (${idx + 1}/${totalBatches})...`);
        let batchCount = 0;

        let page = 1;
        let hasMore = true;
        const seenItemKeysInBatch = new Set<string>();
        let previousPageSignature = '';

        while (hasMore) {
          // 检查是否被取消
          this.checkCancelled(metadataId);

          const params: Record<string, any> = {
            ...config.params,
            [beginDateParam]: batch.begin,
            [endDateParam]: batch.end,
            [pageNumParam]: page
          };
          if (includePageSize) {
            params[pageSizeParam] = configuredPageSize;
          }

          const requestConfig: any = {
            headers,
            timeout: sseHudongRequestTimeoutMs
          };
          if (useSseHudongProxy) {
            requestConfig.httpsAgent = httpsAgent;
            requestConfig.httpAgent = httpAgent;
          }

          let response: any;
          try {
            response = await this.makeRequestWithRetry(
              () => axios.post(url, new URLSearchParams(params).toString(), requestConfig),
              sseHudongMaxRetries,
              sseHudongRetryDelayMs,
              ({ attempt, delayMs, error }) => {
                const status = Number(error?.response?.status || 0);
                const message = String(error?.message || '').toLowerCase();
                const errMsg = error?.response?.status
                  ? `HTTP ${error.response.status}`
                  : (error?.code || error?.message || '未知错误');
                this.reportProgress(
                  metadataId,
                  progress,
                  `${batch.begin} (${idx + 1}/${totalBatches}) 第${page}页网络波动(${errMsg})，${Math.round(delayMs / 1000)}秒后自动重试(${attempt})...`
                );
                if (
                  status === 429 ||
                  status === 403 ||
                  status >= 500 ||
                  message.includes('busy') ||
                  message.includes('forbidden') ||
                  message.includes('blocked')
                ) {
                  applyThrottleCooldown(status > 0 ? `HTTP ${status}` : (message || 'retryable_error'));
                }
              },
              sseHudongMaxRetryDelayMs
            );
          } catch (error: any) {
            const errMsg = error?.response?.status
              ? `HTTP ${error.response.status}`
              : (error?.code || error?.message || '未知错误');
            skippedPageCount++;
            applyThrottleCooldown(errMsg);
            this.reportProgress(
              metadataId,
              progress,
              `${batch.begin} (${idx + 1}/${totalBatches}) 第${page}页连续重试后仍失败(${errMsg})，跳过该日剩余页并继续下一日`
            );
            break;
          }

          // 解析 HTML
          const $ = cheerio.load(response.data);
          const items: any[] = [];

          $('.m_feed_item').each((i, el) => {
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
              answererName, answerTime, answerSource, answer, eitime: batch.begin
            });
          });

          if (items.length === 0) {
            hasMore = false;
          } else {
            const pageSignature = items.map(toPageItemKey).join('||');
            if (pageSignature && pageSignature === previousPageSignature) {
              console.warn(
                `[Crawler][sse_hudong] repeated page detected at ${batch.begin} page=${page}, stop current day to avoid loop`
              );
              applyThrottleCooldown('duplicate_page');
              hasMore = false;
              continue;
            }
            previousPageSignature = pageSignature;

            const freshItems = items.filter(item => {
              const key = toPageItemKey(item);
              if (seenItemKeysInBatch.has(key)) {
                return false;
              }
              seenItemKeysInBatch.add(key);
              return true;
            });

            if (freshItems.length === 0) {
              console.log(
                `[Crawler][sse_hudong] all rows duplicated at ${batch.begin} page=${page}, stop current day`
              );
              hasMore = false;
              continue;
            }

            markRequestStable();
            allData.push(...freshItems);
            batchCount += freshItems.length;
            page++;
            if (dynamicPageDelayMs > 0) {
              await this.sleep(withJitter(dynamicPageDelayMs, -20, 80));
            }
          }
        }

        this.reportProgress(
          metadataId,
          progress,
          `正在采集 ${batch.begin} (${idx + 1}/${totalBatches})，当日 ${batchCount} 条，累计 ${allData.length} 条`
        );

        console.log(`[Crawler] ${batch.begin}: 当日 ${batchCount} 条，累计 ${allData.length} 条`);
        const delayMs = batchCount === 0 ? emptyBatchDelayMs : dynamicBatchDelayMs;
        if (delayMs > 0) {
          await this.sleep(withJitter(delayMs, -30, 110));
        }
      }

      const completionMessage = skippedPageCount > 0
        ? `采集完成，共 ${allData.length} 条数据，跳过 ${skippedPageCount} 个连续失败页/批次`
        : `采集完成，共 ${allData.length} 条数据`;
      console.log(`[Crawler] ${metadata.cn_name} ${completionMessage}`);
      this.reportProgress(metadataId, 95, completionMessage);

      if (allData.length > 0) {
        this.setLastRawFields(Object.keys(allData[0]));
      }

      return allData;
    } catch (error) {
      console.error(`[Crawler] ${metadata.cn_name} 采集失败:`, (error as Error).message);
      this.reportError(metadataId, `采集失败: ${(error as Error).message}`);
      throw error;
    }
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
   * 从响应数据中提取单个值
   */
  private extractValue(responseData: any, dataPath: string): any {
    const paths = dataPath.split('.');
    let data = responseData;
    for (const p of paths) {
      if (data && typeof data === 'object') {
        data = data[p];
      } else {
        return undefined;
      }
    }
    return data;
  }

  /**
   * 将 API 返回的原始数据转换为可存储的格式
   * 保留所有字段，不做任何过滤
   * @param item API 返回的单条数据
   * @returns 转换后的数据
   */
  private convertRawItem(item: any): Record<string, any> {
    const result: Record<string, any> = {};
    for (const key of Object.keys(item)) {
      // 将字段名转换为小写，并将 API 返回的 id 字段重命名为 api_id，避免与表主键冲突
      const lowerKey = key.toLowerCase();
      const fieldName = lowerKey === 'id' ? 'api_id' : lowerKey;
      const value = item[key];
      if (value === undefined) {
        result[fieldName] = null;
      } else if (typeof value === 'boolean') {
        result[fieldName] = value ? 1 : 0;
      } else if (typeof value === 'object' && value !== null) {
        result[fieldName] = JSON.stringify(value);
      } else {
        result[fieldName] = value;
      }
    }
    return result;
  }

  /**
   * 批量转换 API 返回的数据
   * @param items API 返回的数据数组
   * @returns 转换后的数据数组
   */
  private convertRawItems(items: any[]): Record<string, any>[] {
    return items.map(item => this.convertRawItem(item));
  }

  /**
   * 判断请求错误是否可重试
   */
  private isRetryableRequestError(error: any): boolean {
    if (error?.retryable === true) {
      return true;
    }

    const code = String(error?.code || '').toUpperCase();
    const message = String(error?.message || '').toLowerCase();
    const status = error?.response?.status;

    // HTTP 层可重试状态
    if (typeof status === 'number') {
      if (status === 429) return true;
      if (status >= 500 && status < 600) return true;
    }

    // 网络层可重试错误码
    const retryableCodes = new Set([
      'ECONNRESET',
      'ETIMEDOUT',
      'ENOTFOUND',
      'ECONNREFUSED',
      'ECONNABORTED',
      'EPIPE',
      'EHOSTUNREACH',
      'ENETUNREACH',
      'ERR_NETWORK'
    ]);

    if (retryableCodes.has(code)) {
      return true;
    }

    // 文本匹配（兼容代理/流中断等报错）
    return (
      message.includes('socket hang up') ||
      message.includes('secure tls connection') ||
      message.includes('timeout') ||
      message.includes('stream has been aborted') ||
      message.includes('aborted') ||
      message.includes('network error') ||
      message.includes('服务器繁忙') ||
      message.includes('系统繁忙') ||
      message.includes('busy')
    );
  }

  /**
   * 带重试机制的 HTTP 请求
   * @param requestFn 请求函数
   * @param maxRetries 最大重试次数
   * @param retryDelay 初始重试延迟（毫秒）
   * @returns 响应数据
   */
  private async makeRequestWithRetry<T>(
    requestFn: () => Promise<T>,
    maxRetries: number = 5,
    retryDelay: number = 1000,
    onRetry?: (context: { attempt: number; delayMs: number; error: any }) => void,
    maxRetryDelayMs?: number
  ): Promise<T> {
    let lastError: any;
    const retryDelayCapMs = maxRetryDelayMs && maxRetryDelayMs > 0
      ? Math.floor(maxRetryDelayMs)
      : this.getPositiveIntegerEnv('REQUEST_MAX_RETRY_DELAY_MS', 60000);

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return await requestFn();
      } catch (error: any) {
        lastError = error;

        // 检查是否是可重试的错误
        const isRetryable = this.isRetryableRequestError(error);

        if (!isRetryable || attempt === maxRetries) {
          throw error;
        }

        // 计算延迟时间（指数退避）
        let delay = retryDelay * Math.pow(2, attempt);

        // 若服务端返回 Retry-After，优先尊重（常见于 429）
        const retryAfterHeader = error?.response?.headers?.['retry-after'];
        const retryAfterSec = Number(retryAfterHeader);
        if (!Number.isNaN(retryAfterSec) && retryAfterSec > 0) {
          delay = Math.max(delay, retryAfterSec * 1000);
        }

        // 增加轻微抖动，避免重试同频
        const jitter = Math.floor(Math.random() * 500);
        delay += jitter;
        delay = Math.min(delay, retryDelayCapMs);

        const errorMark = error?.response?.status
          ? `HTTP ${error.response.status}`
          : (error.code || error.message);
        console.log(`[Crawler] 请求失败 (${errorMark}), ${delay}ms 后重试 (${attempt + 1}/${maxRetries})...`);

        onRetry?.({
          attempt: attempt + 1,
          delayMs: delay,
          error
        });

        await this.sleep(delay);
      }
    }

    throw lastError;
  }

  /**
   * 延时函数
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * 从响应数据中提取原始字段列表
   * @param response API 响应数据
   * @returns 原始字段名数组
   */
  extractRawFields(response: any): string[] {
    let dataArray: any[] = [];

    if (Array.isArray(response)) {
      if (response.length > 0 && response[0] && Array.isArray(response[0].data)) {
        dataArray = response[0].data;
      } else {
        dataArray = response;
      }
    } else if (response && typeof response === 'object') {
      const possibleFields = ['data', 'list', 'items', 'records', 'result', 'results', 'content'];
      for (const field of possibleFields) {
        if (Array.isArray(response[field])) {
          dataArray = response[field];
          break;
        }
      }
      if (dataArray.length === 0 && response.data && typeof response.data === 'object') {
        for (const field of possibleFields) {
          if (Array.isArray(response.data[field])) {
            dataArray = response.data[field];
            break;
          }
        }
      }
    }

    if (dataArray.length > 0 && dataArray[0] && typeof dataArray[0] === 'object') {
      return Object.keys(dataArray[0]);
    }
    return [];
  }

  /**
   * 解析 API 响应
   * @param response API 响应数据
   * @param outputConfig 出参配置
   * @returns 解析后的数据数组
   */
  parseResponse(response: any, outputConfig: OutputConfig[]): Record<string, any>[] {
    let dataArray: any[] = [];
    let foundArrayField = false;  // 标记是否找到了数组字段

    console.log('[parseResponse] 响应类型:', typeof response, Array.isArray(response) ? '(数组)' : '');

    if (Array.isArray(response)) {
      // 深交所格式: [{metadata: {...}, data: [...]}]
      if (response.length > 0 && response[0] && Array.isArray(response[0].data)) {
        dataArray = response[0].data;
      } else {
        dataArray = response;
      }
      foundArrayField = true;
    } else if (response && typeof response === 'object') {
      // 尝试常见的数据字段名
      const possibleFields = ['data', 'list', 'items', 'records', 'result', 'results', 'content'];

      for (const field of possibleFields) {
        if (Array.isArray(response[field])) {
          console.log(`[parseResponse] 在顶层找到数组字段: ${field}, 长度: ${response[field].length}`);
          dataArray = response[field];
          foundArrayField = true;
          break;
        }
      }

      // 检查嵌套结构
      if (!foundArrayField && response.data && typeof response.data === 'object') {
        console.log('[parseResponse] 检查嵌套结构 response.data');
        for (const field of possibleFields) {
          if (Array.isArray(response.data[field])) {
            console.log(`[parseResponse] 在 response.data 中找到数组字段: ${field}, 长度: ${response.data[field].length}`);
            dataArray = response.data[field];
            foundArrayField = true;
            break;
          }
        }
      }

      // 只有在没有找到任何数组字段时，才将整个响应作为单条数据
      if (!foundArrayField && !Array.isArray(response)) {
        console.log('[parseResponse] 未找到数组字段，将整个响应作为单条数据');
        dataArray = [response];
      }
    }

    console.log(`[parseResponse] 最终数据数组长度: ${dataArray.length}`);
    if (dataArray.length > 0) {
      console.log('[parseResponse] 第一条数据的字段:', Object.keys(dataArray[0]));
      // 记录接口实际返回的原始字段
      this.setLastRawFields(Object.keys(dataArray[0]));
    }

    // 保存 API 返回的所有字段，不再根据 outputConfig 过滤
    // outputConfig 仅用于参考，实际保存以 API 返回为准
    return dataArray.map(item => {
      const result: Record<string, any> = {};

      // 遍历 API 返回的所有字段
      for (const fieldName of Object.keys(item)) {
        const value = item[fieldName];

        if (value === undefined) {
          result[fieldName] = null;
        } else if (typeof value === 'boolean') {
          // SQLite 不支持布尔类型，转换为 0/1
          result[fieldName] = value ? 1 : 0;
        } else if (typeof value === 'object' && value !== null) {
          // 对象或数组转换为 JSON 字符串
          result[fieldName] = JSON.stringify(value);
        } else {
          result[fieldName] = value;
        }
      }

      return result;
    });
  }

  private isJgdySummaryTable(metadata: ApiMetadata): boolean {
    return metadata.table_name === 'biz_eastmoney_jgdy_summary';
  }

  private getValueByCandidates(row: Record<string, any>, candidates: string[]): any {
    for (const candidate of candidates) {
      if (Object.prototype.hasOwnProperty.call(row, candidate)) {
        return row[candidate];
      }
    }

    const entries = Object.entries(row);
    for (const candidate of candidates) {
      const lowerCandidate = candidate.toLowerCase();
      const found = entries.find(([key]) => key.toLowerCase() === lowerCandidate);
      if (found) {
        return found[1];
      }
    }

    return undefined;
  }

  private normalizeDateValue(value: any): string | null {
    if (value === null || value === undefined) return null;
    const raw = String(value).trim();
    if (!raw) return null;

    const ymd = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (ymd) {
      return `${ymd[1]}-${ymd[2]}-${ymd[3]}`;
    }

    const ymdSlash = raw.match(/^(\d{4})\/(\d{2})\/(\d{2})/);
    if (ymdSlash) {
      return `${ymdSlash[1]}-${ymdSlash[2]}-${ymdSlash[3]}`;
    }

    const parsed = new Date(raw);
    if (!Number.isNaN(parsed.getTime())) {
      const year = parsed.getFullYear();
      const month = String(parsed.getMonth() + 1).padStart(2, '0');
      const day = String(parsed.getDate()).padStart(2, '0');
      return `${year}-${month}-${day}`;
    }

    return null;
  }

  private buildJgdyEventKey(row: Record<string, any>): string | null {
    const securityCodeRaw = this.getValueByCandidates(row, ['security_code', 'SECURITY_CODE']);
    const receiveStartDateRaw = this.getValueByCandidates(row, ['receive_start_date', 'RECEIVE_START_DATE']);
    const noticeDateRaw = this.getValueByCandidates(row, ['notice_date', 'NOTICE_DATE']);

    const securityCode = securityCodeRaw === null || securityCodeRaw === undefined
      ? ''
      : String(securityCodeRaw).trim();
    const receiveStartDate = this.normalizeDateValue(receiveStartDateRaw);
    const noticeDate = this.normalizeDateValue(noticeDateRaw);

    if (!securityCode || !receiveStartDate || !noticeDate) {
      return null;
    }

    return `${securityCode}|${receiveStartDate}|${noticeDate}`;
  }

  private hasUsefulJgdyMainContent(row: Record<string, any>): boolean {
    const rawValue = this.getValueByCandidates(row, ['main_content', 'MAIN_CONTENT']);
    if (rawValue === null || rawValue === undefined) {
      return false;
    }

    const rawText = String(rawValue).trim();
    if (!rawText) {
      return false;
    }

    try {
      const parsed = JSON.parse(rawText);
      const content = parsed?.CONTENT ?? parsed?.content;
      if (typeof content === 'string' && content.trim()) {
        return true;
      }

      const contentList = parsed?.CONTENT_LIST ?? parsed?.content_list;
      if (Array.isArray(contentList) && contentList.some(item => String(item || '').trim())) {
        return true;
      }

      return false;
    } catch {
      return true;
    }
  }

  private formatDateOnly(date: Date): string {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  private buildMonthRanges(begin: string, end: string): Array<{ begin: string; end: string }> {
    const start = new Date(`${begin}T00:00:00`);
    const finish = new Date(`${end}T00:00:00`);
    if (Number.isNaN(start.getTime()) || Number.isNaN(finish.getTime()) || start > finish) {
      return [];
    }

    const ranges: Array<{ begin: string; end: string }> = [];
    let year = start.getFullYear();
    let month = start.getMonth();

    while (true) {
      const monthStart = new Date(year, month, 1);
      const monthEnd = new Date(year, month + 1, 0);

      const rangeBeginDate = monthStart < start ? start : monthStart;
      const rangeEndDate = monthEnd > finish ? finish : monthEnd;

      ranges.push({
        begin: this.formatDateOnly(rangeBeginDate),
        end: this.formatDateOnly(rangeEndDate)
      });

      if (monthEnd >= finish) {
        break;
      }

      month += 1;
      if (month > 11) {
        month = 0;
        year += 1;
      }
    }

    return ranges;
  }

  private splitIntoChunks<T>(items: T[], chunkSize: number): T[][] {
    if (!Array.isArray(items) || items.length === 0) {
      return [];
    }

    const normalizedChunkSize = Number.isFinite(chunkSize) && chunkSize > 0 ? Math.floor(chunkSize) : 1;
    const chunks: T[][] = [];
    for (let index = 0; index < items.length; index += normalizedChunkSize) {
      chunks.push(items.slice(index, index + normalizedChunkSize));
    }
    return chunks;
  }

  private async buildJgdySummaryMainContentUpdatesPrecise(
    metadataId: number,
    summaryData: Record<string, any>[],
    headers?: Record<string, string>,
    options?: { progressStart?: number; progressEnd?: number }
  ): Promise<JgdyMainContentUpdate[]> {
    const progressStart = options?.progressStart ?? 91;
    const progressEnd = options?.progressEnd ?? 94;
    const progressRange = Math.max(progressEnd - progressStart, 1);

    const requestHeaders: Record<string, string> = headers || {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Referer': 'https://data.eastmoney.com/jgdy/tj.html',
      'Accept': 'application/json, text/plain, */*'
    };

    type DetailTask = {
      requestKey: string;
      securityCode: string;
      receiveStartDate: string;
      targetEventKeys: Set<string>;
    };

    const summaryKeySet = new Set<string>();
    const receiveStartDates: string[] = [];
    const taskMap = new Map<string, DetailTask>();
    let skippedFilled = 0;
    let skippedInvalid = 0;

    for (const row of summaryData) {
      const eventKey = this.buildJgdyEventKey(row);
      const securityCodeRaw = this.getValueByCandidates(row, ['security_code', 'SECURITY_CODE']);
      const receiveStartDateRaw = this.getValueByCandidates(row, ['receive_start_date', 'RECEIVE_START_DATE']);
      const securityCode = securityCodeRaw === null || securityCodeRaw === undefined
        ? ''
        : String(securityCodeRaw).trim();
      const receiveStartDate = this.normalizeDateValue(receiveStartDateRaw);

      if (!eventKey || !securityCode || !receiveStartDate) {
        skippedInvalid += 1;
        continue;
      }

      summaryKeySet.add(eventKey);
      receiveStartDates.push(receiveStartDate);

      if (this.hasUsefulJgdyMainContent(row)) {
        skippedFilled += 1;
        continue;
      }

      const requestKey = `${securityCode}|${receiveStartDate}`;
      if (!taskMap.has(requestKey)) {
        taskMap.set(requestKey, {
          requestKey,
          securityCode,
          receiveStartDate,
          targetEventKeys: new Set<string>()
        });
      }
      taskMap.get(requestKey)!.targetEventKeys.add(eventKey);
    }

    if (summaryKeySet.size === 0 || receiveStartDates.length === 0) {
      throw new Error('机构调研统计汇总补详情失败：缺少可用于关联详情的事件键或接待日期');
    }

    const tasks = Array.from(taskMap.values()).sort((a, b) => {
      if (a.receiveStartDate === b.receiveStartDate) {
        return a.securityCode.localeCompare(b.securityCode);
      }
      return a.receiveStartDate.localeCompare(b.receiveStartDate);
    });

    if (tasks.length === 0) {
      this.reportProgress(
        metadataId,
        progressEnd,
        `机构调研详情(main_content)无需补充：基础事件 ${summaryKeySet.size} 个，已存在正文 ${skippedFilled} 条，缺少关键字段 ${skippedInvalid} 条`
      );
      return [];
    }

    const sortedReceiveDates = [...receiveStartDates].sort();
    const minReceiveDate = sortedReceiveDates[0];
    const maxReceiveDate = sortedReceiveDates[sortedReceiveDates.length - 1];
    const concurrency = Math.min(Math.max(this.getPositiveIntegerEnv('JGDY_DETAIL_CONCURRENCY', 8), 1), 16);
    const batchSize = Math.min(Math.max(this.getPositiveIntegerEnv('JGDY_DETAIL_BATCH_SIZE', 20), 1), 50);
    const pageSize = this.getPositiveIntegerEnv('JGDY_DETAIL_PAGE_SIZE', 50);
    const requestTimeoutMs = this.getPositiveIntegerEnv('JGDY_DETAIL_TIMEOUT_MS', 15000);
    const maxRetries = this.getPositiveIntegerEnv('JGDY_DETAIL_MAX_RETRIES', 2);
    const retryDelayMs = this.getPositiveIntegerEnv('JGDY_DETAIL_RETRY_DELAY_MS', 500);
    const detailUrl = 'https://datacenter-web.eastmoney.com/api/data/v1/get';
    const startedAt = Date.now();

    const detailMap = new Map<string, Array<{ content: string; url: string; organization: string }>>();
    const coveredEventKeys = new Set<string>();
    let completedTasks = 0;
    let completedBatches = 0;
    let successTasks = 0;
    let emptyTasks = 0;
    let failedTasks = 0;
    let totalDetailFetched = 0;
    let totalDetailPageCount = 0;

    this.reportProgress(
      metadataId,
      progressStart,
      `开始批量精准补详情：基础事件 ${summaryKeySet.size} 个，去重后需请求 ${tasks.length} 个详情键，接待日期范围 ${minReceiveDate}~${maxReceiveDate}，并发 ${concurrency}，已跳过有正文 ${skippedFilled} 条`
    );

    this.reportProgress(
      metadataId,
      progressStart,
      `批量精准补详情参数：详情键 ${tasks.length} 个，接待日期范围 ${minReceiveDate}~${maxReceiveDate}，并发 ${concurrency}，每批 ${batchSize} 个代码`
    );

    const recordDetailRow = (detailRow: Record<string, any>): boolean => {
      const eventKey = this.buildJgdyEventKey(detailRow);
      if (!eventKey || !summaryKeySet.has(eventKey)) {
        return false;
      }

      const contentRaw = this.getValueByCandidates(detailRow, ['CONTENT', 'content']);
      const urlRaw = this.getValueByCandidates(detailRow, ['URL', 'url']);
      const orgRaw = this.getValueByCandidates(detailRow, ['RECEIVE_OBJECT', 'receive_object']);
      const content = contentRaw === null || contentRaw === undefined ? '' : String(contentRaw).trim();
      const url = urlRaw === null || urlRaw === undefined ? '' : String(urlRaw).trim();
      const organization = orgRaw === null || orgRaw === undefined ? '' : String(orgRaw).trim();

      const bucket = detailMap.get(eventKey) || [];
      bucket.push({ content, url, organization });
      detailMap.set(eventKey, bucket);
      coveredEventKeys.add(eventKey);
      return true;
    };

    const reportTaskProgress = (force = false) => {
      if (!force && completedTasks % Math.max(concurrency * 5, 20) !== 0 && completedTasks !== tasks.length) {
        return;
      }

      const elapsedMs = Math.max(Date.now() - startedAt, 1);
      const avgMs = Math.round(elapsedMs / Math.max(completedTasks, 1));
      const progress = progressStart + Math.round((completedTasks / tasks.length) * progressRange);
      this.reportProgress(
        metadataId,
        Math.min(progress, progressEnd),
        `精准补详情进度：已完成 ${completedTasks}/${tasks.length}，成功 ${successTasks}，空结果 ${emptyTasks}，失败 ${failedTasks}，已覆盖基础事件 ${coveredEventKeys.size}/${summaryKeySet.size}，累计拉取明细 ${totalDetailFetched} 条/${totalDetailPageCount} 页，平均 ${avgMs}ms/键`
      );
    };

    type DetailBatch = {
      batchKey: string;
      receiveStartDate: string;
      tasks: DetailTask[];
    };

    const tasksByReceiveDate = new Map<string, DetailTask[]>();
    for (const task of tasks) {
      const dateTasks = tasksByReceiveDate.get(task.receiveStartDate) || [];
      dateTasks.push(task);
      tasksByReceiveDate.set(task.receiveStartDate, dateTasks);
    }

    const taskBatches: DetailBatch[] = [];
    for (const receiveStartDate of Array.from(tasksByReceiveDate.keys()).sort()) {
      const dateTasks = (tasksByReceiveDate.get(receiveStartDate) || [])
        .sort((a, b) => a.securityCode.localeCompare(b.securityCode));
      for (const chunk of this.splitIntoChunks(dateTasks, batchSize)) {
        taskBatches.push({
          batchKey: `${receiveStartDate}|${chunk.map(task => task.securityCode).join(',')}`,
          receiveStartDate,
          tasks: chunk
        });
      }
    }

    const fetchTask = async (task: DetailTask): Promise<void> => {
      let page = 1;
      let totalPages = 1;
      let fetchedForTask = 0;
      let matchedForTask = 0;

      while (page <= totalPages) {
        this.checkCancelled(metadataId);

        const params: Record<string, any> = {
          reportName: 'RPT_ORG_SURVEY',
          sortColumns: 'NUMBERNEW',
          sortTypes: '1',
          columns: 'SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,NOTICE_DATE,RECEIVE_START_DATE,RECEIVE_END_DATE,RECEIVE_OBJECT,RECEIVE_PLACE,RECEIVE_WAY_EXPLAIN,INVESTIGATORS,RECEPTIONIST,NUM,CONTENT,ORG_TYPE,URL',
          source: 'WEB',
          client: 'WEB',
          pageNumber: page,
          pageSize,
          filter: `(IS_SOURCE=\"1\")(SECURITY_CODE=\"${task.securityCode}\")(RECEIVE_START_DATE='${task.receiveStartDate}')`
        };

        const response = await this.makeRequestWithRetry(async () => {
          const resp = await axios.get(detailUrl, {
            params,
            headers: {
              ...requestHeaders,
              Referer: `https://data.eastmoney.com/jgdy/dyxx/${task.securityCode},${task.receiveStartDate}.html`
            },
            timeout: requestTimeoutMs,
            httpsAgent,
            httpAgent
          });

          const body = resp.data;
          if (!body || body.success !== true || !body.result) {
            const apiCode = body?.code;
            const apiMessage = body?.message || '详情接口返回异常';
            const detailError: any = new Error(`[JGDY_DETAIL] code=${apiCode ?? 'unknown'} message=${apiMessage}`);
            const messageText = String(apiMessage).toLowerCase();
            detailError.retryable =
              apiCode === 9701 ||
              messageText.includes('服务器繁忙') ||
              messageText.includes('系统繁忙') ||
              messageText.includes('busy') ||
              messageText.includes('timeout');
            throw detailError;
          }

          return resp;
        }, maxRetries, retryDelayMs, ({ attempt, delayMs, error }) => {
          const errMsg = error?.response?.status
            ? `HTTP ${error.response.status}`
            : (error?.code || error?.message || '未知错误');
          this.reportProgress(
            metadataId,
            Math.min(progressEnd, progressStart + Math.round((completedTasks / tasks.length) * progressRange)),
            `详情键 ${task.securityCode}/${task.receiveStartDate} 第 ${page} 页请求异常（${errMsg}），${Math.round(delayMs / 1000)} 秒后第 ${attempt} 次重试`
          );
        }, 10000);

        const list = this.extractData(response.data, 'result.data');
        if (!list || list.length === 0) {
          break;
        }

        totalPages = this.extractValue(response.data, 'result.pages') || 1;
        fetchedForTask += list.length;
        totalDetailFetched += list.length;
        totalDetailPageCount += 1;

        for (const item of list) {
          if (recordDetailRow(item as Record<string, any>)) {
            matchedForTask += 1;
          }
        }

        page += 1;
      }

      if (matchedForTask > 0) {
        successTasks += 1;
      } else if (fetchedForTask > 0) {
        emptyTasks += 1;
      } else {
        emptyTasks += 1;
      }
    };

    const fetchBatch = async (batch: DetailBatch): Promise<void> => {
      if (batch.tasks.length <= 1) {
        await fetchTask(batch.tasks[0]);
        return;
      }

      let page = 1;
      let totalPages = 1;
      const securityCodes = batch.tasks.map(task => task.securityCode);
      const codeFilter = securityCodes.map(code => `\"${code}\"`).join(',');

      while (page <= totalPages) {
        this.checkCancelled(metadataId);

        const params: Record<string, any> = {
          reportName: 'RPT_ORG_SURVEY',
          sortColumns: 'NUMBERNEW',
          sortTypes: '1',
          columns: 'SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,NOTICE_DATE,RECEIVE_START_DATE,RECEIVE_END_DATE,RECEIVE_OBJECT,RECEIVE_PLACE,RECEIVE_WAY_EXPLAIN,INVESTIGATORS,RECEPTIONIST,NUM,CONTENT,ORG_TYPE,URL',
          source: 'WEB',
          client: 'WEB',
          pageNumber: page,
          pageSize,
          filter: `(IS_SOURCE=\"1\")(RECEIVE_START_DATE='${batch.receiveStartDate}')(SECURITY_CODE in (${codeFilter}))`
        };

        const response = await this.makeRequestWithRetry(async () => {
          const resp = await axios.get(detailUrl, {
            params,
            headers: {
              ...requestHeaders,
              Referer: 'https://data.eastmoney.com/jgdy/tj.html'
            },
            timeout: requestTimeoutMs,
            httpsAgent,
            httpAgent
          });

          const body = resp.data;
          if (!body || body.success !== true || !body.result) {
            const apiCode = body?.code;
            const apiMessage = body?.message || '详情批量接口返回异常';
            const detailError: any = new Error(`[JGDY_DETAIL_BATCH] code=${apiCode ?? 'unknown'} message=${apiMessage}`);
            const messageText = String(apiMessage).toLowerCase();
            detailError.retryable =
              apiCode === 9701 ||
              messageText.includes('服务器繁忙') ||
              messageText.includes('系统繁忙') ||
              messageText.includes('busy') ||
              messageText.includes('timeout');
            throw detailError;
          }

          return resp;
        }, maxRetries, retryDelayMs, ({ attempt, delayMs, error }) => {
          const errMsg = error?.response?.status
            ? `HTTP ${error.response.status}`
            : (error?.code || error?.message || '未知错误');
          this.reportProgress(
            metadataId,
            Math.min(progressEnd, progressStart + Math.round((completedTasks / tasks.length) * progressRange)),
            `详情批量 ${batch.receiveStartDate}(${batch.tasks.length}个代码) 第 ${page} 页请求异常（${errMsg}），${Math.round(delayMs / 1000)} 秒后第 ${attempt} 次重试`
          );
        }, 10000);

        const list = this.extractData(response.data, 'result.data');
        if (!list || list.length === 0) {
          break;
        }

        totalPages = this.extractValue(response.data, 'result.pages') || 1;
        totalDetailFetched += list.length;
        totalDetailPageCount += 1;

        for (const item of list) {
          recordDetailRow(item as Record<string, any>);
        }

        page += 1;
      }

      let matchedTasks = 0;
      for (const task of batch.tasks) {
        const matched = Array.from(task.targetEventKeys).some(eventKey => coveredEventKeys.has(eventKey));
        if (matched) {
          matchedTasks += 1;
        }
      }

      successTasks += matchedTasks;
      emptyTasks += batch.tasks.length - matchedTasks;
    };

    let nextIndex = 0;
    const workerCount = Math.min(concurrency, taskBatches.length);
    const workers = Array.from({ length: workerCount }, async () => {
      while (true) {
        this.checkCancelled(metadataId);
        const batchIndex = nextIndex;
        nextIndex += 1;

        if (batchIndex >= taskBatches.length) {
          return;
        }

        const batch = taskBatches[batchIndex];
        const task = batch.tasks[0];
        try {
          await fetchBatch(batch);
          completedTasks += batch.tasks.length;
          completedBatches += 1;
        } catch (error) {
          if (error instanceof CrawlerCancelledError) {
            throw error;
          }

          const batchErrMsg = (error as Error)?.message || '未知错误';
          console.warn(`[Crawler] 机构调研批量补详情失败，拆成单键重试: ${batch.batchKey}, ${batchErrMsg}`);

          for (const singleTask of batch.tasks) {
            try {
              await fetchTask(singleTask);
            } catch (singleError) {
              if (singleError instanceof CrawlerCancelledError) {
                throw singleError;
              }

              failedTasks += 1;
              const singleErrMsg = (singleError as Error)?.message || '未知错误';
              console.warn(`[Crawler] 机构调研精准补详情失败: ${singleTask.requestKey}, ${singleErrMsg}`);
            } finally {
              completedTasks += 1;
            }
          }
          continue;

          failedTasks += 1;
          const errMsg = (error as Error)?.message || '未知错误';
          console.warn(`[Crawler] 机构调研精准补详情失败: ${task.requestKey}, ${errMsg}`);
        } finally {
          completedTasks += 0;
          reportTaskProgress();
        }
      }
    });

    await Promise.all(workers);
    reportTaskProgress(true);

    const mainContentByKey = new Map<string, string>();
    for (const [eventKey, entries] of detailMap.entries()) {
      const uniqueContents = Array.from(new Set(entries.map(entry => entry.content).filter(Boolean)));
      const uniqueUrls = Array.from(new Set(entries.map(entry => entry.url).filter(Boolean)));
      const uniqueOrganizations = Array.from(new Set(entries.map(entry => entry.organization).filter(Boolean)));
      const primaryContent = uniqueContents.length > 0
        ? uniqueContents.reduce((maxContent, currentContent) => {
            return currentContent.length > maxContent.length ? currentContent : maxContent;
          }, '')
        : '';

      if (uniqueContents.length > 1) {
        console.warn(`[Crawler] 机构调研详情事件存在多条 CONTENT，已自动合并: ${eventKey}, count=${uniqueContents.length}`);
      }

      const payload: Record<string, any> = {
        CONTENT: primaryContent,
        URL: uniqueUrls[0] || '',
        机构名单: uniqueOrganizations
      };

      if (uniqueContents.length > 1) {
        payload.CONTENT_LIST = uniqueContents;
      }

      mainContentByKey.set(eventKey, JSON.stringify(payload));
    }

    this.reportProgress(
      metadataId,
      progressEnd,
      `开始将详情按事件键左关联到基础表：详情事件 ${mainContentByKey.size} 个，基础事件 ${summaryKeySet.size} 个`
    );

    const updates: JgdyMainContentUpdate[] = [];
    for (const row of summaryData) {
      const eventKey = this.buildJgdyEventKey(row);
      if (!eventKey) {
        continue;
      }

      const mainContent = mainContentByKey.get(eventKey);
      if (!mainContent) {
        continue;
      }

      const securityCodeRaw = this.getValueByCandidates(row, ['security_code', 'SECURITY_CODE']);
      const receiveStartDateRaw = this.getValueByCandidates(row, ['receive_start_date', 'RECEIVE_START_DATE']);
      const noticeDateRaw = this.getValueByCandidates(row, ['notice_date', 'NOTICE_DATE']);
      const securityCode = securityCodeRaw === null || securityCodeRaw === undefined ? '' : String(securityCodeRaw).trim();
      const receiveStartDate = this.normalizeDateValue(receiveStartDateRaw);
      const noticeDate = this.normalizeDateValue(noticeDateRaw);

      if (!securityCode || !receiveStartDate || !noticeDate) {
        continue;
      }

      updates.push({
        securityCode,
        receiveStartDate,
        noticeDate,
        mainContent
      });
    }

    if (updates.length === 0) {
      this.reportProgress(metadataId, progressEnd, '机构调研详情暂未补充成功：本次精准请求没有生成可回填 main_content');
      return [];
    }

    const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
    this.reportProgress(
      metadataId,
      progressEnd,
      `机构调研精准补详情完成：请求键 ${completedTasks}/${tasks.length}，成功 ${successTasks}，空结果 ${emptyTasks}，失败 ${failedTasks}，累计拉取明细 ${totalDetailFetched} 条/${totalDetailPageCount} 页，覆盖基础事件 ${coveredEventKeys.size}/${summaryKeySet.size}，可回填 ${updates.length}/${summaryData.length} 条，用时 ${elapsedSec} 秒`
    );

    return updates;
  }

  async buildJgdySummaryMainContentUpdates(
    metadataId: number,
    summaryData: Record<string, any>[],
    headers?: Record<string, string>,
    options?: { progressStart?: number; progressEnd?: number }
  ): Promise<JgdyMainContentUpdate[]> {
    if (this.getPositiveIntegerEnv('JGDY_DETAIL_PRECISE_MODE', 1) > 0) {
      return this.buildJgdySummaryMainContentUpdatesPrecise(metadataId, summaryData, headers, options);
    }

    const progressStart = options?.progressStart ?? 91;
    const progressEnd = options?.progressEnd ?? 94;

    const requestHeaders: Record<string, string> = headers || {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Referer': 'https://data.eastmoney.com/jgdy/tj.html',
      'Accept': 'application/json, text/plain, */*'
    };

    const summaryKeySet = new Set<string>();
    const noticeDateCodeMap = new Map<string, { codes: Set<string>; begin: string; end: string }>();
    const summaryKeysByNoticeDate = new Map<string, Set<string>>();
    const noticeDates: string[] = [];

    for (const row of summaryData) {
      const key = this.buildJgdyEventKey(row);
      if (key) {
        summaryKeySet.add(key);
      }

      const noticeDate = this.normalizeDateValue(this.getValueByCandidates(row, ['notice_date', 'NOTICE_DATE']));
      const securityCodeRaw = this.getValueByCandidates(row, ['security_code', 'SECURITY_CODE']);
      const securityCode = securityCodeRaw === null || securityCodeRaw === undefined
        ? ''
        : String(securityCodeRaw).trim();

      if (noticeDate) {
        noticeDates.push(noticeDate);
        if (key) {
          if (!summaryKeysByNoticeDate.has(noticeDate)) {
            summaryKeysByNoticeDate.set(noticeDate, new Set<string>());
          }
          summaryKeysByNoticeDate.get(noticeDate)!.add(key);
        }
      }

      if (noticeDate && securityCode) {
        const dateKey = noticeDate;
        if (!/^\d{4}-\d{2}-\d{2}$/.test(dateKey)) {
          continue;
        }

        let dateScope = noticeDateCodeMap.get(dateKey);
        if (!dateScope) {
          dateScope = { codes: new Set<string>(), begin: noticeDate, end: noticeDate };
          noticeDateCodeMap.set(dateKey, dateScope);
        }
        dateScope.codes.add(securityCode);
        if (noticeDate < dateScope.begin) {
          dateScope.begin = noticeDate;
        }
        if (noticeDate > dateScope.end) {
          dateScope.end = noticeDate;
        }
      }
    }

    if (summaryKeySet.size === 0 || noticeDateCodeMap.size === 0 || noticeDates.length === 0) {
      throw new Error('机构调研汇总增强失败：缺少可用于关联详情的事件键或日期范围');
    }

    const sortedNoticeDates = [...noticeDates].sort();
    const minNoticeDate = sortedNoticeDates[0];
    const maxNoticeDate = sortedNoticeDates[sortedNoticeDates.length - 1];
    const monthKeys = Array.from(noticeDateCodeMap.keys()).sort();

    this.reportProgress(
      metadataId,
      progressStart,
      `正在补充机构调研详情(main_content)，日期范围 ${minNoticeDate}~${maxNoticeDate}，共 ${monthKeys.length} 个日期段`
    );

    const detailMap = new Map<string, Array<{ content: string; url: string; organization: string }>>();
    // RPT_ORG_SURVEY 详情接口在当前环境中 pageSize >= 100 时容易返回 9701（服务器繁忙）
    // 使用更稳妥的页大小，优先保证可用性
    const pageSize = 50;
    const securityCodeChunkSize = 400;
    // 实测 RPT_ORG_SURVEY 在使用 SECURITY_CODE in (...) 条件时容易返回 9701，
    // 改为按 NOTICE_DATE 范围抓取后在本地匹配事件键，提升稳定性。
    const useSecurityCodeInFilter = false;
    const detailUrl = 'https://datacenter-web.eastmoney.com/api/data/v1/get';
    const progressRange = Math.max(progressEnd - progressStart, 1);
    let totalDetailFetched = 0;
    let totalDetailPageCount = 0;
    let totalDetailMatchedRows = 0;
    const totalMatchedEventSet = new Set<string>();

    for (let monthIdx = 0; monthIdx < monthKeys.length; monthIdx++) {
      this.checkCancelled(metadataId);

      const dateKey = monthKeys[monthIdx];
      const dateScope = noticeDateCodeMap.get(dateKey);
      if (!dateScope) {
        continue;
      }

      const rangeBegin = dateScope.begin;
      const rangeEnd = dateScope.end;
      const rangeTargetKeySet = new Set<string>();
      for (const [noticeDate, keySet] of summaryKeysByNoticeDate.entries()) {
        if (noticeDate >= rangeBegin && noticeDate <= rangeEnd) {
          for (const key of keySet) {
            rangeTargetKeySet.add(key);
          }
        }
      }
      const securityCodes = Array.from(dateScope.codes);
      if (securityCodes.length === 0) {
        continue;
      }

      const securityCodeChunks = useSecurityCodeInFilter
        ? this.splitIntoChunks(securityCodes, securityCodeChunkSize)
        : [securityCodes];
      let monthDetailFetched = 0;
      let monthDetailPageCount = 0;
      let monthDetailMatchedRows = 0;
      const monthMatchedEventSet = new Set<string>();

      for (let chunkIdx = 0; chunkIdx < securityCodeChunks.length; chunkIdx++) {
        const codeChunk = securityCodeChunks[chunkIdx];
        if (!codeChunk.length) {
          continue;
        }

        const codeFilter = codeChunk.map(code => `"${code}"`).join(',');
        const baseFilter = `(IS_SOURCE=\"1\")(NOTICE_DATE>='${rangeBegin}')(NOTICE_DATE<='${rangeEnd}')`;
        const detailFilter = useSecurityCodeInFilter
          ? `${baseFilter}(SECURITY_CODE in (${codeFilter}))`
          : baseFilter;
        let page = 1;
        let totalPages = 1;

        while (page <= totalPages) {
          this.checkCancelled(metadataId);

          const params: Record<string, any> = {
            reportName: 'RPT_ORG_SURVEY',
            sortColumns: 'NOTICE_DATE,RECEIVE_START_DATE,SECURITY_CODE',
            sortTypes: '-1,-1,-1',
            columns: 'SECURITY_CODE,NOTICE_DATE,RECEIVE_START_DATE,RECEIVE_OBJECT,CONTENT,URL',
            source: 'WEB',
            client: 'WEB',
            pageNumber: page,
            pageSize,
            filter: detailFilter
          };

          let response: any;
          try {
            response = await this.makeRequestWithRetry(async () => {
              const resp = await axios.get(detailUrl, {
                params,
                headers: requestHeaders,
                timeout: 30000,
                httpsAgent,
                httpAgent
              });

              const body = resp.data;
              if (!body || body.success !== true || !body.result) {
                const apiCode = body?.code;
                const apiMessage = body?.message || '详情接口返回异常';
                const detailError: any = new Error(`[JGDY_DETAIL] code=${apiCode ?? 'unknown'} message=${apiMessage}`);

                const messageText = String(apiMessage).toLowerCase();
                detailError.retryable =
                  apiCode === 9701 ||
                  messageText.includes('服务器繁忙') ||
                  messageText.includes('系统繁忙') ||
                  messageText.includes('busy') ||
                  messageText.includes('timeout');

                throw detailError;
              }

              return resp;
            }, 5, 800, ({ attempt, delayMs, error }) => {
              const errMsg = error?.response?.status
                ? `HTTP ${error.response.status}`
                : (error?.code || error?.message || '未知错误');
              this.reportProgress(
                metadataId,
                Math.min(progressEnd, progressStart + Math.round(((monthIdx + 1) / monthKeys.length) * progressRange)),
                `详情接口第 ${page}/${totalPages} 页请求异常（${rangeBegin}~${rangeEnd}，原因：${errMsg}），${Math.round(delayMs / 1000)} 秒后第 ${attempt} 次重试`
              );
            }, 30000);
          } catch (error) {
            if (error instanceof CrawlerCancelledError) {
              throw error;
            }

            const errMsg = (error as Error)?.message || '未知错误';
            console.warn(`[Crawler] 机构调研详情补充请求失败，跳过当前代码块: ${errMsg}`);
            this.reportProgress(
              metadataId,
              Math.min(progressEnd, progressStart + Math.round(((monthIdx + 1) / monthKeys.length) * progressRange)),
              `机构调研详情请求失败，跳过当前代码块继续（${rangeBegin}~${rangeEnd}，代码块 ${chunkIdx + 1}/${securityCodeChunks.length}，原因：${errMsg}）`
            );
            break;
          }

          const list = this.extractData(response.data, 'result.data');
          if (!list || list.length === 0) {
            break;
          }

          totalPages = this.extractValue(response.data, 'result.pages') || 1;
          const fetchedCount = list.length;
          totalDetailFetched += fetchedCount;
          totalDetailPageCount += 1;
          monthDetailFetched += fetchedCount;
          monthDetailPageCount += 1;

          let pageMatchedRows = 0;
          const pageMatchedEventSet = new Set<string>();

          for (const item of list) {
            const detailRow = item as Record<string, any>;
            const eventKey = this.buildJgdyEventKey(detailRow);
            if (!eventKey || !summaryKeySet.has(eventKey)) {
              continue;
            }

            const contentRaw = this.getValueByCandidates(detailRow, ['CONTENT', 'content']);
            const urlRaw = this.getValueByCandidates(detailRow, ['URL', 'url']);
            const orgRaw = this.getValueByCandidates(detailRow, ['RECEIVE_OBJECT', 'receive_object']);

            const content = contentRaw === null || contentRaw === undefined ? '' : String(contentRaw).trim();
            const url = urlRaw === null || urlRaw === undefined ? '' : String(urlRaw).trim();
            const organization = orgRaw === null || orgRaw === undefined ? '' : String(orgRaw).trim();

            const bucket = detailMap.get(eventKey) || [];
            bucket.push({ content, url, organization });
            detailMap.set(eventKey, bucket);
            pageMatchedRows += 1;
            pageMatchedEventSet.add(eventKey);
          }

          totalDetailMatchedRows += pageMatchedRows;
          monthDetailMatchedRows += pageMatchedRows;

          for (const matchedEventKey of pageMatchedEventSet) {
            monthMatchedEventSet.add(matchedEventKey);
            totalMatchedEventSet.add(matchedEventKey);
          }

          const chunkProgress = (chunkIdx + (page / Math.max(totalPages, 1))) / Math.max(securityCodeChunks.length, 1);
          const progress = progressStart + Math.round(((monthIdx + chunkProgress) / monthKeys.length) * progressRange);
          this.reportProgress(
            metadataId,
            Math.min(progress, progressEnd),
            `正在拉取机构调研详情(${monthIdx + 1}/${monthKeys.length})，范围 ${rangeBegin}~${rangeEnd}，代码块 ${chunkIdx + 1}/${securityCodeChunks.length}，第 ${page}/${totalPages} 页，本页 ${fetchedCount} 条，累计拉取详情明细 ${totalDetailFetched} 条（本范围 ${monthDetailFetched} 条），累计请求页 ${totalDetailPageCount} 页，已覆盖基础事件 ${totalMatchedEventSet.size}/${summaryKeySet.size} 个`
          );

          if (rangeTargetKeySet.size > 0) {
            let allRangeTargetsMatched = true;
            for (const key of rangeTargetKeySet) {
              if (!monthMatchedEventSet.has(key)) {
                allRangeTargetsMatched = false;
                break;
              }
            }

            if (allRangeTargetsMatched) {
              this.reportProgress(
                metadataId,
                Math.min(progress, progressEnd),
                `范围 ${rangeBegin}~${rangeEnd} 的基础事件已全部覆盖 ${monthMatchedEventSet.size}/${rangeTargetKeySet.size} 个，提前结束该范围后续分页`
              );
              break;
            }
          }

          page += 1;
          await this.sleep(30);
        }
      }

      const progress = progressStart + Math.round(((monthIdx + 1) / monthKeys.length) * progressRange);
      this.reportProgress(
        metadataId,
        Math.min(progress, progressEnd),
        `机构调研详情范围 ${rangeBegin}~${rangeEnd} 拉取完成：本范围详情明细 ${monthDetailFetched} 条（${monthDetailPageCount} 页），覆盖基础事件 ${monthMatchedEventSet.size} 个；累计详情明细 ${totalDetailFetched} 条（${totalDetailPageCount} 页），累计覆盖基础事件 ${totalMatchedEventSet.size}/${summaryKeySet.size} 个`
      );
      await this.sleep(50);
    }

    const mainContentByKey = new Map<string, string>();
    for (const [eventKey, entries] of detailMap.entries()) {
      const uniqueContents = Array.from(new Set(entries.map(entry => entry.content).filter(Boolean)));
      const uniqueUrls = Array.from(new Set(entries.map(entry => entry.url).filter(Boolean)));
      const uniqueOrganizations = Array.from(new Set(entries.map(entry => entry.organization).filter(Boolean)));

      // 同一事件可能存在多条不同 CONTENT，避免因此中断主流程：
      // 1) 选取最长内容作为主 CONTENT
      // 2) 同时保留 CONTENT_LIST 便于追溯
      const primaryContent = uniqueContents.length > 0
        ? uniqueContents.reduce((maxContent, currentContent) => {
            return currentContent.length > maxContent.length ? currentContent : maxContent;
          }, '')
        : '';

      if (uniqueContents.length > 1) {
        console.warn(`[Crawler] 机构调研详情事件存在多条 CONTENT，已自动合并: ${eventKey}, 条数=${uniqueContents.length}`);
      }

      const payload: Record<string, any> = {
        CONTENT: primaryContent,
        URL: uniqueUrls[0] || '',
        机构名单: uniqueOrganizations
      };

      if (uniqueContents.length > 1) {
        payload.CONTENT_LIST = uniqueContents;
      }

      mainContentByKey.set(eventKey, JSON.stringify(payload));
    }

    const updates: JgdyMainContentUpdate[] = [];

    this.reportProgress(
      metadataId,
      progressEnd,
      `开始将详情明细按事件键左关联到基础表：详情事件 ${mainContentByKey.size} 个，基础事件 ${summaryKeySet.size} 个`
    );

    for (const row of summaryData) {
      const eventKey = this.buildJgdyEventKey(row);
      if (!eventKey) {
        continue;
      }

      const mainContent = mainContentByKey.get(eventKey);
      if (!mainContent) {
        continue;
      }

      const securityCodeRaw = this.getValueByCandidates(row, ['security_code', 'SECURITY_CODE']);
      const receiveStartDateRaw = this.getValueByCandidates(row, ['receive_start_date', 'RECEIVE_START_DATE']);
      const noticeDateRaw = this.getValueByCandidates(row, ['notice_date', 'NOTICE_DATE']);

      const securityCode = securityCodeRaw === null || securityCodeRaw === undefined ? '' : String(securityCodeRaw).trim();
      const receiveStartDate = this.normalizeDateValue(receiveStartDateRaw);
      const noticeDate = this.normalizeDateValue(noticeDateRaw);

      if (!securityCode || !receiveStartDate || !noticeDate) {
        continue;
      }

      updates.push({
        securityCode,
        receiveStartDate,
        noticeDate,
        mainContent
      });
    }

    if (updates.length === 0) {
      this.reportProgress(metadataId, progressEnd, '机构调研详情暂未补充成功，先使用汇总主数据继续流程');
      return [];
    }

    this.reportProgress(
      metadataId,
      progressEnd,
      `机构调研详情补充完成：累计拉取详情明细 ${totalDetailFetched} 条（${totalDetailPageCount} 页），覆盖基础事件 ${totalMatchedEventSet.size}/${summaryKeySet.size} 个，左关联可回填 ${updates.length}/${summaryData.length} 条`
    );
    return updates;
  }

  private async enrichJgdySummaryWithDetail(
    metadataId: number,
    summaryData: Record<string, any>[],
    headers: Record<string, string>
  ): Promise<Record<string, any>[]> {
    let updates: JgdyMainContentUpdate[] = [];
    try {
      updates = await this.buildJgdySummaryMainContentUpdates(metadataId, summaryData, headers, {
        progressStart: 91,
        progressEnd: 94
      });
    } catch (error) {
      if (error instanceof CrawlerCancelledError) {
        throw error;
      }

      console.warn(`[Crawler] 机构调研详情补充失败，回退为空详情继续入库: ${(error as Error).message}`);
      this.reportProgress(metadataId, 94, '机构调研详情暂不可用，已回退为空详情继续入库');
      updates = [];
    }

    const mainContentByKey = new Map<string, string>();
    for (const item of updates) {
      const key = `${item.securityCode}|${item.receiveStartDate}|${item.noticeDate}`;
      mainContentByKey.set(key, item.mainContent);
    }

    const enhancedSummaryData = summaryData.map(row => {
      const eventKey = this.buildJgdyEventKey(row);
      const mainContent = eventKey ? mainContentByKey.get(eventKey) : undefined;

      return {
        ...row,
        main_content: mainContent || JSON.stringify({ CONTENT: '', URL: '', 机构名单: [] })
      };
    });

    return enhancedSummaryData;
  }

  /**
   * 报告进度
   */
  reportProgress(metadataId: number, progress: number, message: string): void {
    broadcastProgress(createProgressMessage('progress', metadataId, progress, message));
  }

  /**
   * 报告完成
   */
  reportComplete(metadataId: number, message: string): void {
    broadcastProgress(createProgressMessage('complete', metadataId, 100, message));
  }

  /**
   * 报告错误
   */
  reportError(metadataId: number, message: string): void {
    broadcastProgress(createProgressMessage('error', metadataId, 0, message));
  }

  /**
   * 通用的按日期字段采集方法
   * 根据 metadata 中的 date_field 和 date_range 自动选择合适的采集方法
   * @param metadata 接口元数据（包含 date_field、date_range、future_days 和 fixed_begin_date）
   * @returns 采集到的数据数组
   */
  async fetchDataByDateField(metadata: ApiMetadata, options?: { skipJgdyMainContent?: boolean }): Promise<any[]> {
    const dateRange = metadata.date_range;
    const futureDays = (metadata as any).future_days || 0;
    const fixedBeginDate = (metadata as any).fixed_begin_date || null;

    console.log(`[Crawler] 通用采集: table=${metadata.table_name}, dateRange=${dateRange}, futureDays=${futureDays}, fixedBeginDate=${fixedBeginDate}`);

    // 使用数据库配置进行采集
    const config = this.getApiRequestConfig(metadata);
    if (config && config.apiType) {
      console.log(`[Crawler] 使用数据库配置: apiType=${config.apiType}`);
      return this.fetchByApiConfig(metadata, {
        dateRange: dateRange || undefined,
        futureDays,
        fixedBeginDate,
        skipJgdyMainContent: options?.skipJgdyMainContent
      });
    }

    // 没有配置，报错
    throw new Error(`表 ${metadata.table_name} 缺少 datacenter_config 配置`);
  }

  /**
   * 按日期范围采集数据
   * @param metadata 接口元数据
   * @param beginDate 开始日期，格式：YYYY-MM-DD
   * @param endDate 结束日期，格式：YYYY-MM-DD
   * @returns 采集到的数据数组
   */
  async fetchDataByDateRange(metadata: ApiMetadata, beginDate: string, endDate: string): Promise<any[]> {
    console.log(`[Crawler] 日期范围采集: table=${metadata.table_name}, 日期范围: ${beginDate} ~ ${endDate}`);

    // 计算日期范围天数
    const start = new Date(beginDate);
    const end = new Date(endDate);
    const dateRange = Math.ceil((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24));

    // 使用数据库配置进行采集
    const config = this.getApiRequestConfig(metadata);
    if (config && config.apiType) {
      console.log(`[Crawler] 使用数据库配置: apiType=${config.apiType}`);
      return this.fetchByApiConfig(metadata, { dateRange });
    }

    // 没有配置，报错
    throw new Error(`表 ${metadata.table_name} 缺少 datacenter_config 配置`);
  }
}

export const crawlerService = new CrawlerService();
