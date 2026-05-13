/**
 * 全量更新服务模块
 * 负责数据的全量更新操作
 * 支持增量和全量模式
 */
import { metadataService } from './metadataService';
import { tableService } from './tableService';
import { crawlerService, CrawlerCancelledError } from './crawler';
import { updateLogService } from './updateLogService';
import { dbMaintenanceService } from './dbMaintenanceService';
import { broadcastProgress, createProgressMessage } from '../websocket';
import type { DailyStatItem, ApiMetadata, UpdateMode as UpdateModeType } from '../models/types';

/**
 * 内部更新模式枚举
 * - REPLACE: 使用 INSERT OR REPLACE（增量更新，需要唯一键）
 * - REPLACE: 使用 INSERT OR REPLACE（增量更新，需要唯一键）
 * - DELETE_INSERT: 先删除再插入（全量更新）
 * - DELETE_RANGE_INSERT: 删除指定时间段数据再插入
 */
type InternalUpdateMode = 'REPLACE' | 'DELETE_INSERT' | 'DELETE_RANGE_INSERT';

/**
 * 获取表的默认唯一键
 * @param tableName 表名
 */
function getDefaultUniqueKey(tableName: string): string | undefined {
  const defaultKeys: Record<string, string> = {
    'sse_ehudong': 'qaId',
    'cninfo_stock': 'code',
    'eastmoney_announcement': 'art_code',
    'eastmoney_announcement_raw': 'art_code'
  };
  return defaultKeys[tableName];
}

/**
 * 根据结构化字段获取更新配置
 * @param metadata 接口元数据
 * @returns 内部更新模式和唯一键
 */
function getUpdateConfig(metadata: ApiMetadata): {
  mode: InternalUpdateMode;
  uniqueKey?: string;
  dedupeKey?: string;  // 用于去重的键（即使是全量模式也需要）
} {
  // 从结构化字段读取更新模式
  const updateMode = metadata.update_mode || 'full';

  // 映射更新模式
  let mode: InternalUpdateMode;
  if (updateMode === 'incremental') {
    mode = 'REPLACE';
  } else if (updateMode === 'delete_range_insert') {
    mode = 'DELETE_RANGE_INSERT';
  } else {
    mode = 'DELETE_INSERT';
  }

  let uniqueKey: string | undefined;
  let dedupeKey: string | undefined;

  // 首先尝试从 output_config 中读取唯一键配置
  if (metadata.output_config) {
    try {
      const config = JSON.parse(metadata.output_config);
      // 查找所有标记为唯一键的字段（支持 unique 或 is_unique_key）
      const uniqueFields = config.filter((c: any) => c.unique === true || c.is_unique_key === true);
      if (uniqueFields.length > 0) {
        // 将多个唯一键字段组合成逗号分隔的字符串
        dedupeKey = uniqueFields.map((f: any) => f.name).join(',');
      }
    } catch (e) {
      // 解析失败，继续使用默认配置
    }
  }

  // 如果没有从配置中获取到，使用表名的默认配置
  if (!dedupeKey) {
    dedupeKey = getDefaultUniqueKey(metadata.table_name);
  }

  // 指定接口按需求保留全部记录，不做更新前去重
  const noDedupeTables = new Set([
    'eastmoney_executive_raw',
    'sse_ehudong',
    'eastmoney_announcement_raw'
  ]);
  if (noDedupeTables.has(metadata.table_name)) {
    dedupeKey = undefined;
  }

  // 如果是增量更新模式，需要设置 uniqueKey 用于 INSERT OR REPLACE
  if (mode === 'REPLACE') {
    uniqueKey = dedupeKey;

    // 如果仍然没有唯一键，回退到全量更新
    if (!uniqueKey) {
      console.warn(`[UpdateService] 表 ${metadata.table_name} 没有配置唯一键，回退到全量更新模式`);
      mode = 'DELETE_INSERT';
    }
  }

  return { mode, uniqueKey, dedupeKey };
}

/**
 * 全量更新服务类
 */
export class UpdateService {
  private isJgdySummaryMetadata(metadata: ApiMetadata): boolean {
    return metadata.table_name === 'biz_eastmoney_jgdy_summary';
  }

  private getRowValue(row: Record<string, any>, candidates: string[]): any {
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
    const securityCodeRaw = this.getRowValue(row, ['SECURITY_CODE', 'security_code']);
    const receiveStartDateRaw = this.getRowValue(row, ['RECEIVE_START_DATE', 'receive_start_date']);
    const noticeDateRaw = this.getRowValue(row, ['NOTICE_DATE', 'notice_date']);

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

  private hasUsefulJgdyMainContentValue(value: any): boolean {
    if (value === null || value === undefined) {
      return false;
    }

    const rawText = String(value).trim();
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
      return Array.isArray(contentList) && contentList.some(item => String(item || '').trim());
    } catch {
      return true;
    }
  }

  private collectExistingJgdyMainContent(tableName: string): Map<string, string> {
    const existing = tableService.queryAllData(tableName);
    const contentByKey = new Map<string, string>();

    for (const row of existing.data) {
      const key = this.buildJgdyEventKey(row);
      if (!key) {
        continue;
      }

      const mainContent = this.getRowValue(row, ['main_content', 'MAIN_CONTENT']);
      if (!this.hasUsefulJgdyMainContentValue(mainContent)) {
        continue;
      }

      contentByKey.set(key, String(mainContent));
    }

    return contentByKey;
  }

  private restoreJgdyMainContent(
    rows: Record<string, any>[],
    contentByKey: Map<string, string>
  ): number {
    if (rows.length === 0 || contentByKey.size === 0) {
      return 0;
    }

    let restored = 0;
    for (const row of rows) {
      const key = this.buildJgdyEventKey(row);
      if (!key) {
        continue;
      }

      const mainContent = contentByKey.get(key);
      if (!mainContent) {
        continue;
      }

      row.main_content = mainContent;
      restored += 1;
    }

    return restored;
  }

  /**
   * 预览更新数据量
   * @param metadataId 元数据 ID
   * @returns 预览信息
   */
  async previewUpdate(metadataId: number): Promise<{
    currentCount: number;
    estimatedNewCount: number;
    updateMode: string;
    dateRange: number | null;
    dateField: string | null;
  }> {
    console.log(`[UpdateService] 预览更新，metadataId: ${metadataId}`);

    // 获取元数据
    const metadata = await metadataService.getMetadataById(metadataId);
    const tableName = metadata.table_name;
    const { mode } = getUpdateConfig(metadata);
    const dateField = metadata.date_field;
    const dateRange = metadata.date_range;

    // 查询当前数据库中的数据量
    let currentCount = 0;

    if (mode === 'DELETE_RANGE_INSERT' && dateField && dateRange) {
      // 区间删除模式：统计将被删除的数据量（最近N天）
      const now = new Date();
      const beijingNow = new Date(now.getTime() + 8 * 60 * 60 * 1000);
      const targetDate = new Date(beijingNow);
      targetDate.setDate(targetDate.getDate() - dateRange);
      const beginDate = targetDate.toISOString().split('T')[0];

      currentCount = tableService.countDataByDateRange(tableName, dateField, beginDate);
    } else if (mode === 'DELETE_INSERT') {
      // 全量删除模式：统计所有数据
      currentCount = tableService.countAllData(tableName);
    }

    // 预估新数据量（调用API获取第一页，查看总数）
    let estimatedNewCount = 0;
    try {
      // 这里可以调用 crawler 的方法获取预估数量
      // 暂时返回 0，表示无法预估
      estimatedNewCount = 0;
    } catch (error) {
      console.warn(`[UpdateService] 无法预估新数据量:`, error);
    }

    return {
      currentCount,
      estimatedNewCount,
      updateMode: mode,
      dateRange,
      dateField
    };
  }

  /**
   * 执行全量更新
   * @param metadataId 元数据 ID
   * @param isFullMode 是否为全量更新模式（采集所有历史数据）
   * @param isTrueFullMode 是否为真正全量模式（不带日期过滤器，获取所有历史数据）
   */
  async performFullUpdate(
    metadataId: number,
    isFullMode: boolean = false,
    isTrueFullMode: boolean = false,
    skipJgdyMainContent: boolean = false
  ): Promise<void> {
    const startTime = Date.now();
    const modeDesc = isTrueFullMode ? '真正全量（无日期限制）' : (isFullMode ? '全量（10年）' : '增量');
    console.log(`[UpdateService] 开始${modeDesc}更新，metadataId: ${metadataId}`);

    // 获取元数据
    const metadata = await metadataService.getMetadataById(metadataId);
    const tableName = metadata.table_name;

    // 从结构化字段获取更新配置
    const { mode, uniqueKey, dedupeKey } = getUpdateConfig(metadata);

    // 从结构化字段读取日期字段和天数范围
    const dateField = metadata.date_field;
    let dateRange = metadata.date_range;

    // 如果是真正全量更新模式，不使用日期范围（通过特殊标记）
    if (isTrueFullMode) {
      dateRange = -1; // 特殊标记：-1 表示真正全量，不使用日期过滤
      console.log(`[UpdateService] 真正全量更新模式：将采集所有历史数据（不带日期过滤器）`);
    }
    // 如果是10年全量更新模式，覆盖日期范围为10年（3650天）
    else if (isFullMode && typeof dateRange === 'number' && dateRange > 0) {
      dateRange = 3650; // 10年历史数据
      console.log(`[UpdateService] 10年全量更新模式：将采集最近 ${dateRange} 天的数据`);
    }
    console.log(`[UpdateService] 更新模式: ${mode}, 唯一键: ${uniqueKey || '无'}, 去重键: ${dedupeKey || '无'}, 日期字段: ${dateField || '无'}, 天数范围: ${dateRange || '无'}`);

    // 如果是增量更新模式且有唯一键，确保表有唯一索引
    if (mode === 'REPLACE' && uniqueKey) {
      tableService.ensureUniqueIndex(tableName, uniqueKey);
    }

    // 获取更新前的数据（用于回滚，仅在全量更新模式时需要）
    const beforeData = mode === 'REPLACE' ? { data: [], total: 0 } : tableService.queryData(tableName, 1, 100000);
    const shouldPreserveJgdyMainContent = this.isJgdySummaryMetadata(metadata) && skipJgdyMainContent;
    const preservedJgdyMainContent = shouldPreserveJgdyMainContent
      ? this.collectExistingJgdyMainContent(tableName)
      : new Map<string, string>();

    try {
      // 报告开始更新
      broadcastProgress(createProgressMessage('progress', metadataId, 5, '开始全量更新...'));

      // 步骤 1：根据更新模式决定是否删除现有数据
      if (shouldPreserveJgdyMainContent && preservedJgdyMainContent.size > 0) {
        broadcastProgress(createProgressMessage(
          'progress',
          metadataId,
          8,
          `已缓存 ${preservedJgdyMainContent.size} 条历史 main_content，基础采集后将按事件键保留`
        ));
      }

      let beginDate: string | undefined;

      if (mode === 'DELETE_INSERT') {
        // 全量更新模式：先删除所有数据
        broadcastProgress(createProgressMessage('progress', metadataId, 10, '正在清空现有数据...'));
        tableService.truncateTable(tableName);
      } else if (mode === 'DELETE_RANGE_INSERT') {
        // 区间删除模式：删除最近N天的数据
        if (!dateField || !dateRange) {
          throw new Error(`[UpdateService] DELETE_RANGE_INSERT 模式需要配置 date_field 和 date_range`);
        }

        // 计算开始日期 (Today - dateRange)
        const now = new Date();
        const beijingNow = new Date(now.getTime() + 8 * 60 * 60 * 1000);
        const targetDate = new Date(beijingNow);
        targetDate.setDate(targetDate.getDate() - dateRange);
        beginDate = targetDate.toISOString().split('T')[0];

        broadcastProgress(createProgressMessage('progress', metadataId, 10, `正在删除 ${beginDate} 之后的数据...`));
        tableService.deleteDataByDateRange(tableName, dateField, beginDate);
      } else {
        // 增量更新模式：不删除数据，使用 INSERT OR REPLACE
        broadcastProgress(createProgressMessage('progress', metadataId, 10, '使用增量更新模式...'));
      }

      // 步骤 2：根据结构化字段采集数据
      let newData: any[] = [];

      // 保存原始日期配置
      const originalDateRange = metadata.date_range;
      const originalFixedBeginDate = (metadata as any).fixed_begin_date || null;
      const hasConfiguredDateScope =
        Boolean(originalFixedBeginDate) ||
        (typeof originalDateRange === 'number' && Number.isFinite(originalDateRange));

      // 根据更新模式决定是否调整日期范围
      if (isTrueFullMode) {
        // 真正全量：不限制日期
        (metadata as any).date_range = -1;
        (metadata as any).fixed_begin_date = null;
        console.log('[UpdateService] truly-full mode: unbounded date scope');
      } else if (mode === 'DELETE_INSERT') {
        if (hasConfiguredDateScope) {
          // 常规全量：保留元数据中配置的范围（例如 today-40d）
          (metadata as any).date_range = dateRange;
          (metadata as any).fixed_begin_date = originalFixedBeginDate;
          console.log('[UpdateService] delete-insert mode: using configured date scope');
        } else {
          // 未配置范围时，才回退为不限制日期
          (metadata as any).date_range = -1;
          (metadata as any).fixed_begin_date = null;
          console.log('[UpdateService] delete-insert mode: no date scope configured, fallback to unbounded');
        }
      } else if ((isFullMode || isTrueFullMode) && typeof dateRange === 'number') {
        (metadata as any).date_range = dateRange;
      }

      // 统一使用 fetchDataByDateField，它会根据日期字段和表名判断使用哪个采集方法
      console.log(`[UpdateService] 调用通用采集方法: dateField=${dateField}, dateRange=${(metadata as any).date_range}`);
      broadcastProgress(createProgressMessage('progress', metadataId, 20, '正在采集数据...'));

      newData = await crawlerService.fetchDataByDateField(metadata, { skipJgdyMainContent });

      // 恢复原始日期配置
      (metadata as any).date_range = originalDateRange;
      (metadata as any).fixed_begin_date = originalFixedBeginDate;

      console.log(`[UpdateService] 采集完成，数据条数: ${newData.length}`);

      // 获取期望的日期列表（用于补全没有数据的日期）
      // 使用结构化字段，包含过去天数和未来天数
      const futureDays = (metadata as any).future_days || 0;
      const expectedDates = (typeof dateRange === 'number' && dateRange > 0)
        ? this.getDateRange(dateRange, futureDays)
        : [];

      if (newData.length === 0) {
        // 记录更新日志（无新数据，但仍然显示期望日期的统计）
        const emptyStats = expectedDates.map(date => ({ date, count: 0 }));
        const duration = Date.now() - startTime;
        updateLogService.createLog(metadataId, 0, emptyStats, 'success', null, duration);
        broadcastProgress(createProgressMessage('complete', metadataId, 100, '采集完成，无新数据'));
        await metadataService.updateLastUpdateTime(metadataId);
        return;
      }

      // 步骤 3：数据去重（使用去重键，即使是全量模式也需要去重以避免唯一索引冲突）
      let deduplicatedData = newData;
      if (dedupeKey) {
        const dedupeKeys = dedupeKey.split(',').map(k => k.trim());
        const seen = new Set<string>();
        deduplicatedData = newData.filter(row => {
          const key = dedupeKeys.map(k => row[k]).join('|');
          if (seen.has(key)) {
            return false;
          }
          seen.add(key);
          return true;
        });

        const duplicateCount = newData.length - deduplicatedData.length;
        if (duplicateCount > 0) {
          console.log(`[UpdateService] 去重: 原始 ${newData.length} 条，去重后 ${deduplicatedData.length} 条，移除 ${duplicateCount} 条重复数据`);
        }
      }

      // 步骤 4：插入新数据
      broadcastProgress(createProgressMessage('progress', metadataId, 70, `正在插入 ${deduplicatedData.length} 条数据...`));
      if (shouldPreserveJgdyMainContent && preservedJgdyMainContent.size > 0) {
        const restoredMainContentCount = this.restoreJgdyMainContent(deduplicatedData, preservedJgdyMainContent);
        if (restoredMainContentCount > 0) {
          broadcastProgress(createProgressMessage(
            'progress',
            metadataId,
            68,
            `已按事件键保留 ${restoredMainContentCount} 条历史 main_content`
          ));
        }
      }

      tableService.insertData(tableName, deduplicatedData, metadataId, uniqueKey);

      // 步骤 5：统计每日数据量（传入期望的日期列表和配置的日期字段）
      const dailyStats = this.calculateDailyStats(deduplicatedData, expectedDates, dateField || undefined);

      // 步骤 6：记录更新日志
      const duration = Date.now() - startTime;
      updateLogService.createLog(metadataId, deduplicatedData.length, dailyStats, 'success', null, duration);

      // 步骤 7：更新最后更新时间
      broadcastProgress(createProgressMessage('progress', metadataId, 95, '正在更新时间戳...'));
      await metadataService.updateLastUpdateTime(metadataId);

      // 步骤 8：保存接口实际返回的字段列表
      const rawFields = crawlerService.getLastRawFields();
      if (rawFields.length > 0) {
        await metadataService.updateLastApiFields(metadataId, rawFields);
        console.log(`[UpdateService] 已记录接口返回字段: ${rawFields.length} 个`);
      }

      // 报告完成
      const duplicateCount = newData.length - deduplicatedData.length;
      const message = duplicateCount > 0
        ? `更新完成，共 ${deduplicatedData.length} 条数据（去重 ${duplicateCount} 条）`
        : `更新完成，共 ${deduplicatedData.length} 条数据`;
      broadcastProgress(createProgressMessage('complete', metadataId, 100, message));

      // 成功完成后清除取消标记
      crawlerService.clearCancel(metadataId);

    } catch (error) {
      // 清除取消标记
      crawlerService.clearCancel(metadataId);

      // 检查是否是用户取消
      if (error instanceof CrawlerCancelledError) {
        console.log(`[UpdateService] 采集已被用户取消 [${metadata.cn_name}]`);
        const duration = Date.now() - startTime;
        updateLogService.createLog(metadataId, 0, [], 'cancelled', '用户取消', duration);

        // 对于会先删除再写入的模式，取消时必须回滚，避免表被清空后停留在空状态
        if (mode !== 'REPLACE') {
          try {
            broadcastProgress(createProgressMessage('progress', metadataId, 0, '已收到取消请求，正在回滚原始数据...'));
            tableService.truncateTable(tableName);
            if (beforeData.data.length > 0) {
              tableService.insertData(tableName, beforeData.data, metadataId);
            }
            broadcastProgress(createProgressMessage('error', metadataId, 0, '采集已取消，原始数据已回滚恢复'));
          } catch (rollbackError) {
            console.error('取消后回滚失败:', rollbackError);
            broadcastProgress(createProgressMessage('error', metadataId, 0, '采集已取消，但回滚失败，请手动重试更新'));
          }
        } else {
          broadcastProgress(createProgressMessage('error', metadataId, 0, '采集已被用户取消'));
        }

        return;
      }

      // 发生错误，尝试回滚
      console.error(`全量更新失败 [${metadata.cn_name}]:`, error);

      // 记录错误日志
      const duration = Date.now() - startTime;
      updateLogService.createLog(metadataId, 0, [], 'error', (error as Error).message, duration);

      try {
        // 增量更新模式不需要回滚（数据没有被删除）
        if (mode === 'REPLACE') {
          broadcastProgress(createProgressMessage('error', metadataId, 0, `更新失败: ${(error as Error).message}`));
        } else {
          // 全量更新模式，尝试回滚
          // 清空当前数据
          tableService.truncateTable(tableName);

          // 恢复原有数据
          if (beforeData.data.length > 0) {
            tableService.insertData(tableName, beforeData.data, metadataId);
          }

          broadcastProgress(createProgressMessage('error', metadataId, 0, `更新失败，已回滚: ${(error as Error).message}`));
        }
      } catch (rollbackError) {
        console.error('回滚失败:', rollbackError);
        broadcastProgress(createProgressMessage('error', metadataId, 0, `更新失败且回滚失败: ${(error as Error).message}`));
      }

      throw error;
    }
  }

  /**
   * 检查是否正在更新
   * 简单实现：使用内存标记
   */
  private updatingIds: Set<number> = new Set();

  isUpdating(metadataId: number): boolean {
    return this.updatingIds.has(metadataId);
  }

  /**
   * 是否存在任何进行中的更新任务（单个或批量）
   */
  hasActiveUpdates(): boolean {
    return this.batchUpdating || this.updatingIds.size > 0;
  }

  private scheduleAutoDbMaintenance(trigger: string): void {
    if (this.hasActiveUpdates()) {
      return;
    }

    setImmediate(() => {
      if (this.hasActiveUpdates()) {
        return;
      }

      dbMaintenanceService.maybeAutoCompact(trigger).catch((error) => {
        console.warn(`[UpdateService] 自动数据库维护触发失败 [${trigger}]`, (error as Error).message);
      });
    });
  }

  /**
   * 开始新任务前清理陈旧取消标记，避免误取消
   */
  private prepareCancelStateForStart(metadataId: number): void {
    // 没有任何任务在执行时，清空历史取消标记（包括 stop-all 的 -1）
    if (!this.hasActiveUpdates()) {
      crawlerService.clearAllCancels();
      return;
    }

    // 有其他任务在执行时，仅清理当前接口自己的陈旧取消标记
    crawlerService.clearCancel(metadataId);
  }

  /**
   * 获取最近N天的日期列表（使用北京时间）
   * @param days 天数
   * @returns 日期字符串数组（YYYY-MM-DD格式，降序排列）
   */
  private getRecentDays(days: number): string[] {
    return this.getDateRange(days, 0);
  }

  /**
   * 获取日期范围列表（过去N天 + 未来M天）
   * @param pastDays 过去天数
   * @param futureDays 未来天数
   * @returns 日期字符串数组（YYYY-MM-DD格式，降序排列）
   */
  private getDateRange(pastDays: number, futureDays: number): string[] {
    const result: string[] = [];
    // 获取当前北京时间
    const now = new Date();
    const beijingOffset = 8 * 60 * 60 * 1000; // UTC+8
    const beijingNow = new Date(now.getTime() + beijingOffset);

    // 先添加未来日期（从最远的未来日期开始）
    for (let i = futureDays; i >= 1; i--) {
      const currentDate = new Date(beijingNow);
      currentDate.setDate(currentDate.getDate() + i);
      const year = currentDate.getUTCFullYear();
      const month = String(currentDate.getUTCMonth() + 1).padStart(2, '0');
      const day = String(currentDate.getUTCDate()).padStart(2, '0');
      const dateStr = `${year}-${month}-${day}`;
      result.push(dateStr);
    }

    // 再添加过去日期（从今天开始往前）
    for (let i = 0; i < pastDays; i++) {
      const currentDate = new Date(beijingNow);
      currentDate.setDate(currentDate.getDate() - i);
      const year = currentDate.getUTCFullYear();
      const month = String(currentDate.getUTCMonth() + 1).padStart(2, '0');
      const day = String(currentDate.getUTCDate()).padStart(2, '0');
      const dateStr = `${year}-${month}-${day}`;
      result.push(dateStr);
    }

    // 按日期降序排列
    result.sort((a, b) => b.localeCompare(a));

    return result;
  }

  /**
   * 计算每日数据统计
   * 尝试从数据中提取日期字段并统计每日数量
   * @param data 数据数组
   * @param expectedDates 期望的日期列表（可选），用于补全没有数据的日期
   * @param configuredDateField 配置的日期字段（可选），优先使用此字段
   */
  private calculateDailyStats(data: any[], expectedDates: string[] = [], configuredDateField?: string): DailyStatItem[] {
    // 统计每日数量
    const statsMap = new Map<string, number>();

    // 先用期望的日期初始化（全部为0）
    for (const date of expectedDates) {
      statsMap.set(date, 0);
    }

    if (data.length === 0) {
      // 如果没有数据但有期望日期，返回全0的统计
      if (expectedDates.length > 0) {
        const stats: DailyStatItem[] = expectedDates
          .map(date => ({ date, count: 0 }))
          .sort((a, b) => b.date.localeCompare(a.date));
        console.log(`[UpdateService] 无数据，返回期望日期统计: ${stats.length} 天`);
        return stats;
      }
      return [];
    }

    // 常见的日期字段名（按优先级排序）
    // notice_date 优先，因为股东增减持等数据应该按公告日期统计
    const dateFields = [
      'notice_date',  // 公告日期优先（股东增减持、高管增减持等）
      'tdate', 'date', 'trade_date',  // 交易日期
      'ann_date', 'change_date', 'rdate', 'end_date', 'start_date',
      'pubDate', 'updateDate', 'attachedPubDate',  // 互动易字段
      'createTime', 'update_time', 'publish_date', 'eitime',
      'report_date'
    ];

    // 找到第一个存在且有值的日期字段
    // 如果配置了日期字段，优先使用配置的字段
    let dateField: string | null = null;
    const firstItem = data[0];

    if (configuredDateField && firstItem[configuredDateField] !== undefined && firstItem[configuredDateField] !== null) {
      dateField = configuredDateField;
      console.log(`[UpdateService] 使用配置的日期字段: ${dateField}`);
    } else {
      for (const field of dateFields) {
        if (firstItem[field] !== undefined && firstItem[field] !== null && firstItem[field] !== '') {
          dateField = field;
          break;
        }
      }
    }

    if (!dateField) {
      // 没有找到日期字段，打印第一条数据的所有字段名用于调试
      console.log('[UpdateService] 未找到日期字段，数据字段:', Object.keys(firstItem));
      // 如果有期望日期，返回全0的统计
      if (expectedDates.length > 0) {
        return expectedDates
          .map(date => ({ date, count: 0 }))
          .sort((a, b) => b.date.localeCompare(a.date));
      }
      return [];
    }

    console.log(`[UpdateService] 使用日期字段: ${dateField}, 示例值: ${firstItem[dateField]}`);

    for (const item of data) {
      let dateValue = item[dateField];
      if (!dateValue) continue;

      // 标准化日期格式为 YYYY-MM-DD（使用北京时间）
      let dateStr: string;

      // 处理时间戳（毫秒）
      if (typeof dateValue === 'number' || (typeof dateValue === 'string' && /^\d{10,13}$/.test(dateValue))) {
        const timestamp = typeof dateValue === 'string' ? parseInt(dateValue) : dateValue;
        // 如果是10位数字，认为是秒级时间戳，转换为毫秒
        const ms = timestamp > 9999999999 ? timestamp : timestamp * 1000;
        const date = new Date(ms);
        // 使用北京时间（UTC+8）
        const beijingDate = new Date(date.getTime() + 8 * 60 * 60 * 1000);
        dateStr = beijingDate.toISOString().split('T')[0];
      } else if (typeof dateValue === 'string') {
        // 处理各种日期格式
        if (dateValue.includes('T')) {
          // ISO 格式: 2025-01-01T00:00:00
          dateStr = dateValue.split('T')[0];
        } else if (dateValue.includes(' ')) {
          // 带时间: 2025-01-01 12:00:00
          dateStr = dateValue.split(' ')[0];
        } else if (dateValue.length === 8 && !dateValue.includes('-')) {
          // 紧凑格式: 20250101
          dateStr = `${dateValue.slice(0, 4)}-${dateValue.slice(4, 6)}-${dateValue.slice(6, 8)}`;
        } else {
          // 已经是 YYYY-MM-DD 格式
          dateStr = dateValue;
        }
      } else if (dateValue instanceof Date) {
        dateStr = dateValue.toISOString().split('T')[0];
      } else {
        continue;
      }

      // 验证日期格式
      if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
        console.log(`[UpdateService] 无效日期格式: ${dateStr}`);
        continue;
      }

      statsMap.set(dateStr, (statsMap.get(dateStr) || 0) + 1);
    }

    // 转换为数组并按日期排序
    const stats: DailyStatItem[] = Array.from(statsMap.entries())
      .map(([date, count]) => ({ date, count }))
      .sort((a, b) => b.date.localeCompare(a.date)); // 降序排列

    console.log(`[UpdateService] 每日统计: ${stats.length} 天, 总计 ${data.length} 条`);

    return stats;
  }

  /**
   * 安全执行更新（防止重复更新）
   * @param metadataId 元数据 ID
   * @param isFullMode 是否为全量更新模式
   * @param isTrueFullMode 是否为真正全量模式（不带日期过滤器）
   */
  async safePerformFullUpdate(
    metadataId: number,
    isFullMode: boolean = false,
    isTrueFullMode: boolean = false,
    skipJgdyMainContent: boolean = false
  ): Promise<void> {
    if (this.isUpdating(metadataId)) {
      throw new Error('该接口正在更新中，请稍后再试');
    }

    this.prepareCancelStateForStart(metadataId);

    this.updatingIds.add(metadataId);

    try {
      await this.performFullUpdate(metadataId, isFullMode, isTrueFullMode, skipJgdyMainContent);
    } finally {
      this.updatingIds.delete(metadataId);
      this.scheduleAutoDbMaintenance(`full_update:${metadataId}`);
    }
  }

  async safeBackfillJgdyMainContent(metadataId: number): Promise<void> {
    if (this.isUpdating(metadataId)) {
      throw new Error('该接口正在更新中，请稍后再试');
    }

    this.prepareCancelStateForStart(metadataId);
    this.updatingIds.add(metadataId);

    try {
      await this.backfillJgdyMainContent(metadataId);
    } finally {
      this.updatingIds.delete(metadataId);
      this.scheduleAutoDbMaintenance(`jgdy_main_content:${metadataId}`);
    }
  }

  private async backfillJgdyMainContent(metadataId: number): Promise<void> {
    const metadata = await metadataService.getMetadataById(metadataId);

    if (!this.isJgdySummaryMetadata(metadata)) {
      throw new Error('该接口不是机构调研统计汇总，不能补充 main_content');
    }

    try {
      broadcastProgress(createProgressMessage('progress', metadataId, 5, '开始补充机构调研详情(main_content)...'));

      const result = tableService.queryAllData(metadata.table_name);
      if (result.data.length === 0) {
        broadcastProgress(createProgressMessage('complete', metadataId, 100, '当前表没有基础数据，请先执行基础采集'));
        crawlerService.clearCancel(metadataId);
        return;
      }

      broadcastProgress(createProgressMessage('progress', metadataId, 10, `已读取 ${result.data.length} 条基础数据，准备请求详情接口...`));

      const updates = await crawlerService.buildJgdySummaryMainContentUpdates(
        metadataId,
        result.data,
        undefined,
        { progressStart: 10, progressEnd: 95 }
      );

      const updateResult = tableService.updateJgdySummaryMainContent(metadata.table_name, updates);
      broadcastProgress(
        createProgressMessage(
          'complete',
          metadataId,
          100,
          `机构调研详情(main_content)补充完成：准备 ${updateResult.attempted} 条，落库 ${updateResult.updated} 条`
        )
      );
      crawlerService.clearCancel(metadataId);
    } catch (error) {
      crawlerService.clearCancel(metadataId);

      if (error instanceof CrawlerCancelledError) {
        broadcastProgress(createProgressMessage('error', metadataId, 0, '机构调研详情补充已被用户取消'));
        return;
      }

      broadcastProgress(createProgressMessage('error', metadataId, 0, `机构调研详情补充失败: ${(error as Error).message}`));
      throw error;
    }
  }

  /**
   * 安全执行日期范围更新（防止重复更新）
   * @param metadataId 元数据 ID
   * @param beginDate 开始日期，格式：YYYY-MM-DD
   * @param endDate 结束日期，格式：YYYY-MM-DD
   */
  async safePerformDateRangeUpdate(metadataId: number, beginDate: string, endDate: string): Promise<void> {
    if (this.isUpdating(metadataId)) {
      throw new Error('该接口正在更新中，请稍后再试');
    }

    this.prepareCancelStateForStart(metadataId);

    this.updatingIds.add(metadataId);

    try {
      await this.performDateRangeUpdate(metadataId, beginDate, endDate);
    } finally {
      this.updatingIds.delete(metadataId);
      this.scheduleAutoDbMaintenance(`date_range_update:${metadataId}`);
    }
  }

  /**
   * 执行日期范围更新
   * @param metadataId 元数据 ID
   * @param beginDate 开始日期，格式：YYYY-MM-DD
   * @param endDate 结束日期，格式：YYYY-MM-DD
   */
  async performDateRangeUpdate(metadataId: number, beginDate: string, endDate: string): Promise<void> {
    const startTime = Date.now();
    console.log(`[UpdateService] 开始日期范围更新，metadataId: ${metadataId}, 日期范围: ${beginDate} ~ ${endDate}`);

    // 获取元数据
    const metadata = await metadataService.getMetadataById(metadataId);
    const tableName = metadata.table_name;

    // 从结构化字段获取更新配置
    const { mode, uniqueKey, dedupeKey } = getUpdateConfig(metadata);

    console.log(`[UpdateService] 更新模式: ${mode}, 唯一键: ${uniqueKey || '无'}, 去重键: ${dedupeKey || '无'}, 日期范围: ${beginDate} ~ ${endDate}`);

    // 如果是增量更新模式且有唯一键，确保表有唯一索引
    if (mode === 'REPLACE' && uniqueKey) {
      tableService.ensureUniqueIndex(tableName, uniqueKey);
    }

    try {
      // 步骤 1：采集数据（使用日期范围）
      broadcastProgress(createProgressMessage('progress', metadataId, 10, `正在采集数据 (${beginDate} ~ ${endDate})...`));

      // 调用 crawler 的日期范围采集方法
      const newData = await crawlerService.fetchDataByDateRange(metadata, beginDate, endDate);

      console.log(`[UpdateService] 采集完成，数据条数: ${newData.length}`);

      if (newData.length === 0) {
        // 记录更新日志（无新数据）
        const duration = Date.now() - startTime;
        updateLogService.createLog(metadataId, 0, [], 'success', null, duration);
        broadcastProgress(createProgressMessage('complete', metadataId, 100, '采集完成，无新数据'));
        await metadataService.updateLastUpdateTime(metadataId);
        return;
      }

      // 步骤 2：数据去重（使用去重键）
      let deduplicatedData = newData;
      if (dedupeKey) {
        const dedupeKeys = dedupeKey.split(',').map(k => k.trim());
        const seen = new Set<string>();
        deduplicatedData = newData.filter(row => {
          const key = dedupeKeys.map(k => row[k]).join('|');
          if (seen.has(key)) {
            return false;
          }
          seen.add(key);
          return true;
        });

        const duplicateCount = newData.length - deduplicatedData.length;
        if (duplicateCount > 0) {
          console.log(`[UpdateService] 去重: 原始 ${newData.length} 条，去重后 ${deduplicatedData.length} 条，移除 ${duplicateCount} 条重复数据`);
        }
      }

      // 步骤 3：插入新数据
      broadcastProgress(createProgressMessage('progress', metadataId, 70, `正在插入 ${deduplicatedData.length} 条数据...`));
      tableService.insertData(tableName, deduplicatedData, metadataId, uniqueKey);

      // 步骤 4：统计每日数据量
      const dateField = metadata.date_field;
      const dailyStats = this.calculateDailyStats(deduplicatedData, [], dateField || undefined);

      // 步骤 5：记录更新日志
      broadcastProgress(createProgressMessage('progress', metadataId, 95, '正在记录更新日志...'));
      const duration = Date.now() - startTime;
      updateLogService.createLog(metadataId, deduplicatedData.length, dailyStats, 'success', null, duration);

      // 步骤 6：更新最后更新时间
      await metadataService.updateLastUpdateTime(metadataId);

      // 步骤 7：广播完成消息
      broadcastProgress(createProgressMessage('complete', metadataId, 100, `更新完成，共 ${deduplicatedData.length} 条数据 (${beginDate} ~ ${endDate})`));

      console.log(`[UpdateService] 日期范围更新完成，metadataId: ${metadataId}, 数据条数: ${deduplicatedData.length}`);

      // 成功完成后清除取消标记
      crawlerService.clearCancel(metadataId);

    } catch (error) {
      // 清除取消标记
      crawlerService.clearCancel(metadataId);

      // 检查是否是用户取消
      if (error instanceof CrawlerCancelledError) {
        console.log(`[UpdateService] 日期范围采集已被用户取消 [metadataId: ${metadataId}]`);
        const duration = Date.now() - startTime;
        updateLogService.createLog(metadataId, 0, [], 'cancelled', '用户取消', duration);
        broadcastProgress({
          type: 'error',
          metadataId,
          progress: 0,
          message: '采集已被用户取消'
        });
        return;
      }

      console.error(`[UpdateService] 日期范围更新失败:`, error);

      // 记录错误日志
      const duration = Date.now() - startTime;
      updateLogService.createLog(metadataId, 0, [], 'error', (error as Error).message, duration);

      // 广播错误消息
      broadcastProgress({
        type: 'error',
        metadataId,
        progress: 0,
        message: `更新失败: ${(error as Error).message}`
      });

      throw error;
    }
  }

  /**
   * 安全执行按月份批量更新（防止重复更新）
   * @param metadataId 元数据 ID
   * @param beginDate 开始日期，格式：YYYY-MM-DD
   * @param endDate 结束日期，格式：YYYY-MM-DD
   */
  async safePerformBatchMonthlyUpdate(metadataId: number, beginDate: string, endDate: string): Promise<void> {
    if (this.isUpdating(metadataId)) {
      throw new Error('该接口正在更新中，请稍后再试');
    }

    this.prepareCancelStateForStart(metadataId);

    this.updatingIds.add(metadataId);

    try {
      await this.performBatchMonthlyUpdate(metadataId, beginDate, endDate);
    } finally {
      this.updatingIds.delete(metadataId);
      this.scheduleAutoDbMaintenance(`batch_monthly_update:${metadataId}`);
    }
  }

  /**
   * 执行按月份批量更新
   * @param metadataId 元数据 ID
   * @param beginDate 开始日期，格式：YYYY-MM-DD
   * @param endDate 结束日期，格式：YYYY-MM-DD
   */
  async performBatchMonthlyUpdate(metadataId: number, beginDate: string, endDate: string): Promise<void> {
    const startTime = Date.now();
    console.log(`[UpdateService] 开始按月份批量更新，metadataId: ${metadataId}, 日期范围: ${beginDate} ~ ${endDate}`);

    // 获取元数据
    const metadata = await metadataService.getMetadataById(metadataId);
    const tableName = metadata.table_name;

    // 从结构化字段获取更新配置
    const { mode, uniqueKey, dedupeKey } = getUpdateConfig(metadata);

    console.log(`[UpdateService] 更新模式: ${mode}, 唯一键: ${uniqueKey || '无'}, 去重键: ${dedupeKey || '无'}`);

    // 如果是增量更新模式且有唯一键，确保表有唯一索引
    if (mode === 'REPLACE' && uniqueKey) {
      tableService.ensureUniqueIndex(tableName, uniqueKey);
    }

    try {
      // 生成月份列表
      const monthRanges = this.generateMonthRanges(beginDate, endDate);
      console.log(`[UpdateService] 共需采集 ${monthRanges.length} 个月份`);

      broadcastProgress(createProgressMessage('progress', metadataId, 0, `准备按月份批量采集，共 ${monthRanges.length} 个月...`));

      let totalCollected = 0;
      const allDailyStats: Array<{ date: string; count: number }> = [];

      // 逐月采集
      for (let i = 0; i < monthRanges.length; i++) {
        const { start, end } = monthRanges[i];
        const monthProgress = Math.round((i / monthRanges.length) * 90);

        console.log(`[UpdateService] 正在采集第 ${i + 1}/${monthRanges.length} 个月: ${start} ~ ${end}`);
        broadcastProgress(createProgressMessage('progress', metadataId, monthProgress, `正在采集 ${start} ~ ${end} (${i + 1}/${monthRanges.length})...`));

        try {
          // 调用 crawler 的日期范围采集方法
          const monthData = await crawlerService.fetchDataByDateRange(metadata, start, end);

          console.log(`[UpdateService] ${start} ~ ${end} 采集完成，数据条数: ${monthData.length}`);

          if (monthData.length > 0) {
            // 数据去重（使用去重键）
            let deduplicatedData = monthData;
            if (dedupeKey) {
              const dedupeKeys = dedupeKey.split(',').map(k => k.trim());
              const seen = new Set<string>();
              deduplicatedData = monthData.filter(row => {
                const key = dedupeKeys.map(k => row[k]).join('|');
                if (seen.has(key)) {
                  return false;
                }
                seen.add(key);
                return true;
              });

              const duplicateCount = monthData.length - deduplicatedData.length;
              if (duplicateCount > 0) {
                console.log(`[UpdateService] ${start} ~ ${end} 去重: 原始 ${monthData.length} 条，去重后 ${deduplicatedData.length} 条`);
              }
            }

            // 插入数据
            tableService.insertData(tableName, deduplicatedData, metadataId, uniqueKey);
            totalCollected += deduplicatedData.length;

            // 统计每日数据量
            const dateField = metadata.date_field;
            const monthStats = this.calculateDailyStats(deduplicatedData, [], dateField || undefined);
            allDailyStats.push(...monthStats);
          }

          // 短暂延迟，避免请求过快
          await new Promise(resolve => setTimeout(resolve, 1000));

        } catch (error) {
          console.error(`[UpdateService] ${start} ~ ${end} 采集失败:`, error);
          // 继续采集下一个月份，不中断整个流程
        }
      }

      // 记录更新日志
      broadcastProgress(createProgressMessage('progress', metadataId, 95, '正在记录更新日志...'));
      const duration = Date.now() - startTime;
      updateLogService.createLog(metadataId, totalCollected, allDailyStats, 'success', null, duration);

      // 更新最后更新时间
      await metadataService.updateLastUpdateTime(metadataId);

      // 广播完成消息
      broadcastProgress(createProgressMessage('complete', metadataId, 100, `批量采集完成，共 ${totalCollected} 条数据 (${beginDate} ~ ${endDate})`));

      console.log(`[UpdateService] 按月份批量更新完成，metadataId: ${metadataId}, 总数据条数: ${totalCollected}`);

      // 成功完成后清除取消标记
      crawlerService.clearCancel(metadataId);

    } catch (error) {
      // 清除取消标记
      crawlerService.clearCancel(metadataId);

      // 检查是否是用户取消
      if (error instanceof CrawlerCancelledError) {
        console.log(`[UpdateService] 按月份批量采集已被用户取消 [metadataId: ${metadataId}]`);
        const duration = Date.now() - startTime;
        updateLogService.createLog(metadataId, 0, [], 'cancelled', '用户取消', duration);
        broadcastProgress({
          type: 'error',
          metadataId,
          progress: 0,
          message: '采集已被用户取消'
        });
        return;
      }

      console.error(`[UpdateService] 按月份批量更新失败:`, error);

      // 记录错误日志
      const duration = Date.now() - startTime;
      updateLogService.createLog(metadataId, 0, [], 'error', (error as Error).message, duration);

      // 广播错误消息
      broadcastProgress({
        type: 'error',
        metadataId,
        progress: 0,
        message: `批量更新失败: ${(error as Error).message}`
      });

      throw error;
    }
  }

  /**
   * 生成月份范围列表
   * @param beginDate 开始日期，格式：YYYY-MM-DD
   * @param endDate 结束日期，格式：YYYY-MM-DD
   * @returns 月份范围数组
   */
  private generateMonthRanges(beginDate: string, endDate: string): Array<{ start: string; end: string }> {
    const ranges: Array<{ start: string; end: string }> = [];
    const start = new Date(beginDate);
    const end = new Date(endDate);

    let current = new Date(start.getFullYear(), start.getMonth(), 1);

    while (current <= end) {
      const monthStart = new Date(current.getFullYear(), current.getMonth(), 1);
      const monthEnd = new Date(current.getFullYear(), current.getMonth() + 1, 0);

      // 第一个月：从指定的开始日期开始
      const rangeStart = current.getTime() === new Date(start.getFullYear(), start.getMonth(), 1).getTime()
        ? beginDate
        : this.formatDate(monthStart);

      // 最后一个月：到指定的结束日期结束
      const rangeEnd = monthEnd > end
        ? endDate
        : this.formatDate(monthEnd);

      ranges.push({
        start: rangeStart,
        end: rangeEnd
      });

      // 移动到下一个月
      current = new Date(current.getFullYear(), current.getMonth() + 1, 1);
    }

    return ranges;
  }

  /**
   * 格式化日期为 YYYY-MM-DD
   */
  private formatDate(date: Date): string {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  /**
   * 批量更新状态
   */
  private batchUpdating: boolean = false;
  private batchUpdateQueue: number[] = [];
  private batchUpdateCurrent: number = 0;
  private batchUpdateTotal: number = 0;

  /**
   * 检查是否正在批量更新
   */
  isBatchUpdating(): boolean {
    return this.batchUpdating;
  }

  /**
   * 获取批量更新进度
   */
  getBatchUpdateProgress(): { current: number; total: number; queue: number[] } {
    return {
      current: this.batchUpdateCurrent,
      total: this.batchUpdateTotal,
      queue: this.batchUpdateQueue
    };
  }

  /**
   * 执行批量更新（按顺序逐个更新）
   * @param metadataIds 要更新的接口ID列表
   */
  async performBatchUpdate(metadataIds: number[]): Promise<void> {
    if (this.batchUpdating) {
      throw new Error('已有批量更新任务在进行中');
    }

    this.batchUpdating = true;
    this.batchUpdateQueue = [...metadataIds];
    this.batchUpdateTotal = metadataIds.length;
    this.batchUpdateCurrent = 0;

    console.log(`[UpdateService] 开始批量更新，共 ${this.batchUpdateTotal} 个接口`);

    // 记录批量更新开始时间
    const batchStartTime = Date.now();

    // 广播批量更新开始
    broadcastProgress({
      type: 'batch_start',
      metadataId: 0,
      progress: 0,
      message: `开始批量更新，共 ${this.batchUpdateTotal} 个接口`,
      total: this.batchUpdateTotal,
      current: 0
    });

    let successCount = 0;
    let errorCount = 0;

    try {
      for (let i = 0; i < metadataIds.length; i++) {
        // 检查是否被取消（检查全局取消标记）
        if (crawlerService.isCancelled(-1)) {
          console.log(`[UpdateService] 批量更新已被用户取消，已完成 ${i}/${this.batchUpdateTotal} 个接口`);
          const totalDuration = Date.now() - batchStartTime;

          // 清除取消标记
          crawlerService.clearAllCancels();

          // 广播批量更新被取消
          broadcastProgress({
            type: 'batch_complete',
            metadataId: 0,
            progress: 100,
            message: `批量更新已取消，已完成 ${successCount} 个接口，失败 ${errorCount} 个`,
            total: this.batchUpdateTotal,
            current: i,
            duration: totalDuration
          } as any);

          return;
        }

        const metadataId = metadataIds[i];
        this.batchUpdateCurrent = i + 1;

        // 记录单个接口更新开始时间
        const itemStartTime = Date.now();

        // 广播当前更新的接口
        broadcastProgress({
          type: 'batch_progress',
          metadataId: metadataId,
          progress: Math.round((i / this.batchUpdateTotal) * 100),
          message: `正在更新第 ${i + 1}/${this.batchUpdateTotal} 个接口`,
          total: this.batchUpdateTotal,
          current: i + 1
        });

        try {
          // 执行单个接口的更新
          await this.safePerformFullUpdate(metadataId);

          // 计算单个接口耗时
          const itemDuration = Date.now() - itemStartTime;
          const totalElapsed = Date.now() - batchStartTime;

          console.log(`[UpdateService] 批量更新: ${i + 1}/${this.batchUpdateTotal} 完成，耗时 ${this.formatDuration(itemDuration)}`);
          successCount++;

          // 广播单个接口完成（包含耗时）
          broadcastProgress({
            type: 'batch_item_complete',
            metadataId: metadataId,
            progress: Math.round(((i + 1) / this.batchUpdateTotal) * 100),
            message: `第 ${i + 1}/${this.batchUpdateTotal} 个接口更新完成`,
            total: this.batchUpdateTotal,
            current: i + 1,
            duration: itemDuration,
            totalElapsed: totalElapsed
          } as any);
        } catch (error) {
          // 计算失败接口耗时
          const itemDuration = Date.now() - itemStartTime;
          const totalElapsed = Date.now() - batchStartTime;

          console.error(`[UpdateService] 批量更新: 接口 ${metadataId} 更新失败:`, error);
          errorCount++;

          // 广播单个接口失败（包含耗时）
          broadcastProgress({
            type: 'batch_item_error',
            metadataId: metadataId,
            progress: Math.round(((i + 1) / this.batchUpdateTotal) * 100),
            message: `第 ${i + 1}/${this.batchUpdateTotal} 个接口更新失败: ${(error as Error).message}`,
            total: this.batchUpdateTotal,
            current: i + 1,
            duration: itemDuration,
            totalElapsed: totalElapsed
          } as any);
          // 继续更新下一个接口，不中断批量更新
        }
      }

      // 计算总耗时
      const totalDuration = Date.now() - batchStartTime;

      // 保存批量更新日志到数据库
      updateLogService.createBatchLog(this.batchUpdateTotal, successCount, errorCount, totalDuration);

      // 广播批量更新完成
      broadcastProgress({
        type: 'batch_complete',
        metadataId: 0,
        progress: 100,
        message: `批量更新完成，共 ${this.batchUpdateTotal} 个接口，总耗时 ${this.formatDuration(totalDuration)}`,
        total: this.batchUpdateTotal,
        current: this.batchUpdateTotal,
        duration: totalDuration
      } as any);

      console.log(`[UpdateService] 批量更新完成，共 ${this.batchUpdateTotal} 个接口，总耗时 ${this.formatDuration(totalDuration)}`);

    } finally {
      // 清除所有取消标记
      crawlerService.clearAllCancels();
      this.batchUpdating = false;
      this.batchUpdateQueue = [];
      this.batchUpdateCurrent = 0;
      this.batchUpdateTotal = 0;
      this.scheduleAutoDbMaintenance('batch_update_complete');
    }
  }

  /**
   * 格式化时长显示
   * @param ms 毫秒数
   * @returns 格式化的时长字符串
   */
  private formatDuration(ms: number): string {
    if (ms < 1000) {
      return `${ms}ms`;
    }
    const seconds = Math.floor(ms / 1000);
    if (seconds < 60) {
      return `${seconds}秒`;
    }
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    if (minutes < 60) {
      return remainingSeconds > 0 ? `${minutes}分${remainingSeconds}秒` : `${minutes}分`;
    }
    const hours = Math.floor(minutes / 60);
    const remainingMinutes = minutes % 60;
    return remainingMinutes > 0 ? `${hours}小时${remainingMinutes}分` : `${hours}小时`;
  }
}

export const updateService = new UpdateService();
