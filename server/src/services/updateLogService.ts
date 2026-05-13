/**
 * 更新日志服务模块
 * 负责记录和查询更新历史
 */
import { query, run, getLastInsertRowId } from './database';
import type { UpdateLog, DailyStatItem } from '../models/types';

/**
 * 更新日志服务类
 */
export class UpdateLogService {
  /**
   * 创建更新日志
   */
  createLog(
    metadataId: number,
    totalCount: number,
    dailyStats: DailyStatItem[],
    status: 'success' | 'error' | 'cancelled' = 'success',
    errorMessage: string | null = null,
    duration: number | null = null
  ): number {
    const now = new Date().toISOString();
    const dailyStatsJson = JSON.stringify(dailyStats);

    run(
      `INSERT INTO update_logs (metadata_id, update_time, total_count, daily_stats, status, error_message, created_at, duration)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [metadataId, now, totalCount, dailyStatsJson, status, errorMessage, now, duration]
    );

    return getLastInsertRowId();
  }

  /**
   * 获取指定接口的更新日志列表
   */
  getLogsByMetadataId(metadataId: number, page: number = 1, pageSize: number = 20): {
    data: UpdateLog[];
    total: number;
    page: number;
    pageSize: number;
    totalPages: number;
  } {
    // 获取总数
    const countResult = query<{ count: number }>(
      'SELECT COUNT(*) as count FROM update_logs WHERE metadata_id = ?',
      [metadataId]
    );
    const total = countResult[0]?.count || 0;
    
    // 获取分页数据
    const offset = (page - 1) * pageSize;
    const data = query<UpdateLog>(
      `SELECT * FROM update_logs WHERE metadata_id = ? ORDER BY update_time DESC LIMIT ? OFFSET ?`,
      [metadataId, pageSize, offset]
    );
    
    return {
      data,
      total,
      page,
      pageSize,
      totalPages: Math.ceil(total / pageSize)
    };
  }

  /**
   * 获取单条更新日志详情
   */
  getLogById(logId: number): UpdateLog | null {
    const result = query<UpdateLog>(
      'SELECT * FROM update_logs WHERE id = ?',
      [logId]
    );
    return result[0] || null;
  }

  /**
   * 获取指定接口最新一次成功更新的耗时
   */
  getLatestDuration(metadataId: number): number | null {
    const result = query<{ duration: number | null }>(
      `SELECT duration FROM update_logs
       WHERE metadata_id = ? AND status = 'success' AND duration IS NOT NULL
       ORDER BY update_time DESC LIMIT 1`,
      [metadataId]
    );
    return result[0]?.duration ?? null;
  }

  /**
   * 删除指定接口的所有更新日志
   */
  deleteLogsByMetadataId(metadataId: number): void {
    run('DELETE FROM update_logs WHERE metadata_id = ?', [metadataId]);
  }

  /**
   * 创建批量更新日志
   */
  createBatchLog(
    totalCount: number,
    successCount: number,
    errorCount: number,
    duration: number
  ): number {
    const now = new Date().toISOString();

    run(
      `INSERT INTO batch_update_logs (update_time, total_count, success_count, error_count, duration, created_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [now, totalCount, successCount, errorCount, duration, now]
    );

    return getLastInsertRowId();
  }

  /**
   * 获取最新一次批量更新的耗时
   */
  getLatestBatchDuration(): { duration: number; update_time: string } | null {
    const result = query<{ duration: number; update_time: string }>(
      `SELECT duration, update_time FROM batch_update_logs ORDER BY id DESC LIMIT 1`
    );
    return result[0] || null;
  }
}

export const updateLogService = new UpdateLogService();
