/**
 * 数据更新 API 路由
 * 处理一键更新操作
 */
import { Router, Request, Response } from 'express';
import { updateService } from '../services/updateService';
import { metadataService } from '../services/metadataService';
import { crawlerService } from '../services/crawler';

const router = Router();

/**
 * POST /api/update/stop/:metadataId - 停止指定接口的采集
 */
router.post('/stop/:metadataId', async (req: Request, res: Response) => {
  try {
    const metadataId = parseInt(req.params.metadataId, 10);

    if (isNaN(metadataId)) {
      res.status(400).json({
        success: false,
        message: '无效的元数据 ID'
      });
      return;
    }

    // 仅当该接口确实在更新中时才设置取消标记，避免残留取消状态
    if (!updateService.isUpdating(metadataId)) {
      res.json({
        success: true,
        message: '当前接口没有进行中的采集任务'
      });
      return;
    }

    // 请求取消采集
    crawlerService.requestCancel(metadataId);

    res.json({
      success: true,
      message: '已发送停止请求'
    });

  } catch (error) {
    console.error('停止采集失败:', error);
    res.status(500).json({
      success: false,
      message: '停止采集失败',
      error: (error as Error).message
    });
  }
});

/**
 * POST /api/update/stop-all - 停止所有采集（包括批量更新）
 */
router.post('/stop-all', async (req: Request, res: Response) => {
  try {
    // 仅在确有进行中任务时才设置全局取消标记
    if (!updateService.hasActiveUpdates()) {
      res.json({
        success: true,
        message: '当前没有进行中的采集任务'
      });
      return;
    }

    // 请求取消所有采集
    crawlerService.requestCancelAll();

    res.json({
      success: true,
      message: '已发送停止所有采集请求'
    });

  } catch (error) {
    console.error('停止所有采集失败:', error);
    res.status(500).json({
      success: false,
      message: '停止所有采集失败',
      error: (error as Error).message
    });
  }
});

/**
 * POST /api/update/all - 触发所有接口的顺序更新
 */
router.post('/all', async (req: Request, res: Response) => {
  try {
    // 检查是否已有批量更新在进行
    if (updateService.isBatchUpdating()) {
      res.status(409).json({
        success: false,
        message: '已有批量更新任务在进行中'
      });
      return;
    }
    
    // 获取所有接口列表（按 sort_order 排序）
    const metadataList = await metadataService.getMetadataList();
    
    if (metadataList.length === 0) {
      res.json({
        success: true,
        message: '没有需要更新的接口'
      });
      return;
    }
    
    // 异步执行批量更新
    updateService.performBatchUpdate(metadataList.map((m: any) => m.id))
      .catch(error => {
        console.error('批量更新失败:', error);
      });
    
    res.json({
      success: true,
      message: `批量更新任务已启动，共 ${metadataList.length} 个接口`,
      total: metadataList.length
    });
    
  } catch (error) {
    console.error('触发批量更新失败:', error);
    res.status(500).json({
      success: false,
      message: '触发批量更新失败',
      error: (error as Error).message
    });
  }
});

/**
 * GET /api/update/:metadataId/preview - 预览更新数据量
 * 返回预计将要更新的数据量信息
 */
router.get('/:metadataId/preview', async (req: Request, res: Response) => {
  try {
    const metadataId = parseInt(req.params.metadataId, 10);

    if (isNaN(metadataId)) {
      res.status(400).json({
        success: false,
        message: '无效的元数据 ID'
      });
      return;
    }

    const preview = await updateService.previewUpdate(metadataId);

    res.json({
      success: true,
      data: preview
    });

  } catch (error) {
    console.error('预览更新失败:', error);
    res.status(500).json({
      success: false,
      message: '预览更新失败',
      error: (error as Error).message
    });
  }
});

/**
 * POST /api/update/:metadataId/jgdy-main-content - 单独补充机构调研统计汇总 main_content
 */
router.post('/:metadataId/jgdy-main-content', async (req: Request, res: Response) => {
  try {
    const metadataId = parseInt(req.params.metadataId, 10);

    if (isNaN(metadataId)) {
      res.status(400).json({
        success: false,
        message: '无效的元数据 ID'
      });
      return;
    }

    if (updateService.isUpdating(metadataId)) {
      res.status(409).json({
        success: false,
        message: '该接口正在更新中，请稍后再试'
      });
      return;
    }

    updateService.safeBackfillJgdyMainContent(metadataId)
      .catch(error => {
        console.error(`机构调研详情补充失败 [ID: ${metadataId}]:`, error);
      });

    res.json({
      success: true,
      message: '机构调研详情(main_content)补充任务已启动，请通过 WebSocket 查看进度'
    });
  } catch (error) {
    console.error('触发机构调研详情补充失败:', error);

    if ((error as Error).message.includes('正在更新中')) {
      res.status(409).json({
        success: false,
        message: (error as Error).message
      });
    } else {
      res.status(500).json({
        success: false,
        message: '触发机构调研详情补充失败',
        error: (error as Error).message
      });
    }
  }
});

/**
 * POST /api/update/:metadataId - 触发指定接口的更新
 * 支持查询参数:
 *   - mode=full: 10年全量更新
 *   - mode=truefull: 真正全量更新（不带日期过滤器，获取所有历史数据）
 *   - mode=daterange: 日期范围更新（需配合 begin_date 和 end_date 参数）
 *   - mode=batchmonthly: 按月份批量采集（需配合 begin_date 和 end_date 参数）
 *   - begin_date: 开始日期，格式：YYYY-MM-DD
 *   - end_date: 结束日期，格式：YYYY-MM-DD
 */
router.post('/:metadataId', async (req: Request, res: Response) => {
  try {
    const metadataId = parseInt(req.params.metadataId, 10);
    const mode = req.query.mode as string; // 'full' 表示10年全量更新, 'truefull' 表示真正全量更新, 'daterange' 表示日期范围更新
    const beginDate = req.query.begin_date as string;
    const endDate = req.query.end_date as string;
    const skipJgdyMainContent = ['1', 'true', 'yes'].includes(
      String(req.query.skip_jgdy_main_content || '').toLowerCase()
    );

    if (isNaN(metadataId)) {
      res.status(400).json({
        success: false,
        message: '无效的元数据 ID'
      });
      return;
    }

    // 异步执行更新，立即返回响应
    // 进度通过 WebSocket 推送
    if (mode === 'daterange') {
      // 日期范围更新：采集指定日期范围的数据
      if (!beginDate || !endDate) {
        res.status(400).json({
          success: false,
          message: '日期范围更新需要提供 begin_date 和 end_date 参数'
        });
        return;
      }

      updateService.safePerformDateRangeUpdate(metadataId, beginDate, endDate)
        .catch((error: Error) => {
          console.error(`日期范围更新失败 [ID: ${metadataId}]:`, error);
        });

      res.json({
        success: true,
        message: `日期范围更新任务已启动（${beginDate} ~ ${endDate}），请通过 WebSocket 查看进度`
      });
    } else if (mode === 'batchmonthly') {
      // 按月份批量采集：将日期范围拆分为多个月份，逐月采集
      if (!beginDate || !endDate) {
        res.status(400).json({
          success: false,
          message: '按月份批量采集需要提供 begin_date 和 end_date 参数'
        });
        return;
      }

      updateService.safePerformBatchMonthlyUpdate(metadataId, beginDate, endDate)
        .catch((error: Error) => {
          console.error(`按月份批量采集失败 [ID: ${metadataId}]:`, error);
        });

      res.json({
        success: true,
        message: `按月份批量采集任务已启动（${beginDate} ~ ${endDate}），请通过 WebSocket 查看进度`
      });
    } else if (mode === 'truefull') {
      // 真正全量更新：采集所有历史数据（不带日期过滤器）
      updateService.safePerformFullUpdate(metadataId, false, true, skipJgdyMainContent)
        .catch(error => {
          console.error(`真正全量更新失败 [ID: ${metadataId}]:`, error);
        });

      res.json({
        success: true,
        message: '真正全量更新任务已启动（将采集所有历史数据），请通过 WebSocket 查看进度'
      });
    } else if (mode === 'full') {
      // 10年全量更新：采集最近10年数据
      updateService.safePerformFullUpdate(metadataId, true, false, skipJgdyMainContent)
        .catch(error => {
          console.error(`10年全量更新失败 [ID: ${metadataId}]:`, error);
        });

      res.json({
        success: true,
        message: '10年全量更新任务已启动，请通过 WebSocket 查看进度'
      });
    } else {
      // 增量更新：按配置的日期范围采集
      updateService.safePerformFullUpdate(metadataId, false, false, skipJgdyMainContent)
        .catch(error => {
          console.error(`更新失败 [ID: ${metadataId}]:`, error);
        });

      res.json({
        success: true,
        message: '更新任务已启动，请通过 WebSocket 查看进度'
      });
    }

  } catch (error) {
    console.error('触发更新失败:', error);

    if ((error as Error).message.includes('正在更新中')) {
      res.status(409).json({
        success: false,
        message: (error as Error).message
      });
    } else if ((error as Error).message.includes('不存在')) {
      res.status(404).json({
        success: false,
        message: (error as Error).message
      });
    } else {
      res.status(500).json({
        success: false,
        message: '触发更新失败',
        error: (error as Error).message
      });
    }
  }
});

export default router;
