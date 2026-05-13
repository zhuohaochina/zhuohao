/**
 * 更新日志路由
 */
import { Router, Request, Response } from 'express';
import { updateLogService } from '../services/updateLogService';
import { metadataService } from '../services/metadataService';

const router = Router();

/**
 * 获取指定接口的更新日志列表
 * GET /api/update-logs/:metadataId
 */
router.get('/:metadataId', async (req: Request, res: Response) => {
  try {
    const metadataId = parseInt(req.params.metadataId);
    const page = parseInt(req.query.page as string) || 1;
    const pageSize = parseInt(req.query.pageSize as string) || 20;
    
    if (isNaN(metadataId)) {
      return res.status(400).json({ success: false, message: '无效的接口 ID' });
    }
    
    // 获取接口信息
    const metadata = await metadataService.getMetadataById(metadataId);
    
    // 获取更新日志
    const result = updateLogService.getLogsByMetadataId(metadataId, page, pageSize);
    
    res.json({
      success: true,
      data: {
        metadata: {
          id: metadata.id,
          cn_name: metadata.cn_name,
          update_mode: metadata.update_mode,
          date_field: metadata.date_field,
          date_range: metadata.date_range,
          future_days: metadata.future_days
        },
        logs: result.data,
        total: result.total,
        page: result.page,
        pageSize: result.pageSize,
        totalPages: result.totalPages
      }
    });
  } catch (error) {
    console.error('获取更新日志失败:', error);
    res.status(500).json({ success: false, message: (error as Error).message });
  }
});

/**
 * 获取单条更新日志详情
 * GET /api/update-logs/detail/:logId
 */
router.get('/detail/:logId', async (req: Request, res: Response) => {
  try {
    const logId = parseInt(req.params.logId);
    
    if (isNaN(logId)) {
      return res.status(400).json({ success: false, message: '无效的日志 ID' });
    }
    
    const log = updateLogService.getLogById(logId);
    
    if (!log) {
      return res.status(404).json({ success: false, message: '日志不存在' });
    }
    
    res.json({ success: true, data: log });
  } catch (error) {
    console.error('获取日志详情失败:', error);
    res.status(500).json({ success: false, message: (error as Error).message });
  }
});

export default router;
