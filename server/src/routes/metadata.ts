/**
 * 元数据 API 路由
 * 处理接口元数据的 CRUD 操作
 */
import { Router, Request, Response } from 'express';
import { metadataService } from '../services/metadataService';
import { ValidationError } from '../models/validators';
import { fieldSyncService } from '../services/fieldSyncService';
import { updateLogService } from '../services/updateLogService';

const router = Router();

/**
 * GET /api/metadata - 获取所有元数据列表
 */
router.get('/', async (req: Request, res: Response) => {
  try {
    const includeDisabled = req.query.includeDisabled === '1' || req.query.includeDisabled === 'true';
    const list = await metadataService.getMetadataList(includeDisabled);
    const lastBatchDuration = updateLogService.getLatestBatchDuration();
    res.json({
      success: true,
      data: list,
      lastBatchDuration: lastBatchDuration
    });
  } catch (error) {
    console.error('获取元数据列表失败:', error);
    res.status(500).json({
      success: false,
      message: '获取元数据列表失败',
      error: (error as Error).message
    });
  }
});

/**
 * POST /api/metadata/sync-fields - 检查并同步所有接口的字段结构
 */
router.post('/sync-fields', async (req: Request, res: Response) => {
  try {
    const result = await fieldSyncService.syncAllFields();
    res.json({
      success: true,
      data: result,
      message: `检查完成：${result.summary.total} 个接口，${result.summary.synced} 个需要同步，${result.summary.failed} 个失败`
    });
  } catch (error) {
    console.error('同步字段失败:', error);
    res.status(500).json({
      success: false,
      message: '同步字段失败',
      error: (error as Error).message
    });
  }
});

/**
 * GET /api/metadata/check-fields - 检查所有接口的字段差异（不执行同步）
 */
router.get('/check-fields', async (req: Request, res: Response) => {
  try {
    const result = await fieldSyncService.checkAllFields();
    res.json({
      success: true,
      data: result
    });
  } catch (error) {
    console.error('检查字段失败:', error);
    res.status(500).json({
      success: false,
      message: '检查字段失败',
      error: (error as Error).message
    });
  }
});

/**
 * GET /api/metadata/:id/check-fields - 检查单个接口的字段差异
 */
router.get('/:id/check-fields', async (req: Request, res: Response) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (isNaN(id)) {
      res.status(400).json({
        success: false,
        message: '无效的 ID'
      });
      return;
    }

    const result = await fieldSyncService.checkSingleField(id);
    res.json({
      success: true,
      data: result
    });
  } catch (error) {
    console.error('检查单个接口字段失败:', error);
    res.status(500).json({
      success: false,
      message: '检查字段失败',
      error: (error as Error).message
    });
  }
});

/**
 * POST /api/metadata/:id/sync-fields - 同步单个接口的字段结构
 */
router.post('/:id/sync-fields', async (req: Request, res: Response) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (isNaN(id)) {
      res.status(400).json({
        success: false,
        message: '无效的 ID'
      });
      return;
    }

    const result = await fieldSyncService.syncSingleField(id);
    res.json({
      success: true,
      data: result,
      message: result.synced ? '同步完成' : '无需同步'
    });
  } catch (error) {
    console.error('同步单个接口字段失败:', error);
    res.status(500).json({
      success: false,
      message: '同步字段失败',
      error: (error as Error).message
    });
  }
});

/**
 * POST /api/metadata/:id/test-max-page-size - 检测单个接口的实际最大返回数量
 */
router.post('/:id/test-max-page-size', async (req: Request, res: Response) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (isNaN(id)) {
      res.status(400).json({
        success: false,
        message: '无效的 ID'
      });
      return;
    }

    const result = await fieldSyncService.testMaxPageSizeForMetadata(id);
    res.json({
      success: true,
      data: result
    });
  } catch (error) {
    console.error('检测实际最大返回数量失败:', error);
    res.status(500).json({
      success: false,
      message: '检测实际最大返回数量失败',
      error: (error as Error).message
    });
  }
});

/**
 * POST /api/metadata - 创建新的元数据
 */
router.post('/', async (req: Request, res: Response) => {
  try {
    const metadata = await metadataService.createMetadata(req.body);
    res.status(201).json({
      success: true,
      data: metadata,
      message: '创建成功'
    });
  } catch (error) {
    console.error('创建元数据失败:', error);
    
    if (error instanceof ValidationError) {
      res.status(400).json({
        success: false,
        message: error.message
      });
    } else {
      res.status(500).json({
        success: false,
        message: '创建元数据失败',
        error: (error as Error).message
      });
    }
  }
});

/**
 * PUT /api/metadata/sort - 更新排序（必须放在 :id 路由之前）
 */
router.put('/sort', async (req: Request, res: Response) => {
  try {
    const { orders } = req.body;
    if (!Array.isArray(orders)) {
      res.status(400).json({
        success: false,
        message: '无效的排序数据'
      });
      return;
    }
    
    await metadataService.updateSortOrder(orders);
    res.json({
      success: true,
      message: '排序更新成功'
    });
  } catch (error) {
    console.error('更新排序失败:', error);
    res.status(500).json({
      success: false,
      message: '更新排序失败',
      error: (error as Error).message
    });
  }
});

/**
 * GET /api/metadata/:id - 获取单个元数据详情
 */
router.get('/:id', async (req: Request, res: Response) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (isNaN(id)) {
      res.status(400).json({
        success: false,
        message: '无效的 ID'
      });
      return;
    }
    
    const metadata = await metadataService.getMetadataById(id);
    res.json({
      success: true,
      data: metadata
    });
  } catch (error) {
    console.error('获取元数据详情失败:', error);
    
    if ((error as Error).message.includes('不存在')) {
      res.status(404).json({
        success: false,
        message: (error as Error).message
      });
    } else {
      res.status(500).json({
        success: false,
        message: '获取元数据详情失败',
        error: (error as Error).message
      });
    }
  }
});

/**
 * PUT /api/metadata/:id - 更新元数据
 */
router.put('/:id', async (req: Request, res: Response) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (isNaN(id)) {
      res.status(400).json({
        success: false,
        message: '无效的 ID'
      });
      return;
    }
    
    const metadata = await metadataService.updateMetadata(id, req.body);
    res.json({
      success: true,
      data: metadata,
      message: '更新成功'
    });
  } catch (error) {
    console.error('更新元数据失败:', error);
    
    if (error instanceof ValidationError) {
      res.status(400).json({
        success: false,
        message: error.message
      });
    } else if ((error as Error).message.includes('不存在')) {
      res.status(404).json({
        success: false,
        message: (error as Error).message
      });
    } else {
      res.status(500).json({
        success: false,
        message: '更新元数据失败',
        error: (error as Error).message
      });
    }
  }
});

/**
 * DELETE /api/metadata/:id - 删除元数据
 */
router.delete('/:id', async (req: Request, res: Response) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (isNaN(id)) {
      res.status(400).json({
        success: false,
        message: '无效的 ID'
      });
      return;
    }
    
    await metadataService.deleteMetadata(id);
    res.json({
      success: true,
      message: '删除成功'
    });
  } catch (error) {
    console.error('删除元数据失败:', error);
    
    if ((error as Error).message.includes('不存在')) {
      res.status(404).json({
        success: false,
        message: (error as Error).message
      });
    } else {
      res.status(500).json({
        success: false,
        message: '删除元数据失败',
        error: (error as Error).message
      });
    }
  }
});

export default router;
