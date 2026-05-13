import { Router, Request, Response } from 'express';
import { dataComboService } from '../services/dataComboService';

const router = Router();

router.get('/options/metadata', (_req: Request, res: Response) => {
  try {
    res.json({
      success: true,
      data: dataComboService.listMetadataOptions()
    });
  } catch (error) {
    console.error('获取关联视图配置项失败:', error);
    res.status(500).json({
      success: false,
      message: '获取关联视图配置项失败',
      error: (error as Error).message
    });
  }
});

router.get('/', (_req: Request, res: Response) => {
  try {
    res.json({
      success: true,
      data: dataComboService.listCombos()
    });
  } catch (error) {
    console.error('获取关联视图列表失败:', error);
    res.status(500).json({
      success: false,
      message: '获取关联视图列表失败',
      error: (error as Error).message
    });
  }
});

router.post('/', (req: Request, res: Response) => {
  try {
    res.status(201).json({
      success: true,
      data: dataComboService.createCombo(req.body),
      message: '关联视图创建成功'
    });
  } catch (error) {
    const message = (error as Error).message;
    console.error('创建关联视图失败:', error);
    res.status(400).json({
      success: false,
      message,
      error: message
    });
  }
});

router.get('/:id/row/:code', (req: Request, res: Response) => {
  try {
    res.json({
      success: true,
      data: dataComboService.getComboRow(req.params.id, decodeURIComponent(req.params.code))
    });
  } catch (error) {
    const message = (error as Error).message;
    console.error('获取关联视图单行详情失败:', error);
    res.status(message === '关联视图不存在' || message === '未找到对应主表记录' ? 404 : 400).json({
      success: false,
      message,
      error: message
    });
  }
});

router.get('/:id/data', (req: Request, res: Response) => {
  try {
    const page = parseInt(req.query.page as string, 10) || 1;
    const pageSize = parseInt(req.query.pageSize as string, 10) || 20;
    const keyword = (req.query.keyword as string) || '';
    const sortField = (req.query.sortField as string) || '';
    const sortOrder = (req.query.sortOrder as string) || '';

    res.json({
      success: true,
      data: dataComboService.getComboData(req.params.id, page, pageSize, keyword, sortField, sortOrder)
    });
  } catch (error) {
    const message = (error as Error).message;
    console.error('获取关联视图数据失败:', error);
    res.status(message === '关联视图不存在' ? 404 : 500).json({
      success: false,
      message,
      error: message
    });
  }
});

router.get('/:id', (req: Request, res: Response) => {
  try {
    res.json({
      success: true,
      data: dataComboService.getComboById(req.params.id)
    });
  } catch (error) {
    const message = (error as Error).message;
    console.error('获取关联视图详情失败:', error);
    res.status(message === '关联视图不存在' ? 404 : 500).json({
      success: false,
      message,
      error: message
    });
  }
});

router.delete('/:id', (req: Request, res: Response) => {
  try {
    dataComboService.deleteCombo(req.params.id);
    res.json({
      success: true,
      message: '关联视图已删除'
    });
  } catch (error) {
    const message = (error as Error).message;
    console.error('删除关联视图失败:', error);
    res.status(message === '关联视图不存在' ? 404 : 400).json({
      success: false,
      message,
      error: message
    });
  }
});

export default router;
