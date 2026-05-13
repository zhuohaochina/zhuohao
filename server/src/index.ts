/**
 * 后端入口
 */
import express from 'express';
import cors from 'cors';
import { createServer } from 'http';
import { initDatabase } from './services/database';
import { dbMaintenanceService } from './services/dbMaintenanceService';
import { initWebSocket } from './websocket';
import metadataRoutes from './routes/metadata';
import dataRoutes from './routes/data';
import dataComboRoutes from './routes/dataCombo';
import updateRoutes from './routes/update';
import updateLogRoutes from './routes/updateLog';
import fieldMappingRoutes from './routes/fieldMapping';

const app = express();
const PORT = process.env.PORT || 3000;

// 中间件
app.use(cors());
app.use(express.json());

// 路由
app.use('/api/metadata', metadataRoutes);
app.use('/api/data', dataRoutes);
app.use('/api/data-combo', dataComboRoutes);
app.use('/api/update', updateRoutes);
app.use('/api/update-logs', updateLogRoutes);
app.use('/api/field-mapping', fieldMappingRoutes);

// 健康检查
app.get('/api/health', (_req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

const server = createServer(app);
initWebSocket(server);

async function start() {
  await initDatabase();

  // Trigger a non-blocking maintenance check after startup.
  setImmediate(() => {
    dbMaintenanceService.maybeAutoCompact('startup').catch((error) => {
      console.warn('[DBMaintenance] Startup auto maintenance trigger failed:', (error as Error).message);
    });
  });

  server.listen(PORT, () => {
    console.log(`服务器已启动，监听端口 ${PORT}`);
    console.log(`API 地址: http://localhost:${PORT}/api`);
    console.log(`WebSocket 地址: ws://localhost:${PORT}`);
  });
}

start().catch(console.error);

export default app;
