/**
 * WebSocket 服务模块
 * 负责实时进度推送
 */
import { WebSocketServer, WebSocket } from 'ws';
import { Server } from 'http';
import { ProgressMessage } from '../models/types';

let wss: WebSocketServer;

// 存储所有连接的客户端
const clients: Set<WebSocket> = new Set();

/**
 * 初始化 WebSocket 服务
 */
export function initWebSocket(server: Server): void {
  wss = new WebSocketServer({ server });
  
  wss.on('connection', (ws: WebSocket) => {
    console.log('WebSocket 客户端已连接');
    clients.add(ws);
    
    ws.on('close', () => {
      console.log('WebSocket 客户端已断开');
      clients.delete(ws);
    });
    
    ws.on('error', (error) => {
      console.error('WebSocket 错误:', error);
      clients.delete(ws);
    });
  });
  
  console.log('WebSocket 服务已初始化');
}

/**
 * 向所有客户端广播进度消息
 */
export function broadcastProgress(message: ProgressMessage): void {
  const messageStr = JSON.stringify(message);
  
  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(messageStr);
    }
  });
}

/**
 * 创建进度消息
 */
export function createProgressMessage(
  type: ProgressMessage['type'],
  metadataId: number,
  progress: number,
  message: string
): ProgressMessage {
  return {
    type,
    metadataId,
    progress: Math.min(100, Math.max(0, progress)), // 确保在 0-100 范围内
    message,
    timestamp: Date.now()
  };
}

/**
 * 获取 WebSocket 服务实例
 */
export function getWebSocketServer(): WebSocketServer | undefined {
  return wss;
}

/**
 * 获取当前连接的客户端数量
 */
export function getClientCount(): number {
  return clients.size;
}
