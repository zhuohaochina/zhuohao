/**
 * WebSocket 连接管理工具
 */
import { ref } from 'vue'
import type { ProgressMessage } from '../types'

// WebSocket 实例
let ws: WebSocket | null = null

// 连接状态
export const isConnected = ref(false)

// 当前进度消息
export const currentProgress = ref<ProgressMessage | null>(null)

// 消息回调
type MessageCallback = (message: ProgressMessage) => void
const callbacks: MessageCallback[] = []

/**
 * 连接 WebSocket
 */
export function connect(): void {
  if (ws && ws.readyState === WebSocket.OPEN) {
    return
  }

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const host = window.location.hostname
  const port = '3000' // 后端端口
  const url = `${protocol}//${host}:${port}`

  ws = new WebSocket(url)

  ws.onopen = () => {
    console.log('WebSocket 已连接')
    isConnected.value = true
  }

  ws.onclose = () => {
    console.log('WebSocket 已断开')
    isConnected.value = false
    // 自动重连
    setTimeout(connect, 3000)
  }

  ws.onerror = (error) => {
    console.error('WebSocket 错误:', error)
  }

  ws.onmessage = (event) => {
    try {
      const message: ProgressMessage = JSON.parse(event.data)
      currentProgress.value = message
      
      // 调用所有回调
      callbacks.forEach(cb => cb(message))
      
      // 如果是完成或错误状态，3秒后清除
      if (message.type === 'complete' || message.type === 'error') {
        setTimeout(() => {
          if (currentProgress.value?.metadataId === message.metadataId) {
            currentProgress.value = null
          }
        }, 3000)
      }
    } catch (error) {
      console.error('解析 WebSocket 消息失败:', error)
    }
  }
}

/**
 * 断开 WebSocket
 */
export function disconnect(): void {
  if (ws) {
    ws.close()
    ws = null
  }
}

/**
 * 注册消息回调
 */
export function onMessage(callback: MessageCallback): () => void {
  callbacks.push(callback)
  // 返回取消注册函数
  return () => {
    const index = callbacks.indexOf(callback)
    if (index > -1) {
      callbacks.splice(index, 1)
    }
  }
}

/**
 * 获取当前进度
 */
export function getProgress(): ProgressMessage | null {
  return currentProgress.value
}
