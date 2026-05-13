<template>
  <Transition name="slide">
    <div v-if="visible" class="progress-bar-container" :class="statusClass">
      <div class="progress-content">
        <el-progress 
          :percentage="progress" 
          :status="progressStatus"
          :stroke-width="8"
          :show-text="false"
        />
        <div class="progress-info">
          <span class="progress-text">{{ message }}</span>
          <span class="progress-percent">{{ progress }}%</span>
        </div>
      </div>
      <el-button 
        v-if="canClose" 
        class="close-btn" 
        :icon="Close" 
        circle 
        size="small"
        @click="hide"
      />
    </div>
  </Transition>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { Close } from '@element-plus/icons-vue'
import { connect, disconnect, onMessage, currentProgress } from '../utils/websocket'
import type { ProgressMessage } from '../types'

// 状态
const visible = ref(false)
const progress = ref(0)
const message = ref('')
const status = ref<'progress' | 'complete' | 'error'>('progress')

// 计算属性
const progressStatus = computed(() => {
  if (status.value === 'complete') return 'success'
  if (status.value === 'error') return 'exception'
  return ''
})

const statusClass = computed(() => {
  return `status-${status.value}`
})

const canClose = computed(() => {
  return status.value === 'complete' || status.value === 'error'
})

// 方法
function show() {
  visible.value = true
}

function hide() {
  visible.value = false
}

function handleMessage(msg: ProgressMessage) {
  show()
  progress.value = msg.progress
  message.value = msg.message
  status.value = msg.type
  
  // 完成或错误后自动隐藏
  if (msg.type === 'complete' || msg.type === 'error') {
    setTimeout(hide, 5000)
  }
}

// 生命周期
let unsubscribe: (() => void) | null = null

onMounted(() => {
  connect()
  unsubscribe = onMessage(handleMessage)
})

onUnmounted(() => {
  if (unsubscribe) {
    unsubscribe()
  }
})

// 暴露方法
defineExpose({ show, hide })
</script>

<style scoped>
.progress-bar-container {
  position: fixed;
  top: 60px;
  left: 0;
  right: 0;
  background: white;
  padding: 12px 20px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.15);
  z-index: 2000;
  display: flex;
  align-items: center;
  gap: 15px;
}

.progress-bar-container.status-complete {
  background: linear-gradient(135deg, #f0f9eb 0%, #e1f3d8 100%);
}

.progress-bar-container.status-error {
  background: linear-gradient(135deg, #fef0f0 0%, #fde2e2 100%);
}

.progress-content {
  flex: 1;
}

.progress-info {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 8px;
}

.progress-text {
  color: #606266;
  font-size: 14px;
}

.progress-percent {
  color: #409eff;
  font-weight: 600;
  font-size: 14px;
}

.status-complete .progress-percent {
  color: #67c23a;
}

.status-error .progress-percent {
  color: #f56c6c;
}

.close-btn {
  flex-shrink: 0;
}

/* 过渡动画 */
.slide-enter-active,
.slide-leave-active {
  transition: all 0.3s ease;
}

.slide-enter-from,
.slide-leave-to {
  transform: translateY(-100%);
  opacity: 0;
}
</style>
