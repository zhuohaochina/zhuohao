<template>
  <div class="fields-sync">
    <!-- 页面头部 -->
    <div class="header">
      <div class="header-left">
        <el-button @click="goBack" :icon="ArrowLeft">返回</el-button>
        <h2>更新接口&字段</h2>
      </div>
      <div class="header-right">
        <span v-if="checkingAll" class="check-all-progress">
          全局进度 {{ checkAllCurrent }}/{{ checkAllTotal }}
        </span>
        <el-button
          type="primary"
          @click="checkAllInterfaces"
          :loading="checkingAll"
        >
          全部比对
        </el-button>
        <el-button
          type="danger"
          plain
          @click="clearAllDebugLogs"
          :disabled="!hasAnyDebugLogs"
        >
          全部清空
        </el-button>
        <el-button
          v-if="hasDiffItems"
          type="primary"
          @click="executeSyncFields"
          :loading="syncing"
        >
          执行同步
        </el-button>
      </div>
    </div>

    <!-- 同步完成提示 -->
    <div v-if="syncExecuted" class="sync-success">
      <el-alert type="success" :closable="false">
        同步已完成！表结构已更新。
      </el-alert>
    </div>

    <!-- 加载中 -->
    <div v-if="loading" class="loading-section">
      <el-skeleton :rows="5" animated />
    </div>

    <!-- 接口卡片列表 -->
    <div v-if="!loading" class="log-section">
      <div
        v-for="(item, index) in interfaceList"
        :key="item.id"
        class="log-item"
        :class="getCardClass(item.checkResult?.status)"
      >
        <div class="debug-log-section debug-log-section-inline">
          <div class="debug-log-inline-title">
            <span class="debug-log-title">{{ index + 1 }}. {{ item.cn_name }}</span>
            <div class="interface-verify-control">
              <el-radio-group
                :model-value="item.fields_verified"
                size="small"
                :disabled="item.savingFieldsVerified"
                @change="(value) => updateFieldsVerified(item, value)"
              >
                <el-radio-button :label="0">待确认</el-radio-button>
                <el-radio-button :label="1">已确认</el-radio-button>
              </el-radio-group>
            </div>
          </div>
          <div class="debug-log-actions">
            <el-button
              size="small"
              type="primary"
              @click="checkSingleInterface(item, { skipJgdyMainContent: isJgdySummaryItem(item) })"
              :loading="item.checking"
              :disabled="isInterfaceUpdating(item.id)"
            >
              {{ isJgdySummaryItem(item) ? '基础采集' : '比对' }}
            </el-button>
            <el-button
              v-if="isJgdySummaryItem(item)"
              size="small"
              type="success"
              plain
              @click="backfillJgdyMainContent(item)"
              :loading="isJgdyMainContentUpdating(item.id)"
              :disabled="item.checking || (isInterfaceUpdating(item.id) && !isJgdyMainContentUpdating(item.id))"
            >
              补详情
            </el-button>
            <el-button
              v-if="item.checkResult?.status === 'diff'"
              size="small"
              type="warning"
              @click="syncSingleInterface(item)"
              :loading="item.syncing"
            >
              同步
            </el-button>
            <el-button
              v-if="isInterfaceUpdating(item.id)"
              size="small"
              type="danger"
              plain
              :loading="isStoppingInterfaceUpdate(item.id)"
              @click="stopInterfaceUpdate(item)"
            >
              停止
            </el-button>
            <el-button
              v-if="getDebugLogs(item.id).length > MAX_DEBUG_LOG_VIEW_PER_INTERFACE"
              size="small"
              type="primary"
              link
              @click="toggleDebugLogExpand(item.id)"
            >
              {{ isDebugLogExpanded(item.id) ? '收起' : '展开全部' }}
            </el-button>
            <el-button
              size="small"
              type="danger"
              link
              @click="clearDebugLogs(item.id)"
              :disabled="getDebugLogs(item.id).length === 0"
            >
              清空
            </el-button>
            <el-button
              size="small"
              type="primary"
              link
              @click="openSourcePage(item)"
              :disabled="!item.source_url"
            >
              来源
            </el-button>
          </div>
          <div class="debug-log-content" v-if="getVisibleDebugLogs(item.id).length > 0">
            <template v-for="(log, logIndex) in getVisibleDebugLogs(item.id)" :key="`${item.id}-${logIndex}`">
              <br v-if="!log.time && !log.label" />
              <span v-else class="debug-log-item" :class="'log-' + log.type">
                <span class="debug-log-time" v-if="log.time">{{ log.time }}</span>
                <span class="debug-log-label">{{ log.label }}:</span>
                <span class="debug-log-data">{{ log.data }}</span>
              </span>
            </template>
          </div>
          <div class="debug-log-tip" v-if="getDebugLogTip(item.id)">{{ getDebugLogTip(item.id) }}</div>
          <div class="debug-log-empty" v-else-if="getDebugLogs(item.id).length === 0">暂无调试日志，点击上方“比对”按钮开始。</div>
        </div>
      </div>
    </div>

    <!-- 示例数据弹窗 -->
    <el-dialog v-model="sampleDialogVisible" :title="currentSampleTitle" width="800px">
      <pre class="sample-data-content">{{ currentSampleData }}</pre>
    </el-dialog>

    <!-- 更新规则说明弹窗 -->
    <el-dialog v-model="updateModeHelpVisible" title="更新规则说明" width="700px">
      <div class="update-mode-help">
        <h4>三种更新模式对比</h4>
        <table class="help-table">
          <thead>
            <tr>
              <th>模式</th>
              <th>数据库值</th>
              <th>操作逻辑</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>全量</strong></td>
              <td><code>full</code></td>
              <td>先<strong>删除表中所有数据</strong>，再插入新数据</td>
            </tr>
            <tr>
              <td><strong>增量</strong></td>
              <td><code>incremental</code></td>
              <td><strong>不删除</strong>任何数据，使用 INSERT OR REPLACE（依赖唯一键）</td>
            </tr>
            <tr>
              <td><strong>删除范围后插入</strong></td>
              <td><code>delete_range_insert</code></td>
              <td>只<strong>删除指定时间段</strong>的数据，再插入该时间段的新数据</td>
            </tr>
          </tbody>
        </table>

        <h4>详细说明</h4>
        <div class="help-section">
          <h5>1. 全量 (full)</h5>
          <p class="help-flow">清空整张表 → 重新采集所有数据 → 插入</p>
          <ul>
            <li>最简单粗暴，保证数据完全一致</li>
            <li>缺点：数据量大时耗时长，更新期间表为空</li>
          </ul>
        </div>

        <div class="help-section">
          <h5>2. 增量 (incremental)</h5>
          <p class="help-flow">采集新数据 → INSERT OR REPLACE（按唯一键去重）</p>
          <ul>
            <li><strong>必须配置唯一键</strong>（如 qaId、art_code）</li>
            <li>新数据覆盖旧数据，旧数据保留</li>
            <li>如果没有唯一键，会自动回退到全量模式</li>
          </ul>
        </div>

        <div class="help-section">
          <h5>3. 删除范围后插入 (delete_range_insert)</h5>
          <p class="help-flow">删除最近 N 天数据 → 采集最近 N 天数据 → 插入</p>
          <ul>
            <li>需要配置 date_field 和 date_range</li>
            <li>只影响指定时间范围，历史数据不动</li>
            <li>适合按日期分区的数据，兼顾效率和一致性</li>
          </ul>
        </div>

        <h4>适用场景</h4>
        <ul class="scenario-list">
          <li><strong>全量</strong>：数据量小、或需要完全同步的场景</li>
          <li><strong>增量</strong>：有唯一标识、数据只增不改的场景（如公告）</li>
          <li><strong>删除范围后插入</strong>：按日期更新、历史数据稳定的场景（如每日行情）</li>
        </ul>
      </div>
    </el-dialog>

    <!-- 更新日期范围说明弹窗 -->
    <el-dialog v-model="dateRangeHelpVisible" title="更新日期范围说明" width="700px">
      <div class="update-mode-help">
        <h4>数据库字段</h4>
        <table class="help-table">
          <thead>
            <tr>
              <th>字段</th>
              <th>类型</th>
              <th>说明</th>
              <th>示例</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><code>fixed_begin_date</code></td>
              <td>TEXT</td>
              <td>固定开始日期</td>
              <td><code>'2025-01-01'</code></td>
            </tr>
            <tr>
              <td><code>date_range</code></td>
              <td>INTEGER</td>
              <td>往前推 N 天（动态计算）</td>
              <td><code>30</code> = 最近30天</td>
            </tr>
            <tr>
              <td><code>future_days</code></td>
              <td>INTEGER</td>
              <td>往后推 N 天</td>
              <td><code>15</code> = 今天+15天</td>
            </tr>
          </tbody>
        </table>

        <h4>日期范围计算逻辑</h4>
        <table class="help-table">
          <thead>
            <tr>
              <th>配置组合</th>
              <th>开始日期</th>
              <th>结束日期</th>
              <th>显示示例</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>无任何配置</td>
              <td>无</td>
              <td>今天</td>
              <td>全部数据</td>
            </tr>
            <tr>
              <td><code>fixed_begin_date</code></td>
              <td>固定值</td>
              <td>今天</td>
              <td>2025-01-01 ~ 2026-02-02</td>
            </tr>
            <tr>
              <td><code>fixed_begin_date</code> + <code>future_days</code></td>
              <td>固定值</td>
              <td>今天+N</td>
              <td>2025-01-01 ~ 2026-02-17</td>
            </tr>
            <tr>
              <td><code>date_range</code></td>
              <td>今天-N</td>
              <td>今天</td>
              <td>2026-01-03 ~ 2026-02-02</td>
            </tr>
            <tr>
              <td><code>date_range</code> + <code>future_days</code></td>
              <td>今天-N</td>
              <td>今天+N</td>
              <td>2026-01-03 ~ 2026-02-17</td>
            </tr>
          </tbody>
        </table>

        <h4>配置示例</h4>
        <div class="help-section">
          <h5>示例1：固定开始日期 + 未来15天</h5>
          <pre class="code-block">UPDATE api_metadata
SET fixed_begin_date = '2025-01-01', future_days = 15
WHERE cn_name = 'xxx';</pre>
          <p>结果：2025-01-01 ~ 今天+15天</p>
        </div>

        <div class="help-section">
          <h5>示例2：最近30天</h5>
          <pre class="code-block">UPDATE api_metadata
SET date_range = 30, fixed_begin_date = NULL
WHERE cn_name = 'xxx';</pre>
          <p>结果：今天-30天 ~ 今天</p>
        </div>

        <div class="help-section">
          <h5>示例3：恢复为全部数据</h5>
          <pre class="code-block">UPDATE api_metadata
SET fixed_begin_date = NULL, date_range = NULL, future_days = NULL
WHERE cn_name = 'xxx';</pre>
          <p>结果：全部数据</p>
        </div>

        <h4>优先级说明</h4>
        <ul class="scenario-list">
          <li><code>fixed_begin_date</code> 优先级高于 <code>date_range</code></li>
          <li>两者都配置时，使用 <code>fixed_begin_date</code></li>
          <li>两者都为空时，显示"全部数据"</li>
        </ul>
      </div>
    </el-dialog>

    <!-- 循环批次说明弹窗 -->
    <el-dialog v-model="loopStrategyHelpVisible" title="循环批次与采集流程说明" width="750px">
      <div class="update-mode-help">
        <h4>两种循环策略</h4>
        <table class="help-table">
          <thead>
            <tr>
              <th>策略</th>
              <th>配置值</th>
              <th>显示</th>
              <th>适用场景</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>一次性分页</strong></td>
              <td><code>type: 'single'</code></td>
              <td>一次性分页</td>
              <td>数据量不大、或 API 不支持日期过滤</td>
            </tr>
            <tr>
              <td><strong>日期分批+分页</strong></td>
              <td><code>type: 'double'</code></td>
              <td>每N天</td>
              <td>数据量大、需要按日期分批采集</td>
            </tr>
          </tbody>
        </table>

        <h4>采集流程</h4>
        <div class="help-section">
          <h5>策略1：一次性分页 (single)</h5>
          <div class="flow-diagram">
            <div class="flow-step">第1页(100条)</div>
            <div class="flow-arrow">→</div>
            <div class="flow-step">第2页(100条)</div>
            <div class="flow-arrow">→</div>
            <div class="flow-step">第3页(100条)</div>
            <div class="flow-arrow">→</div>
            <div class="flow-step">...</div>
            <div class="flow-arrow">→</div>
            <div class="flow-step flow-end">直到没有数据</div>
          </div>
        </div>

        <div class="help-section">
          <h5>策略2：日期分批+分页 (double)</h5>
          <div class="flow-diagram-vertical">
            <div class="batch-group">
              <div class="batch-label">日期批次1 (2026-01-01 ~ 2026-01-10):</div>
              <div class="flow-diagram">
                <div class="flow-step">第1页</div>
                <div class="flow-arrow">→</div>
                <div class="flow-step">第2页</div>
                <div class="flow-arrow">→</div>
                <div class="flow-step">...</div>
              </div>
            </div>
            <div class="batch-group">
              <div class="batch-label">日期批次2 (2026-01-11 ~ 2026-01-20):</div>
              <div class="flow-diagram">
                <div class="flow-step">第1页</div>
                <div class="flow-arrow">→</div>
                <div class="flow-step">第2页</div>
                <div class="flow-arrow">→</div>
                <div class="flow-step">...</div>
              </div>
            </div>
            <div class="batch-group">
              <div class="batch-label">... 继续下一个日期批次</div>
            </div>
          </div>
        </div>

        <h4>自动检测 API 限制</h4>
        <div class="help-section">
          <p>系统会<strong>自动检测</strong>每个 API 的实际返回数量限制：</p>
          <ol class="flow-steps">
            <li>
              <span class="step-num">1</span>
              <span class="step-text">首次请求时，系统尝试请求 <code>5000</code> 条（配置的 crawlerPageSize）</span>
            </li>
            <li>
              <span class="step-num">2</span>
              <span class="step-text">API 实际只返回 <code>100</code> 条（受 API 限制）</span>
            </li>
            <li>
              <span class="step-num">3</span>
              <span class="step-text">系统检测到限制，自动将 pageSize 调整为 <code>100</code></span>
            </li>
            <li>
              <span class="step-num">4</span>
              <span class="step-text">根据 API 返回的总数计算总页数：<code>totalPages = Math.ceil(总数 / 100)</code></span>
            </li>
            <li>
              <span class="step-num">5</span>
              <span class="step-text">循环请求每一页，直到采集完成</span>
            </li>
          </ol>
        </div>

        <h4>举例说明</h4>
        <div class="help-section">
          <p>假设某接口有 <strong>350 条</strong>数据，API 每页最多 <strong>100 条</strong>：</p>
          <table class="help-table">
            <thead>
              <tr>
                <th>请求</th>
                <th>参数</th>
                <th>返回</th>
                <th>说明</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>第1页</td>
                <td>pageSize=5000</td>
                <td>100条</td>
                <td>检测到限制，调整 pageSize=100，计算总页数=4</td>
              </tr>
              <tr>
                <td>第2页</td>
                <td>pageSize=100</td>
                <td>100条</td>
                <td>累计 200 条</td>
              </tr>
              <tr>
                <td>第3页</td>
                <td>pageSize=100</td>
                <td>100条</td>
                <td>累计 300 条</td>
              </tr>
              <tr>
                <td>第4页</td>
                <td>pageSize=100</td>
                <td>50条</td>
                <td>采集完成，共 350 条</td>
              </tr>
            </tbody>
          </table>
        </div>

        <h4>相关配置字段</h4>
        <table class="help-table">
          <thead>
            <tr>
              <th>字段</th>
              <th>位置</th>
              <th>说明</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><code>loopStrategy.type</code></td>
              <td>datacenter_config</td>
              <td><code>'single'</code> 或 <code>'double'</code></td>
            </tr>
            <tr>
              <td><code>loopStrategy.dateBatch.batchDays</code></td>
              <td>datacenter_config</td>
              <td>每批天数（如 10 天）</td>
            </tr>
            <tr>
              <td><code>pagination.crawlerPageSize</code></td>
              <td>datacenter_config</td>
              <td>期望的每页数量（会被 API 限制覆盖）</td>
            </tr>
          </tbody>
        </table>
      </div>
    </el-dialog>

  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { metadataApi, updateApi } from '../api'
import { connect, onMessage } from '../utils/websocket'
import type { ProgressMessage } from '../types'
import { ArrowLeft } from '@element-plus/icons-vue'

// 调试日志类型
interface DebugLog {
  time: string
  type: 'info' | 'request' | 'response' | 'success' | 'error' | 'warning'
  label: string
  data: string
}

interface InterfaceItem {
  id: number
  cn_name: string
  table_name: string
  source_url: string
  request_method: string
  update_mode: string
  date_field: string | null
  date_range: number | null
  future_days: number | null
  fixed_begin_date: string | null
  fields_verified: number
  savingFieldsVerified: boolean
  checking: boolean
  syncing: boolean
  checkResult: any | null
  datacenter_config?: string  // JSON 字符串
  // 实际最大返回数量检测
  testingMaxPageSize: boolean
  maxPageSizeResult: { maxPageSize: number | null; message: string } | null
}

const router = useRouter()

const loading = ref(false)
const syncing = ref(false)
const checkingAll = ref(false)
const syncExecuted = ref(false)
const interfaceList = ref<InterfaceItem[]>([])
const checkAllCurrent = ref(0)
const checkAllTotal = ref(0)
const sampleDialogVisible = ref(false)
const currentSampleTitle = ref('')
const currentSampleData = ref('')
const updateModeHelpVisible = ref(false)
const dateRangeHelpVisible = ref(false)
const loopStrategyHelpVisible = ref(false)

// 调试日志（按接口独立存储）
const MAX_DEBUG_LOG_VIEW_PER_INTERFACE = 120
const DEBUG_LOG_STORAGE_KEY = 'fields-sync-debug-logs-v1'
const MAX_PERSIST_LOGS_PER_INTERFACE = 2000
const FALLBACK_PERSIST_LOGS_PER_INTERFACE = 300
const LAST_TOTAL_DURATION_STORAGE_KEY = 'dashboard-last-total-duration-v1'
const debugLogsByInterface = ref<Record<number, DebugLog[]>>({})
const expandedDebugLogIds = ref<Set<number>>(new Set())
const currentUpdatingIds = ref<Set<number>>(new Set())
const currentUpdatingActionByInterface = ref<Record<number, 'collect' | 'mainContent'>>({})
const stoppingUpdateIds = ref<Set<number>>(new Set())
let persistDebugLogTimer: ReturnType<typeof setTimeout> | null = null

interface LastTotalDurationCache {
  duration: number
  finishedAt: string
  source: 'batch_update' | 'fields_sync_check_all'
}

interface PersistedDebugLogCache {
  version: number
  savedAt: string
  logsByInterface: Record<string, DebugLog[]>
}

function normalizeDebugLog(rawLog: any): DebugLog | null {
  if (!rawLog || typeof rawLog !== 'object') return null

  const allowedTypes: DebugLog['type'][] = ['info', 'request', 'response', 'success', 'error', 'warning']
  const type = allowedTypes.includes(rawLog.type) ? rawLog.type : 'info'
  const time = typeof rawLog.time === 'string' ? rawLog.time : ''
  const label = typeof rawLog.label === 'string' ? rawLog.label : ''
  const data = typeof rawLog.data === 'string' ? rawLog.data : ''

  return { time, type, label, data }
}

function saveDebugLogsToStorage(limitPerInterface: number = MAX_PERSIST_LOGS_PER_INTERFACE): void {
  try {
    if (typeof window === 'undefined') return

    const logsByInterface: Record<string, DebugLog[]> = {}
    for (const [interfaceId, logs] of Object.entries(debugLogsByInterface.value)) {
      const validLogs = (logs || [])
        .map(normalizeDebugLog)
        .filter((log): log is DebugLog => log !== null)

      if (validLogs.length > 0) {
        logsByInterface[interfaceId] = validLogs.slice(-limitPerInterface)
      }
    }

    const payload: PersistedDebugLogCache = {
      version: 1,
      savedAt: new Date().toISOString(),
      logsByInterface
    }

    localStorage.setItem(DEBUG_LOG_STORAGE_KEY, JSON.stringify(payload))
  } catch (error) {
    if (limitPerInterface !== FALLBACK_PERSIST_LOGS_PER_INTERFACE) {
      saveDebugLogsToStorage(FALLBACK_PERSIST_LOGS_PER_INTERFACE)
    }
  }
}

function scheduleSaveDebugLogs(): void {
  if (persistDebugLogTimer) {
    clearTimeout(persistDebugLogTimer)
  }

  persistDebugLogTimer = setTimeout(() => {
    persistDebugLogTimer = null
    saveDebugLogsToStorage()
  }, 200)
}

function restoreDebugLogsFromStorage(): void {
  try {
    if (typeof window === 'undefined') return

    const raw = localStorage.getItem(DEBUG_LOG_STORAGE_KEY)
    if (!raw) return

    const parsed = JSON.parse(raw) as PersistedDebugLogCache
    const restoredLogsByInterface: Record<number, DebugLog[]> = {}

    for (const [interfaceIdStr, logs] of Object.entries(parsed?.logsByInterface || {})) {
      const interfaceId = Number(interfaceIdStr)
      if (!Number.isFinite(interfaceId) || interfaceId <= 0 || !Array.isArray(logs)) continue

      const normalizedLogs = logs
        .map(normalizeDebugLog)
        .filter((log): log is DebugLog => log !== null)

      if (normalizedLogs.length > 0) {
        restoredLogsByInterface[interfaceId] = normalizedLogs.slice(-MAX_PERSIST_LOGS_PER_INTERFACE)
      }
    }

    debugLogsByInterface.value = restoredLogsByInterface
  } catch (error) {
    localStorage.removeItem(DEBUG_LOG_STORAGE_KEY)
  }
}

function pruneDebugLogsForCurrentInterfaces(): void {
  if (interfaceList.value.length === 0) return

  const interfaceIdSet = new Set(interfaceList.value.map(item => item.id))
  const nextLogsByInterface: Record<number, DebugLog[]> = {}
  const nextExpandedDebugLogIds = new Set<number>()
  let hasChanged = false

  for (const [interfaceIdStr, logs] of Object.entries(debugLogsByInterface.value)) {
    const interfaceId = Number(interfaceIdStr)
    if (interfaceIdSet.has(interfaceId)) {
      nextLogsByInterface[interfaceId] = logs
    } else {
      hasChanged = true
    }
  }

  expandedDebugLogIds.value.forEach((interfaceId) => {
    if (interfaceIdSet.has(interfaceId)) {
      nextExpandedDebugLogIds.add(interfaceId)
    } else {
      hasChanged = true
    }
  })

  if (!hasChanged) return

  debugLogsByInterface.value = nextLogsByInterface
  expandedDebugLogIds.value = nextExpandedDebugLogIds
  scheduleSaveDebugLogs()
}

// 获取当前时间字符串
function getCurrentTime(): string {
  const now = new Date()
  return now.toLocaleTimeString('zh-CN', { hour12: false }) + '.' + String(now.getMilliseconds()).padStart(3, '0')
}

function getDebugLogs(interfaceId: number): DebugLog[] {
  return debugLogsByInterface.value[interfaceId] || []
}

function getVisibleDebugLogs(interfaceId: number): DebugLog[] {
  const logs = getDebugLogs(interfaceId)
  if (expandedDebugLogIds.value.has(interfaceId)) return logs
  if (logs.length <= MAX_DEBUG_LOG_VIEW_PER_INTERFACE) return logs
  return logs.slice(logs.length - MAX_DEBUG_LOG_VIEW_PER_INTERFACE)
}

function isDebugLogExpanded(interfaceId: number): boolean {
  return expandedDebugLogIds.value.has(interfaceId)
}

function toggleDebugLogExpand(interfaceId: number) {
  if (expandedDebugLogIds.value.has(interfaceId)) {
    expandedDebugLogIds.value.delete(interfaceId)
  } else {
    expandedDebugLogIds.value.add(interfaceId)
  }

  scheduleSaveDebugLogs()
}

function getDebugLogTip(interfaceId: number): string {
  const logs = getDebugLogs(interfaceId)
  const hiddenCount = Math.max(0, logs.length - MAX_DEBUG_LOG_VIEW_PER_INTERFACE)
  const expanded = expandedDebugLogIds.value.has(interfaceId)

  const parts: string[] = []
  if (!expanded && hiddenCount > 0) {
    parts.push(`仅显示最近 ${MAX_DEBUG_LOG_VIEW_PER_INTERFACE} 条，已隐藏 ${hiddenCount} 条`) 
  } else if (expanded) {
    parts.push(`当前显示全部 ${logs.length} 条`)
  }

  return parts.join('；')
}

// 添加调试日志
function addDebugLog(interfaceId: number, type: DebugLog['type'], label: string, data: any) {
  if (!debugLogsByInterface.value[interfaceId]) {
    debugLogsByInterface.value[interfaceId] = []
  }

  const logData = typeof data === 'string' ? data : JSON.stringify(data)

  // 如果label和data都为空，说明是空行
  if (label === '' && logData === '') {
    debugLogsByInterface.value[interfaceId].push({
      time: '',
      type,
      label: '',
      data: ''
    })
  } else {
    debugLogsByInterface.value[interfaceId].push({
      time: getCurrentTime(),
      type,
      label,
      data: logData
    })
  }

  scheduleSaveDebugLogs()

}

// 清空调试日志
function clearDebugLogs(interfaceId: number) {
  debugLogsByInterface.value[interfaceId] = []
  expandedDebugLogIds.value.delete(interfaceId)
  scheduleSaveDebugLogs()
}

function isInterfaceUpdating(interfaceId: number): boolean {
  return currentUpdatingIds.value.has(interfaceId)
}

function isJgdyMainContentUpdating(interfaceId: number): boolean {
  return currentUpdatingActionByInterface.value[interfaceId] === 'mainContent'
}

function isStoppingInterfaceUpdate(interfaceId: number): boolean {
  return stoppingUpdateIds.value.has(interfaceId)
}

function addInterfaceUpdating(interfaceId: number, action: 'collect' | 'mainContent' = 'collect'): void {
  if (!currentUpdatingIds.value.has(interfaceId)) {
    const nextSet = new Set(currentUpdatingIds.value)
    nextSet.add(interfaceId)
    currentUpdatingIds.value = nextSet
  }

  currentUpdatingActionByInterface.value = {
    ...currentUpdatingActionByInterface.value,
    [interfaceId]: action
  }
}

function removeInterfaceUpdating(interfaceId: number): void {
  if (currentUpdatingIds.value.has(interfaceId)) {
    const nextSet = new Set(currentUpdatingIds.value)
    nextSet.delete(interfaceId)
    currentUpdatingIds.value = nextSet
  }

  const nextActions = { ...currentUpdatingActionByInterface.value }
  delete nextActions[interfaceId]
  currentUpdatingActionByInterface.value = nextActions
}

function isJgdySummaryItem(item: InterfaceItem): boolean {
  return item.table_name === 'biz_eastmoney_jgdy_summary' || item.cn_name.includes('机构调研统计汇总')
}

async function stopInterfaceUpdate(item: InterfaceItem) {
  if (!isInterfaceUpdating(item.id) || isStoppingInterfaceUpdate(item.id)) {
    return
  }

  try {
    await ElMessageBox.confirm(`确定停止 ${item.cn_name} 当前采集任务吗？`, '停止采集', {
      type: 'warning',
      confirmButtonText: '确定停止',
      cancelButtonText: '取消'
    })

    const nextSet = new Set(stoppingUpdateIds.value)
    nextSet.add(item.id)
    stoppingUpdateIds.value = nextSet

    addDebugLog(item.id, 'warning', '请求', `POST /api/update/stop/${item.id}`)
    const res = await updateApi.stopUpdate(item.id) as any

    if (res?.success) {
      removeInterfaceUpdating(item.id)
      addDebugLog(item.id, 'warning', '停止', res.message || '已发送停止请求，等待当前页完成后中断')
      ElMessage.success(res.message || '已发送停止请求')
    } else {
      addDebugLog(item.id, 'error', '停止失败', res?.message || '未知错误')
      ElMessage.error(res?.message || '停止采集失败')
    }
  } catch (error) {
    if ((error as any) === 'cancel') {
      return
    }

    const message = (error as Error).message || '停止采集失败'
    addDebugLog(item.id, 'error', '停止异常', message)
    ElMessage.error(message)
  } finally {
    const nextSet = new Set(stoppingUpdateIds.value)
    nextSet.delete(item.id)
    stoppingUpdateIds.value = nextSet
  }
}

// 计算是否有需要同步的项
const hasDiffItems = computed(() => {
  return interfaceList.value.some(item => item.checkResult?.status === 'diff')
})

const hasAnyDebugLogs = computed(() => {
  return Object.values(debugLogsByInterface.value).some((logs) => logs.length > 0)
})

function goBack() {
  router.push('/')
}

function clearAllDebugLogs(): void {
  if (!hasAnyDebugLogs.value) return
  debugLogsByInterface.value = {}
  expandedDebugLogIds.value = new Set()
  scheduleSaveDebugLogs()
}

function normalizeFieldsVerified(value: unknown): number {
  return value === 1 || value === '1' || value === true ? 1 : 0
}

async function updateFieldsVerified(item: InterfaceItem, value: number | string | boolean) {
  if (item.savingFieldsVerified) return

  const nextValue = normalizeFieldsVerified(value)
  if (item.fields_verified === nextValue) return

  const previousValue = item.fields_verified
  item.fields_verified = nextValue
  item.savingFieldsVerified = true

  try {
    const res = await metadataApi.update(item.id, { fields_verified: nextValue }) as any
    if (!res?.success) {
      throw new Error(res?.message || '保存失败')
    }
    item.fields_verified = normalizeFieldsVerified(res?.data?.fields_verified ?? nextValue)
  } catch {
    item.fields_verified = previousValue
    ElMessage.error(`保存 ${item.cn_name} 状态失败`)
  } finally {
    item.savingFieldsVerified = false
  }
}

function openSourcePage(item: InterfaceItem) {
  if (!item.source_url) {
    ElMessage.warning('该接口未配置来源地址')
    return
  }

  window.open(item.source_url, '_blank', 'noopener,noreferrer')
}

function formatActualParams(params: Record<string, any> | null | undefined): string {
  if (!params || Object.keys(params).length === 0) return '-'

  return Object.entries(params)
    .map(([key, value]) => {
      const strValue = String(value)
      const displayValue = strValue.length > 50 ? strValue.substring(0, 50) + '...' : strValue
      return `${key}=${displayValue}`
    })
    .join(', ')
}

function sortFields(fields: string[]): string[] {
  if (!fields || fields.length === 0) return []
  return [...fields].sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()))
}

// 格式化字段并高亮差异
function formatFieldsWithHighlight(fields: string[], diffFields: string[] | undefined, type: 'added' | 'removed'): string {
  if (!fields || fields.length === 0) return '-'

  const sortedFields = sortFields(fields)
  const diffSet = new Set((diffFields || []).map(f => f.toLowerCase()))

  return sortedFields.map(field => {
    if (diffSet.has(field.toLowerCase())) {
      const className = type === 'added' ? 'field-added' : 'field-removed'
      return `<span class="${className}">${field}</span>`
    }
    return field
  }).join(', ')
}

function getStatusClass(status: string | undefined): string {
  switch (status) {
    case 'ok': return 'text-success'
    case 'diff': return 'text-warning'
    case 'skip': return 'text-info'
    case 'error': return 'text-danger'
    default: return 'text-info'
  }
}

function getCardClass(status: string | undefined): string {
  switch (status) {
    case 'ok': return 'card-ok'
    case 'diff': return 'card-diff'
    case 'error': return 'card-error'
    case 'skip': return 'card-skip'
    default: return ''
  }
}

function getConclusion(result: any): string {
  if (!result) return '-'
  switch (result.status) {
    case 'ok':
      return `字段一致 (共 ${result.apiFields?.length || 0} 个字段)`
    case 'diff':
      const added = result.addedFields?.length || 0
      const removed = result.removedFields?.length || 0
      let msg = '需要同步 - '
      if (added > 0) msg += `新增 ${added} 个字段`
      if (added > 0 && removed > 0) msg += '，'
      if (removed > 0) msg += `删除 ${removed} 个字段`
      return msg
    case 'skip':
      return result.message || '跳过'
    case 'error':
      return result.message || '检查失败'
    default:
      return '-'
  }
}

function showSampleData(item: InterfaceItem) {
  currentSampleTitle.value = `${item.cn_name} - 示例数据`
  currentSampleData.value = JSON.stringify(item.checkResult?.sampleData, null, 2)
  sampleDialogVisible.value = true
}

function showUpdateModeHelp() {
  updateModeHelpVisible.value = true
}

function showDateRangeHelp() {
  dateRangeHelpVisible.value = true
}

function showLoopStrategyHelp() {
  loopStrategyHelpVisible.value = true
}

// 获取硬编码信息
function getHardcodedInfo(item: InterfaceItem): string | null {
  const tableName = item.checkResult?.table_name
  if (!tableName) return null

  const infos: string[] = []

  // 上证e互动 - HTML解析
  if (tableName === 'sse_ehudong' || tableName === 'sse_ehudong2') {
    infos.push('HTML解析，字段由代码定义：qaId, stockCode, stockName, askerName, askTime, askSource, question, answererName, answerTime, answerSource, answer, eitime')
  }

  // A股公告 - 嵌套数据展开
  if (tableName === 'eastmoney_announcement_raw') {
    infos.push('嵌套数据展开：codes[0] → stock_code/stock_name/market_code/ann_type，columns[0] → column_code/column_name，拼接 pdf_url')
  }

  // id → api_id 转换
  if (item.checkResult?.apiFields?.includes('id')) {
    infos.push('id → api_id (避免与主键冲突)')
  }

  return infos.length > 0 ? infos.join('；') : null
}

// 解析 datacenter_config
function parseConfig(item: InterfaceItem): any | null {
  if (!item.datacenter_config) return null
  try {
    return JSON.parse(item.datacenter_config)
  } catch {
    return null
  }
}

function normalizeWeekday(value: number): number {
  return ((value % 7) + 7) % 7
}

function calculateRelativeWeekdayDate(rule: any, referenceDate: Date): Date | null {
  if (!rule || rule.mode !== 'relative_weekday') return null

  const weekday = normalizeWeekday(Number(rule.weekday ?? 0))
  const weekStartsOn = normalizeWeekday(Number(rule.weekStartsOn ?? 1))
  const weeksAgo = Math.max(1, Math.trunc(Number(rule.weeksAgo ?? 1)))
  const beijingNow = new Date(referenceDate.getTime() + 8 * 60 * 60 * 1000)
  beijingNow.setUTCHours(0, 0, 0, 0)

  const currentWeekOffset = normalizeWeekday(beijingNow.getUTCDay() - weekStartsOn)
  const currentWeekStart = new Date(beijingNow)
  currentWeekStart.setUTCDate(currentWeekStart.getUTCDate() - currentWeekOffset - weeksAgo * 7)

  const targetDate = new Date(currentWeekStart)
  targetDate.setUTCDate(targetDate.getUTCDate() + normalizeWeekday(weekday - weekStartsOn))

  return targetDate
}

function getRelativeDateRuleLabel(rule: any): string | null {
  if (!rule || rule.mode !== 'relative_weekday') return null

  const weekdayNames = ['周日', '周一', '周二', '周三', '周四', '周五', '周六']
  const weekday = normalizeWeekday(Number(rule.weekday ?? 0))
  const weeksAgo = Math.max(1, Math.trunc(Number(rule.weeksAgo ?? 1)))

  if (weeksAgo === 1) {
    return `上${weekdayNames[weekday]}`
  }

  return `${weeksAgo}周前${weekdayNames[weekday]}`
}

// 获取更新规则显示（从逻辑字段生成）
function getUpdateRuleDisplay(item: InterfaceItem): string {
  const parts: string[] = []
  const config = parseConfig(item)
  const relativeDateLabel = getRelativeDateRuleLabel(config?.dateConfig?.relativeDateRule)

  // 更新模式：从 update_mode 生成
  if (item.update_mode === 'incremental') {
    parts.push('增量')
  } else if (item.update_mode === 'full') {
    parts.push('全量')
  }

  // 日期字段：从 loopStrategy.dateBatch.dateFieldLabel 或 date_field 读取
  if (config?.loopStrategy?.dateBatch?.dateFieldLabel) {
    parts.push(config.loopStrategy.dateBatch.dateFieldLabel)
  } else if (item.date_field) {
    parts.push(item.date_field)
  }

  // 日期范围天数：从 date_range 和 future_days 生成
  if (item.date_range) {
    const dateUnitLabel = config?.dateConfig?.dateUnit === 'trading_day' ? '个交易日' : '天'
    if (item.future_days && item.future_days > 0) {
      parts.push(`-${item.date_range}${dateUnitLabel}~+${item.future_days}${dateUnitLabel}`)
    } else {
      parts.push(`${item.date_range}${dateUnitLabel}`)
    }
  } else if (relativeDateLabel) {
    parts.push(relativeDateLabel)
  }

  return parts.length > 0 ? parts.join(' | ') : '-'
}

function isClosedDate(date: Date, closedDateRanges: Array<{ begin: string; end: string }> = []): boolean {
  const key = date.toISOString().split('T')[0]
  return closedDateRanges.some(range => key >= range.begin && key <= range.end)
}

function isTradingDate(date: Date, closedDateRanges: Array<{ begin: string; end: string }> = []): boolean {
  const day = date.getUTCDay()
  return day >= 1 && day <= 5 && !isClosedDate(date, closedDateRanges)
}

function addTradingDays(date: Date, days: number, closedDateRanges: Array<{ begin: string; end: string }> = []): Date {
  const direction = days > 0 ? 1 : -1
  let remaining = Math.abs(days)
  const next = new Date(date)

  while (remaining > 0) {
    next.setUTCDate(next.getUTCDate() + direction)
    if (isTradingDate(next, closedDateRanges)) {
      remaining--
    }
  }

  return next
}

function resolveTradingEndDate(date: Date, futureDays = 0, closedDateRanges: Array<{ begin: string; end: string }> = []): Date {
  if (futureDays > 0) {
    return addTradingDays(date, futureDays, closedDateRanges)
  }

  const endDate = new Date(date)
  while (!isTradingDate(endDate, closedDateRanges)) {
    endDate.setUTCDate(endDate.getUTCDate() - 1)
  }
  return endDate
}

// 获取更新日期范围明细
function getUpdateDateRangeDetails(item: InterfaceItem): { startDate: string | null; endDate: string | null } {
  const config = parseConfig(item)
  const now = new Date()
  const beijingNow = new Date(now.getTime() + 8 * 60 * 60 * 1000)
  const formatDate = (date: Date): string => date.toISOString().split('T')[0]
  const isTradingDayScope = config?.dateConfig?.dateUnit === 'trading_day'
  const closedDateRanges = config?.dateConfig?.closedDateRanges || []

  // 计算开始日期
  let startDateStr: string | null = null
  let endDateStr: string | null = null
  const relativeDate = calculateRelativeWeekdayDate(config?.dateConfig?.relativeDateRule, now)
  if (relativeDate) {
    startDateStr = formatDate(relativeDate)
    endDateStr = formatDate(relativeDate)
  } else if (item.fixed_begin_date) {
    // 使用固定开始日期
    startDateStr = item.fixed_begin_date
  } else if (item.date_range) {
    if (isTradingDayScope) {
      const endDate = resolveTradingEndDate(beijingNow, item.future_days || 0, closedDateRanges)
      const startDate = addTradingDays(endDate, -(item.date_range - 1), closedDateRanges)
      startDateStr = formatDate(startDate)
      endDateStr = formatDate(endDate)
    } else {
    // 动态计算：今天 - N 天
      const startDate = new Date(beijingNow)
      const backwardDays = config?.dateConfig?.dateRangeMode === 'count_including_today'
        ? Math.max(0, item.date_range - 1)
        : item.date_range
      startDate.setDate(startDate.getDate() - backwardDays)
      startDateStr = formatDate(startDate)
    }
  }

  if (startDateStr && !endDateStr && !relativeDate) {
    // 计算结束日期（基于 metadata 的 future_days）
    const endDate = new Date(beijingNow)
    if (item.future_days) {
      endDate.setDate(endDate.getDate() + item.future_days)
    }
    endDateStr = formatDate(endDate)
  } else if (config?.displayConfig?.fixedDateRange) {
    // 仅在 metadata 未配置时，才降级使用 displayConfig
    const { begin, end } = config.displayConfig.fixedDateRange
    startDateStr = begin || null
    endDateStr = end || null
  }

  // 无可用日期范围配置：返回空
  if (!startDateStr || !endDateStr) {
    return {
      startDate: null,
      endDate: null
    }
  }

  return {
    startDate: startDateStr,
    endDate: endDateStr
  }
}

// 获取更新日期范围显示
function getUpdateDateRangeDisplay(item: InterfaceItem): string {
  const { startDate, endDate } = getUpdateDateRangeDetails(item)
  if (!startDate || !endDate) {
    return '全部数据'
  }

  return `${startDate} ~ ${endDate}`
}

// 获取根据字段取数显示
function getFetchDateFieldDisplay(item: InterfaceItem): string {
  const config = parseConfig(item)
  const dateBatch = config?.loopStrategy?.dateBatch

  if (dateBatch?.dateField) {
    const label = dateBatch.dateFieldLabel ? `（${dateBatch.dateFieldLabel}）` : ''
    return `${dateBatch.dateField}${label}`
  }

  const dateConfig = config?.dateConfig
  if (dateConfig?.type === 'param') {
    const beginParam = dateConfig.beginParam || 'beginTime'
    const endParam = dateConfig.endParam || 'endTime'
    return `${beginParam} ~ ${endParam}（日期参数）`
  }

  const filterDateField = dateConfig?.field
  if (filterDateField) {
    const label = dateConfig?.type === 'filter' ? '（过滤字段）' : ''
    return `${filterDateField}${label}`
  }

  if (item.date_field) {
    return item.date_field
  }

  return '-'
}

// 获取循环批次显示
function getBatchSizeDisplay(item: InterfaceItem): string {
  const config = parseConfig(item)
  if (!config?.loopStrategy) return '-'

  const { type, dateBatch } = config.loopStrategy

  if (type === 'double' && dateBatch?.batchDays) {
    return `每${dateBatch.batchDays}天`
  }

  if (type === 'single') {
    return '一次性分页'
  }

  return '-'
}

// 获取实际最大返回数量的样式类
function getMaxPageSizeClass(result: any): string {
  if (!result) return 'text-info'
  if (result.isConfirmed) return 'text-success'
  return 'text-warning'  // 数据量不足，无法确定
}

// 获取日期过滤说明
function getDateFilterInfo(item: InterfaceItem): string | null {
  if (!item.datacenter_config) return null

  try {
    const config = JSON.parse(item.datacenter_config)
    const dateBatch = config.loopStrategy?.dateBatch
    if (!dateBatch || !dateBatch.beginDateParam || !dateBatch.dateField) return null

    const label = dateBatch.dateFieldLabel ? `（${dateBatch.dateFieldLabel}）` : ''
    return `返回数据中 ${dateBatch.dateField}${label} 字段的值会被限制在请求参数 ${dateBatch.beginDateParam}/${dateBatch.endDateParam} 指定的日期范围内`
  } catch {
    return null
  }
}

// 获取循环策略信息
function getLoopStrategyInfo(item: InterfaceItem): string | null {
  if (!item.datacenter_config) return null

  try {
    const config = JSON.parse(item.datacenter_config)
    if (!config.loopStrategy) return null

    const { type, dateBatch, requestDelay } = config.loopStrategy
    const parts: string[] = []

    if (type === 'single') {
      parts.push('单层循环（仅分页）')
    } else if (type === 'double') {
      parts.push('双层循环（日期分批+分页）')
      if (dateBatch) {
        parts.push(`每${dateBatch.batchDays}天一批`)
        if (dateBatch.useSingleDate) {
          parts.push('按天循环')
        }
        if (dateBatch.beginDateParam && dateBatch.endDateParam) {
          let dateInfo = `请求参数: ${dateBatch.beginDateParam}/${dateBatch.endDateParam}`
          if (dateBatch.dateField) {
            const label = dateBatch.dateFieldLabel ? `（${dateBatch.dateFieldLabel}）` : ''
            dateInfo += ` → 对应返回字段: ${dateBatch.dateField}${label}`
          }
          parts.push(dateInfo)
        }
      }
    }

    if (requestDelay) {
      const isAnnouncementApi = config.apiType === 'announcement'
      const pageDelay = isAnnouncementApi
        ? Math.max(requestDelay.betweenPages || 0, 1000)
        : requestDelay.betweenPages
      const batchDelay = isAnnouncementApi
        ? Math.max(requestDelay.betweenBatches || 0, 1500)
        : requestDelay.betweenBatches
      const delays: string[] = []
      if (pageDelay) {
        delays.push(`${isAnnouncementApi ? '页间至少' : '页间'}${pageDelay}ms`)
      }
      if (batchDelay) {
        delays.push(`${isAnnouncementApi ? '批间至少' : '批间'}${batchDelay}ms`)
      }
      if (delays.length > 0) {
        parts.push(`延迟: ${delays.join(', ')}${isAnnouncementApi ? '（公告接口保护）' : ''}`)
      }
    }

    return parts.join('，')
  } catch {
    return null
  }
}

// 获取采集统计信息显示
function getCrawlStatsDisplay(item: InterfaceItem): string {
  if (!item.checkResult?.crawlStats) return '-'

  const stats = item.checkResult.crawlStats
  const parts: string[] = []

  // 批次数
  if (stats.totalBatches > 1) {
    parts.push(`共${stats.totalBatches}个批次`)
  }

  // API 返回的记录数（测试批次）
  if (stats.apiTotalRecord !== null) {
    parts.push(`测试批次${stats.apiTotalRecord}条`)
    if (stats.hasMorePages) {
      parts.push('⚠️有分页')
    }
  }

  return parts.length > 0 ? parts.join('，') : '-'
}

// 获取接口列表（不请求字段比对）
async function loadInterfaceList() {
  try {
    loading.value = true
    const res = await metadataApi.getList() as any
    if (res.success) {
      interfaceList.value = res.data.map((item: any) => ({
        id: item.id,
        cn_name: item.cn_name,
        table_name: item.table_name || '',
        source_url: item.source_url || '',
        request_method: item.request_method || '-',
        update_mode: item.update_mode || 'full',
        date_field: item.date_field || null,
        date_range: item.date_range || null,
        future_days: item.future_days || null,
        fixed_begin_date: item.fixed_begin_date || null,
        fields_verified: normalizeFieldsVerified(item.fields_verified),
        savingFieldsVerified: false,
        checking: false,
        syncing: false,
        checkResult: null,
        datacenter_config: item.datacenter_config,
        // 实际最大返回数量检测
        testingMaxPageSize: false,
        maxPageSizeResult: null
      }))

      pruneDebugLogsForCurrentInterfaces()
    } else {
      ElMessage.error('获取接口列表失败')
    }
  } catch (error) {
    ElMessage.error('获取接口列表失败')
  } finally {
    loading.value = false
  }
}

// 检测实际最大返回数量
async function testMaxPageSize(item: InterfaceItem) {
  try {
    item.testingMaxPageSize = true

    const res = await metadataApi.testMaxPageSize(item.id) as any
    if (res.success) {
      item.maxPageSizeResult = res.data
      if (res.data.isConfirmed) {
        ElMessage.success(`${item.cn_name} 实际最大返回数量: ${res.data.message}`)
      } else {
        ElMessage.warning(`${item.cn_name}: ${res.data.message}`)
      }
    } else {
      ElMessage.error(`检测 ${item.cn_name} 失败`)
    }
  } catch (error) {
    ElMessage.error(`检测 ${item.cn_name} 失败: ${(error as Error).message}`)
  } finally {
    item.testingMaxPageSize = false
  }
}

// 延迟函数
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function formatElapsed(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  const seconds = Math.floor(ms / 1000)
  if (seconds < 60) return `${seconds}秒`
  const minutes = Math.floor(seconds / 60)
  const remainingSeconds = seconds % 60
  if (minutes < 60) return remainingSeconds > 0 ? `${minutes}分${remainingSeconds}秒` : `${minutes}分`
  const hours = Math.floor(minutes / 60)
  const remainingMinutes = minutes % 60
  return remainingMinutes > 0 ? `${hours}小时${remainingMinutes}分` : `${hours}小时`
}

function saveLastTotalDuration(duration: number): void {
  if (duration <= 0 || typeof window === 'undefined') return

  const payload: LastTotalDurationCache = {
    duration,
    finishedAt: new Date().toISOString(),
    source: 'fields_sync_check_all'
  }

  try {
    localStorage.setItem(LAST_TOTAL_DURATION_STORAGE_KEY, JSON.stringify(payload))
  } catch {
    // 忽略本地存储异常
  }
}

async function waitForAllInterfaceUpdatesDone(timeoutMs: number = 60 * 60 * 1000): Promise<boolean> {
  const startedAt = Date.now()
  while (currentUpdatingIds.value.size > 0) {
    if (Date.now() - startedAt >= timeoutMs) {
      return false
    }
    await sleep(200)
  }
  return true
}

// 检查单个接口（带日志）
async function checkSingleInterface(item: InterfaceItem, options: { skipJgdyMainContent?: boolean } = {}) {
  try {
    item.checking = true
    clearDebugLogs(item.id)

    // 每次比对前刷新该接口最新配置，避免页面未刷新导致日志显示旧配置
    try {
      const latestRes = await metadataApi.getById(item.id) as any
      if (latestRes?.success && latestRes.data) {
        const latest = latestRes.data
        item.table_name = latest.table_name || item.table_name
        item.source_url = latest.source_url || item.source_url
        item.request_method = latest.request_method || item.request_method
        item.update_mode = latest.update_mode || item.update_mode
        item.date_field = latest.date_field ?? item.date_field
        item.date_range = latest.date_range ?? item.date_range
        item.future_days = latest.future_days ?? item.future_days
        item.fixed_begin_date = latest.fixed_begin_date ?? item.fixed_begin_date
        item.fields_verified = normalizeFieldsVerified(latest.fields_verified ?? item.fields_verified)
        item.datacenter_config = latest.datacenter_config || item.datacenter_config
      }
    } catch {
      // 拉取最新配置失败时，继续使用当前页面已有配置
    }

    addDebugLog(item.id, 'info', '开始比对', `${item.cn_name} (ID:${item.id}) —— 验证API返回字段与数据库字段是否一致`)
    addDebugLog(item.id, 'request', '请求', `GET /api/metadata/${item.id}/check-fields`)

    const res = await metadataApi.checkSingleField(item.id) as any

    if (res.success) {
      item.checkResult = res.data
      const d = res.data

      addDebugLog(item.id, 'response', '响应', `success:true`)
      if (d.actualUrl) addDebugLog(item.id, 'info', '实际URL', d.actualUrl)
      if (d.actualParams && Object.keys(d.actualParams).length > 0) addDebugLog(item.id, 'info', '参数(本页)', JSON.stringify(d.actualParams))
      if (d.apiFields?.length > 0) addDebugLog(item.id, 'info', `API字段(${d.apiFields.length})`, d.apiFields.join(','))
      if (d.dbFields?.length > 0) addDebugLog(item.id, 'info', `DB字段(${d.dbFields.length})`, d.dbFields.join(','))
      if (d.addedFields?.length > 0) addDebugLog(item.id, 'warning', '新增字段', d.addedFields.join(','))
      if (d.removedFields?.length > 0) addDebugLog(item.id, 'warning', '删除字段', d.removedFields.join(','))
      if (d.sampleData) addDebugLog(item.id, 'info', '示例数据', JSON.stringify(d.sampleData))

      const statusMap: Record<string, DebugLog['type']> = { 'ok': 'success', 'diff': 'warning', 'skip': 'info', 'error': 'error' }
      addDebugLog(item.id, statusMap[d.status] || 'info', '结论', d.message || getConclusion(d))

      // 延迟 3 秒再进行下一个检测
      addDebugLog(item.id, 'info', '', '')  // 空行
      addDebugLog(item.id, 'info', '等待', '延迟 3 秒后继续下一个检测...')
      await sleep(3000)

      // 检测实际最大返回数量
      addDebugLog(item.id, 'info', '', '')  // 空行
      addDebugLog(item.id, 'info', '检测API单次最大返回量', `${item.cn_name} —— 验证API单次请求最多返回多少条数据`)
      addDebugLog(item.id, 'request', '请求', `POST /api/metadata/${item.id}/test-max-page-size`)

      try {
        const maxRes = await metadataApi.testMaxPageSize(item.id) as any
        if (maxRes.success) {
          item.maxPageSizeResult = maxRes.data
          addDebugLog(item.id, 'response', '响应', `success:true`)
          addDebugLog(item.id, maxRes.data.isConfirmed ? 'success' : 'warning', '结论', maxRes.data.message)
        } else {
          addDebugLog(item.id, 'error', '失败', maxRes.message || '未知错误')
        }
      } catch (e) {
        addDebugLog(item.id, 'error', '异常', (e as Error).message)
      }

      // 延迟 3 秒再进行下一个检测
      addDebugLog(item.id, 'info', '', '')  // 空行
      addDebugLog(item.id, 'info', '等待', '延迟 3 秒后继续下一个检测...')
      await sleep(3000)

      // 执行数据采集
      addDebugLog(item.id, 'info', '', '')  // 空行
      addDebugLog(item.id, 'info', '执行数据采集', `${item.cn_name} —— 从API获取数据并存入数据库`)

      // 显示采集配置信息
      if (d.actualUrl) addDebugLog(item.id, 'info', '接口地址', d.actualUrl)
      addDebugLog(item.id, 'info', '接口方法', item.request_method || 'GET')
      addDebugLog(item.id, 'info', '更新规则', item.update_mode === 'incremental' ? '增量' : '全量')
      addDebugLog(item.id, 'info', '更新日期范围', getUpdateDateRangeDisplay(item))
      const { startDate, endDate } = getUpdateDateRangeDetails(item)
      addDebugLog(item.id, 'info', '开始日期', startDate || '-')
      addDebugLog(item.id, 'info', '结束日期', endDate || '-')
      addDebugLog(item.id, 'info', '根据字段取数', getFetchDateFieldDisplay(item))
      addDebugLog(item.id, 'info', '循环批次', getBatchSizeDisplay(item))
      addDebugLog(item.id, 'info', '循环策略', getLoopStrategyInfo(item) || '-')

      const updateRequestPath = options.skipJgdyMainContent
        ? `/api/update/${item.id}?skip_jgdy_main_content=1`
        : `/api/update/${item.id}`
      addDebugLog(item.id, 'request', '请求', `POST ${updateRequestPath}`)

      try {
        addInterfaceUpdating(item.id, 'collect')
        const updateRes = options.skipJgdyMainContent
          ? (await updateApi.triggerJgdyBasicUpdate(item.id) as any)
          : (await updateApi.triggerUpdate(item.id) as any)
        if (updateRes.success) {
          addDebugLog(item.id, 'response', '响应', `success:true，等待WebSocket推送进度...`)
        } else {
          addDebugLog(item.id, 'error', '失败', updateRes.message || '未知错误')
          removeInterfaceUpdating(item.id)
        }
      } catch (e) {
        addDebugLog(item.id, 'error', '异常', (e as Error).message)
        removeInterfaceUpdating(item.id)
        stoppingUpdateIds.value.delete(item.id)
      }
    } else {
      addDebugLog(item.id, 'error', '失败', res.message || '未知错误')
      ElMessage.error(`检查 ${item.cn_name} 失败`)
    }
  } catch (error) {
    addDebugLog(item.id, 'error', '异常', (error as Error).message)
    ElMessage.error(`检查 ${item.cn_name} 失败`)
  } finally {
    item.checking = false
    if (!currentUpdatingIds.value.has(item.id)) {
      stoppingUpdateIds.value.delete(item.id)
    }
  }
}

async function backfillJgdyMainContent(item: InterfaceItem) {
  if (!isJgdySummaryItem(item) || isInterfaceUpdating(item.id)) {
    return
  }

  try {
    if (getDebugLogs(item.id).length > 0) {
      addDebugLog(item.id, 'info', '', '')
    }
    addDebugLog(item.id, 'info', '开始补详情', `${item.cn_name} (ID:${item.id}) —— 单独请求详情接口并回填 main_content`)
    addDebugLog(item.id, 'request', '请求', `POST /api/update/${item.id}/jgdy-main-content`)

    addInterfaceUpdating(item.id, 'mainContent')
    const res = await updateApi.triggerJgdyMainContentUpdate(item.id) as any
    if (res?.success) {
      addDebugLog(item.id, 'response', '响应', 'success:true，等待WebSocket推送进度...')
    } else {
      addDebugLog(item.id, 'error', '失败', res?.message || '未知错误')
      removeInterfaceUpdating(item.id)
    }
  } catch (error) {
    addDebugLog(item.id, 'error', '异常', (error as Error).message)
    removeInterfaceUpdating(item.id)
    stoppingUpdateIds.value.delete(item.id)
    ElMessage.error(`补充 ${item.cn_name} main_content 失败`)
  }
}

// 检查所有接口
async function checkAllInterfaces() {
  const startedAt = Date.now()
  checkingAll.value = true
  checkAllCurrent.value = 0
  checkAllTotal.value = interfaceList.value.length

  try {
    for (let index = 0; index < interfaceList.value.length; index++) {
      checkAllCurrent.value = index + 1
      const item = interfaceList.value[index]
      await checkSingleInterface(item, { skipJgdyMainContent: isJgdySummaryItem(item) })
    }

    if (currentUpdatingIds.value.size > 0) {
      ElMessage.info(`全部比对请求已发出，等待 ${currentUpdatingIds.value.size} 个接口更新完成...`)
      const allDone = await waitForAllInterfaceUpdatesDone()
      if (!allDone) {
        ElMessage.warning('等待更新完成超时，部分接口可能仍在后台运行')
      }
    }

    const totalElapsed = Date.now() - startedAt
    saveLastTotalDuration(totalElapsed)

    ElMessage.success(`全部比对完成，总耗时 ${formatElapsed(totalElapsed)}`)
  } finally {
    checkingAll.value = false
    checkAllCurrent.value = 0
    checkAllTotal.value = 0
  }
}

// 同步单个接口
async function syncSingleInterface(item: InterfaceItem) {
  try {
    item.syncing = true
    const res = await metadataApi.syncSingleField(item.id) as any
    if (res.success) {
      item.checkResult = res.data
      ElMessage.success(`${item.cn_name} 同步完成`)
    } else {
      ElMessage.error(`同步 ${item.cn_name} 失败`)
    }
  } catch (error) {
    ElMessage.error(`同步 ${item.cn_name} 失败`)
  } finally {
    item.syncing = false
  }
}

// 执行同步（只同步已比对且需要同步的接口）
async function executeSyncFields() {
  try {
    syncing.value = true
    const syncRes = await metadataApi.syncFields() as any
    if (syncRes.success) {
      syncExecuted.value = true
      ElMessage.success(syncRes.message || '同步完成')
      // 清空所有比对结果，让用户重新比对
      interfaceList.value.forEach(item => {
        item.checkResult = null
      })
    } else {
      ElMessage.error('同步失败')
    }
  } catch (error) {
    ElMessage.error('同步字段失败')
  } finally {
    syncing.value = false
  }
}

onMounted(() => {
  restoreDebugLogsFromStorage()
  loadInterfaceList()

  // 连接 WebSocket 并监听进度消息
  connect()
  onMessage((message: ProgressMessage) => {
    const metadataId = Number(message.metadataId)
    if (!Number.isFinite(metadataId)) return

    // 只处理当前正在更新的接口的消息
    if (currentUpdatingIds.value.has(metadataId)) {
      if (message.type === 'progress') {
        addDebugLog(metadataId, 'info', '进度', message.message)
      } else if (message.type === 'complete') {
        addDebugLog(metadataId, 'success', '完成', message.message)
        removeInterfaceUpdating(metadataId)
        stoppingUpdateIds.value.delete(metadataId)
      } else if (message.type === 'error') {
        const messageText = message.message || ''
        const isCancelled = messageText.includes('取消') || messageText.includes('停止')
        addDebugLog(metadataId, isCancelled ? 'warning' : 'error', isCancelled ? '已停止' : '错误', messageText)
        removeInterfaceUpdating(metadataId)
        stoppingUpdateIds.value.delete(metadataId)
      }
    }
  })
})

onBeforeUnmount(() => {
  if (persistDebugLogTimer) {
    clearTimeout(persistDebugLogTimer)
    persistDebugLogTimer = null
  }
  saveDebugLogsToStorage()
})
</script>

<style scoped>
.fields-sync {
  background: white;
  padding: 20px;
  border-radius: 8px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.1);
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.header-left {
  display: flex;
  align-items: center;
  gap: 15px;
}

.header-left h2 {
  margin: 0;
  color: #303133;
}

.header-right {
  display: flex;
  align-items: center;
  gap: 10px;
}

.check-all-progress {
  color: #909399;
  font-size: 13px;
  line-height: 1;
  white-space: nowrap;
}

.sync-success {
  margin-bottom: 20px;
}

.loading-section {
  padding: 20px;
}

.text-success {
  color: #67c23a;
}

.text-warning {
  color: #e6a23c;
}

.text-danger {
  color: #f56c6c;
}

.text-info {
  color: #909399;
}

.log-section {
  margin-bottom: 20px;
}

.log-item {
  margin-bottom: 16px;
  border: none;
  border-radius: 0;
  overflow: visible;
  background: transparent;
}

.log-item:last-child {
  margin-bottom: 0;
}

.sample-data-content {
  background: #f5f7fa;
  padding: 15px;
  border-radius: 4px;
  max-height: 500px;
  overflow: auto;
  font-size: 12px;
  line-height: 1.5;
  white-space: pre-wrap;
  word-break: break-all;
}

.help-icon {
  margin-left: 4px;
  color: #909399;
  cursor: pointer;
  vertical-align: middle;
}

.help-icon:hover {
  color: #409eff;
}

.update-mode-help {
  font-size: 14px;
  line-height: 1.6;
}

.update-mode-help h4 {
  margin: 20px 0 10px;
  color: #303133;
  border-bottom: 1px solid #ebeef5;
  padding-bottom: 8px;
}

.update-mode-help h4:first-child {
  margin-top: 0;
}

.update-mode-help h5 {
  margin: 12px 0 8px;
  color: #606266;
}

.help-table {
  width: 100%;
  border-collapse: collapse;
  margin: 10px 0;
}

.help-table th,
.help-table td {
  border: 1px solid #ebeef5;
  padding: 10px 12px;
  text-align: left;
}

.help-table th {
  background: #f5f7fa;
  font-weight: 600;
  color: #303133;
}

.help-table code {
  background: #f0f0f0;
  padding: 2px 6px;
  border-radius: 3px;
  font-family: monospace;
  font-size: 13px;
}

.help-section {
  margin: 15px 0;
  padding: 12px;
  background: #fafafa;
  border-radius: 4px;
}

.help-flow {
  color: #409eff;
  font-weight: 500;
  margin: 8px 0;
}

.help-section ul {
  margin: 8px 0;
  padding-left: 20px;
}

.help-section li {
  margin: 4px 0;
  color: #606266;
}

.scenario-list {
  padding-left: 20px;
}

.scenario-list li {
  margin: 8px 0;
  color: #606266;
}

.code-block {
  background: #f5f7fa;
  padding: 12px;
  border-radius: 4px;
  font-family: monospace;
  font-size: 13px;
  line-height: 1.5;
  overflow-x: auto;
  white-space: pre;
  margin: 8px 0;
}

.flow-diagram {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  margin: 10px 0;
}

.flow-step {
  background: #ecf5ff;
  border: 1px solid #b3d8ff;
  color: #409eff;
  padding: 6px 12px;
  border-radius: 4px;
  font-size: 13px;
}

.flow-step.flow-end {
  background: #f0f9eb;
  border-color: #c2e7b0;
  color: #67c23a;
}

.flow-arrow {
  color: #909399;
  font-weight: bold;
}

.flow-diagram-vertical {
  margin: 10px 0;
}

.batch-group {
  margin: 12px 0;
  padding-left: 10px;
  border-left: 3px solid #e6a23c;
}

.batch-label {
  color: #e6a23c;
  font-weight: 500;
  margin-bottom: 8px;
  font-size: 13px;
}

.flow-steps {
  list-style: none;
  padding: 0;
  margin: 15px 0;
}

.flow-steps li {
  display: flex;
  align-items: flex-start;
  margin: 12px 0;
}

.step-num {
  background: #409eff;
  color: white;
  width: 24px;
  height: 24px;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  font-weight: bold;
  flex-shrink: 0;
  margin-right: 12px;
}

.step-text {
  color: #606266;
  line-height: 24px;
}

.step-text code {
  background: #f0f0f0;
  padding: 2px 6px;
  border-radius: 3px;
  font-family: monospace;
  color: #e6a23c;
}

.testing-text {
  margin-left: 8px;
  color: #409eff;
  font-size: 12px;
}

/* 调试日志区域样式 */
.debug-log-section {
  margin-bottom: 20px;
  border: 1px solid #dcdfe6;
  border-radius: 4px;
  overflow: hidden;
}

.debug-log-section-inline {
  margin-top: 12px;
  margin-bottom: 0;
}

.debug-log-inline-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  flex-wrap: wrap;
  padding: 8px 10px 4px 10px;
}

.debug-log-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 4px 10px 8px 10px;
  flex-wrap: wrap;
  border-bottom: 1px solid #dcdfe6;
  gap: 8px;
}

.interface-verify-control {
  display: inline-flex;
  align-items: center;
  margin-left: auto;
}

.interface-verify-control :deep(.el-radio-group) {
  display: inline-flex;
  border: 1px solid #e0e7f1;
  border-radius: 999px;
  overflow: hidden;
  background: #f7f9fc;
  box-shadow: none;
}

.interface-verify-control :deep(.el-radio-button__inner) {
  min-width: 68px;
  height: 24px;
  line-height: 22px;
  border: none;
  padding: 0 10px;
  font-size: 11px;
  font-weight: 600;
  color: #6a778c;
  background: transparent;
  box-shadow: none;
}

.interface-verify-control :deep(.el-radio-button + .el-radio-button .el-radio-button__inner) {
  border-left: 1px solid #e3eaf4;
}

.interface-verify-control :deep(.el-radio-button:first-child.is-active .el-radio-button__inner) {
  color: #8a5b00;
  background: #fff4dd;
}

.interface-verify-control :deep(.el-radio-button:last-child.is-active .el-radio-button__inner) {
  color: #147a4d;
  background: #e8f7ef;
}

.interface-verify-control :deep(.el-radio-group.is-disabled .el-radio-button__inner) {
  opacity: 0.6;
}

.debug-log-title {
  font-weight: bold;
  color: #303133;
  font-size: 12px;
}

.debug-log-content {
  background: #1e1e1e;
  padding: 8px 10px;
  font-family: 'Consolas', 'Monaco', monospace;
  font-size: 11px;
  line-height: 1.6;
  word-break: break-all;
  white-space: pre-wrap;
}

.debug-log-empty {
  background: #1e1e1e;
  padding: 20px;
  color: #718096;
  font-size: 12px;
  text-align: center;
}

.debug-log-tip {
  padding: 6px 10px;
  font-size: 12px;
  color: #8c6d1f;
  background: #fff7e6;
  border-top: 1px solid #f5deb3;
}

.debug-log-item {
  display: block;
  white-space: pre-wrap;
}

.debug-log-item.log-info .debug-log-label { color: #63b3ed; }
.debug-log-item.log-request .debug-log-label { color: #9f7aea; }
.debug-log-item.log-response .debug-log-label { color: #4fd1c5; }
.debug-log-item.log-success .debug-log-label { color: #68d391; }
.debug-log-item.log-warning .debug-log-label { color: #f6ad55; }
.debug-log-item.log-error .debug-log-label { color: #fc8181; }

.debug-log-time {
  color: #718096;
  margin-right: 6px;
}

.debug-log-label {
  font-weight: bold;
  margin-right: 4px;
}

.debug-log-data {
  color: #e2e8f0;
}
</style>
