<template>
  <div class="dashboard">
    <!-- 接口管理区域 -->
    <div class="header">
      <h2>接口管理</h2>
      <div class="header-actions">
        <el-button type="warning" @click="goToFieldsSync">
          <el-icon><Setting /></el-icon>
          更新接口&字段
        </el-button>
        <el-button type="primary" plain @click="goToDataCombos">
          <el-icon><Connection /></el-icon>
          关联视图
        </el-button>
        <el-switch
          v-model="showDisabled"
          class="show-disabled-switch"
          active-text="显示停用"
          @change="loadData"
        />
        <el-button type="success" @click="triggerUpdateAll" :loading="batchUpdating" :disabled="batchUpdating">
          <el-icon><Refresh /></el-icon>
          {{ batchUpdating ? '更新中' : '全部更新' }}
        </el-button>
        <el-button v-if="batchUpdating || updatingIds.size > 0" type="danger" @click="stopAllUpdates">
          <el-icon><VideoPause /></el-icon>
          停止采集
        </el-button>
        <span v-if="!batchUpdating && lastBatchDuration > 0" class="last-batch-duration">
          <span class="duration-label">上次总耗时</span>
          <span class="duration-value">{{ formatDuration(lastBatchDuration) }}</span>
        </span>
        <el-button type="primary" @click="checkFields" :loading="checking">
          <el-icon><Document /></el-icon>
          检查字段配置
        </el-button>
        <el-button type="danger" @click="clearAllData" :loading="clearing">
          <el-icon><Delete /></el-icon>
          清空所有数据
        </el-button>
        <div v-if="batchUpdating" class="batch-progress-panel">
          <div class="batch-progress-head">
            <span class="batch-progress-title">批量更新进度</span>
            <span class="batch-progress-ratio">{{ batchCurrent }}/{{ batchTotal }}</span>
          </div>
          <el-progress
            :percentage="batchProgressPercentage"
            :stroke-width="8"
            :show-text="false"
            status="success"
          />
          <div v-if="batchTotalElapsed > 0" class="batch-progress-elapsed">
            已耗时 {{ formatDuration(batchTotalElapsed) }}
          </div>
        </div>
      </div>
    </div>

    <!-- 接口列表表格 -->
    <el-table
      :data="metadataList"
      v-loading="loading"
      stripe
      row-key="id"
      ref="tableRef"
      :row-class-name="getRowClassName"
      :row-style="getRowStyle"
    >
      <el-table-column width="80" align="center">
        <template #default>
          <span class="drag-handle"><el-icon><Rank /></el-icon></span>
        </template>
      </el-table-column>
      <el-table-column label="接口名称" min-width="240" :show-overflow-tooltip="false">
        <template #default="{ row }">
          <div v-if="editingNameId === row.id" class="name-edit-row">
            <el-input
              v-model="editingNameValue"
              size="small"
              maxlength="60"
              @keyup.enter="saveInterfaceName(row)"
              @keyup.esc="cancelInterfaceNameEdit"
            />
            <el-button
              text
              size="small"
              type="success"
              class="name-inline-button name-inline-button--save"
              :loading="nameSavingIds.has(row.id)"
              @click="saveInterfaceName(row)"
            >
              保存
            </el-button>
            <el-button
              text
              size="small"
              type="info"
              class="name-inline-button name-inline-button--cancel"
              :disabled="nameSavingIds.has(row.id)"
              @click="cancelInterfaceNameEdit"
            >
              取消
            </el-button>
          </div>
          <div v-else class="name-display-row">
            <span class="name-text">{{ row.cn_name }}</span>
            <el-tag v-if="!isMetadataActive(row)" type="info" size="small">停用</el-tag>
          </div>
        </template>
      </el-table-column>

      <el-table-column prop="record_count" label="本地数据量" width="150" align="center" :show-overflow-tooltip="false">
        <template #default="{ row }">
          <el-tag>{{ row.record_count || 0 }} 条</el-tag>
        </template>
      </el-table-column>

      <el-table-column label="最后更新" width="150" align="center" :show-overflow-tooltip="false">
        <template #default="{ row }">
          <div>{{ formatDate(row.last_update_time) }}</div>
          <div v-if="getItemDuration(row.id)" class="duration-text">{{ getItemDuration(row.id) }}</div>
        </template>
      </el-table-column>

      <el-table-column label="背景色" width="200" align="center" :show-overflow-tooltip="false">
        <template #default="{ row }">
          <el-select
            class="row-color-select"
            :model-value="row.row_bg_color || ''"
            size="small"
            placeholder="默认"
            style="width: 120px"
            :disabled="rowColorSavingIds.has(row.id)"
            @change="(value) => updateRowColor(row, value)"
          >
            <template v-if="row.row_bg_color" #prefix>
              <span class="color-chip" :style="{ backgroundColor: row.row_bg_color }"></span>
            </template>
            <el-option label="默认" value="" />
            <el-option
              v-for="option in rowColorOptions"
              :key="option.value"
              :label="option.label"
              :value="option.value"
            >
              <div class="color-option">
                <span class="color-chip" :style="{ backgroundColor: option.value }"></span>
                <span>{{ option.label }}</span>
              </div>
            </el-option>
          </el-select>
        </template>
      </el-table-column>

      <el-table-column label="操作" width="520" :show-overflow-tooltip="false">
        <template #default="{ row }">
          <el-button plain size="small" type="primary" class="flat-action-button" @click="viewData(row)">
            <el-icon><View /></el-icon>
            查看
          </el-button>
          <el-button
            plain
            size="small"
            :type="editingNameId === row.id ? 'warning' : 'primary'"
            class="flat-action-button"
            :disabled="nameSavingIds.has(row.id)"
            @click="startInterfaceNameEdit(row)"
          >
            <el-icon><Edit /></el-icon>
            {{ editingNameId === row.id ? '编辑中' : '编辑' }}
          </el-button>
          <el-button plain size="small" type="success" class="flat-action-button" @click="triggerUpdate(row)" :loading="updatingIds.has(row.id)" :disabled="!isMetadataActive(row)">
            <el-icon><Refresh /></el-icon>
            更新
          </el-button>
          <el-button
            plain
            size="small"
            :type="isMetadataActive(row) ? 'warning' : 'success'"
            class="flat-action-button"
            :loading="activeSavingIds.has(row.id)"
            @click="setInterfaceActive(row, !isMetadataActive(row))"
          >
            <el-icon><VideoPause /></el-icon>
            {{ isMetadataActive(row) ? '停用' : '恢复' }}
          </el-button>
          <el-button
            v-if="row.update_mode === 'incremental' && row.date_field && row.table_name !== 'hudongyi' && row.table_name !== 'sse_ehudong'"
            link
            type="danger"
            @click="triggerFullUpdate(row)"
            :loading="updatingIds.has(row.id)"
            :disabled="!isMetadataActive(row)"
          >
            <el-icon><Download /></el-icon>
            10年全量
          </el-button>
          <el-button
            v-if="row.update_mode === 'incremental' && row.date_field && (row.table_name.includes('executive') || row.table_name.includes('gaoguan'))"
            link
            type="danger"
            @click="triggerTrueFullUpdate(row)"
            :loading="updatingIds.has(row.id)"
            :disabled="!isMetadataActive(row)"
            style="color: #f56c6c; font-weight: bold;"
          >
            <el-icon><Download /></el-icon>
            真·全量
          </el-button>
          <el-button plain size="small" type="warning" class="flat-action-button" @click="openSource(row.source_url)">
            <el-icon><Link /></el-icon>
            来源
          </el-button>
          <el-button plain size="small" type="info" class="flat-action-button" @click="viewUpdateHistory(row)">
            <el-icon><Clock /></el-icon>
            更新情况
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog v-model="fieldsCheckDialogVisible" title="字段配置检查结果" width="800px">
      <div v-loading="checking">
        <div v-if="fieldsCheckResult" class="fields-check-container">
          <div class="check-summary">
            <el-tag type="success">一致: {{ fieldsCheckResult.summary.ok }}</el-tag>
            <el-tag type="warning" v-if="fieldsCheckResult.summary.mismatch > 0">不一致: {{ fieldsCheckResult.summary.mismatch }}</el-tag>
            <el-tag type="danger" v-if="fieldsCheckResult.summary.error > 0">错误: {{ fieldsCheckResult.summary.error }}</el-tag>
          </div>
          <el-table :data="fieldsCheckResult.details" max-height="500" size="small" stripe>
            <el-table-column prop="cn_name" label="接口名称" width="180" />
            <el-table-column prop="status" label="状态" width="100" align="center">
              <template #default="{ row }">
                <el-tag :type="row.status === 'ok' ? 'success' : row.status === 'mismatch' ? 'warning' : 'danger'" size="small">
                  {{ row.status === 'ok' ? '一致' : row.status === 'mismatch' ? '不一致' : '错误' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="接口字段数" width="120" align="center">
              <template #default="{ row }">
                {{ row.apiFieldCount }}
                <el-tag v-if="row.fieldSource === 'config'" type="info" size="small" style="margin-left: 4px;">配置</el-tag>
                <el-tag v-else type="success" size="small" style="margin-left: 4px;">实际</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="表字段数" width="100" align="center">
              <template #default="{ row }">{{ row.tableFieldCount }}</template>
            </el-table-column>
            <el-table-column label="问题详情" min-width="200">
              <template #default="{ row }">
                <div v-if="row.status === 'ok'" class="text-success">✓ 字段完全一致</div>
                <div v-else-if="row.status === 'error'" class="text-danger">{{ row.message }}</div>
                <div v-else>
                  <div v-if="row.missingInTable.length > 0" class="text-warning">
                    表中缺少: {{ row.missingInTable.join(', ') }}
                  </div>
                  <div v-if="row.extraInTable.length > 0" class="text-info">
                    表中多余: {{ row.extraInTable.join(', ') }}
                  </div>
                </div>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, nextTick } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { metadataApi, updateApi, dataApi } from '../api'
import { onMessage } from '../utils/websocket'
import type { ApiMetadata } from '../types'
import Sortable from 'sortablejs'
import { View, Download, Rank, Refresh, Link, Clock, Delete, Document, Setting, VideoPause, Connection, Edit } from '@element-plus/icons-vue'

const router = useRouter()

let unsubscribe: (() => void) | null = null
let sortableInstance: Sortable | null = null

const clearing = ref(false)
const checking = ref(false)
const fieldsCheckDialogVisible = ref(false)
const fieldsCheckResult = ref<any>(null)
const tableRef = ref<any>(null)
const metadataList = ref<ApiMetadata[]>([])
const showDisabled = ref(false)
const loading = ref(false)
const editingNameId = ref<number | null>(null)
const editingNameValue = ref('')
const nameSavingIds = ref<Set<number>>(new Set())
const rowColorSavingIds = ref<Set<number>>(new Set())
const activeSavingIds = ref<Set<number>>(new Set())
const updatingIds = ref<Set<number>>(new Set())
const batchUpdating = ref(false)
const batchCurrent = ref(0)
const batchTotal = ref(0)
const batchTotalElapsed = ref(0)
const lastBatchDuration = ref(0)  // 上次批量更新总耗时
const batchItemDurations = ref<Map<number, number>>(new Map())
const LAST_TOTAL_DURATION_STORAGE_KEY = 'dashboard-last-total-duration-v1'
const rowColorOptions = [
  { label: '浅粉', value: '#fdf2f2' },
  { label: '浅橙', value: '#fff7e8' },
  { label: '浅黄', value: '#f7f8e8' },
  { label: '浅绿', value: '#f0f9eb' },
  { label: '浅蓝', value: '#ecf5ff' },
  { label: '浅紫', value: '#f4f0ff' }
]

const batchProgressPercentage = computed(() => {
  if (!batchUpdating.value || batchTotal.value <= 0) return 0
  const percentage = Math.round((batchCurrent.value / batchTotal.value) * 100)
  return Math.min(100, Math.max(0, percentage))
})

const activeMetadataCount = computed(() =>
  metadataList.value.filter(item => isMetadataActive(item)).length
)

interface LastTotalDurationCache {
  duration: number
  finishedAt: string
  source: 'batch_update' | 'fields_sync_check_all'
}

function toTimestamp(value: string | null | undefined): number {
  if (!value) return 0
  const time = new Date(value).getTime()
  return Number.isFinite(time) ? time : 0
}

function readLastTotalDurationFromStorage(): LastTotalDurationCache | null {
  if (typeof window === 'undefined') return null

  try {
    const raw = localStorage.getItem(LAST_TOTAL_DURATION_STORAGE_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw) as LastTotalDurationCache
    if (!parsed || typeof parsed.duration !== 'number' || parsed.duration <= 0) {
      return null
    }
    return {
      duration: parsed.duration,
      finishedAt: typeof parsed.finishedAt === 'string' ? parsed.finishedAt : '',
      source: parsed.source === 'batch_update' ? 'batch_update' : 'fields_sync_check_all'
    }
  } catch {
    return null
  }
}

function saveLastTotalDuration(duration: number, source: LastTotalDurationCache['source']): void {
  if (duration <= 0 || typeof window === 'undefined') return

  const payload: LastTotalDurationCache = {
    duration,
    finishedAt: new Date().toISOString(),
    source
  }

  try {
    localStorage.setItem(LAST_TOTAL_DURATION_STORAGE_KEY, JSON.stringify(payload))
  } catch {
    // 忽略本地存储异常
  }
}


async function loadData() {
  loading.value = true
  try {
    const res = await metadataApi.getList(showDisabled.value) as any
    metadataList.value = res.data || []
    // 仅使用批量更新（batch_update）耗时：数据库记录 + 本地同源回退
    const dbDuration = Number(res.lastBatchDuration?.duration || 0)
    const dbFinishedAt = toTimestamp(res.lastBatchDuration?.update_time)

    const localRecord = readLastTotalDurationFromStorage()
    const localBatchRecord = localRecord?.source === 'batch_update' ? localRecord : null
    const localDuration = localBatchRecord?.duration || 0
    const localFinishedAt = toTimestamp(localBatchRecord?.finishedAt)

    const useLocal = localDuration > 0 && localFinishedAt >= dbFinishedAt
    lastBatchDuration.value = useLocal ? localDuration : dbDuration
    nextTick(() => { initSortable() })
  } catch (error) {
    ElMessage.error('加载数据失败')
  } finally {
    loading.value = false
  }
}

function initSortable() {
  if (sortableInstance) { sortableInstance.destroy() }
  const el = tableRef.value?.$el?.querySelector('.el-table__body-wrapper tbody')
  if (!el) return
  sortableInstance = Sortable.create(el, {
    handle: '.drag-handle',
    animation: 150,
    onEnd: async ({ oldIndex, newIndex }) => {
      if (oldIndex === undefined || newIndex === undefined || oldIndex === newIndex) return
      const list = [...metadataList.value]
      const [removed] = list.splice(oldIndex, 1)
      list.splice(newIndex, 0, removed)
      metadataList.value = list
      const orders = list.map((item, index) => ({ id: item.id, sort_order: index + 1 }))
      try {
        await metadataApi.updateSort(orders)
      } catch (error) {
        ElMessage.error('保存排序失败')
        loadData()
      }
    }
  })
}


function formatDate(dateStr: string | null): string {
  if (!dateStr) return '未更新'
  try {
    const date = new Date(dateStr)
    if (Number.isNaN(date.getTime())) {
      return dateStr
    }
    const year = date.getFullYear()
    const month = String(date.getMonth() + 1).padStart(2, '0')
    const day = String(date.getDate()).padStart(2, '0')
    const hours = String(date.getHours()).padStart(2, '0')
    const minutes = String(date.getMinutes()).padStart(2, '0')
    return `${year}-${month}-${day} ${hours}:${minutes}`
  } catch { return dateStr }
}

function formatDuration(ms: number): string {
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

function getItemDuration(id: number): string {
  // 优先使用内存中的实时时长（批量更新时），否则使用数据库中的持久化时长
  const realtimeDuration = batchItemDurations.value.get(id)
  if (realtimeDuration) {
    return formatDuration(realtimeDuration)
  }
  // 从数据库获取的持久化时长
  const item = metadataList.value.find(m => m.id === id)
  if (item?.last_duration) {
    return formatDuration(item.last_duration)
  }
  return ''
}

function isMetadataActive(row: ApiMetadata): boolean {
  return row.is_active !== 0
}

function startInterfaceNameEdit(row: ApiMetadata) {
  editingNameId.value = row.id
  editingNameValue.value = row.cn_name
}

function cancelInterfaceNameEdit() {
  editingNameId.value = null
  editingNameValue.value = ''
}

async function saveInterfaceName(row: ApiMetadata) {
  const nextName = editingNameValue.value.trim()
  if (!nextName) {
    ElMessage.warning('接口名称不能为空')
    return
  }

  if (nextName === row.cn_name) {
    cancelInterfaceNameEdit()
    return
  }

  nameSavingIds.value.add(row.id)

  try {
    await metadataApi.update(row.id, { cn_name: nextName })
    row.cn_name = nextName
    ElMessage.success('接口名称已保存')
    cancelInterfaceNameEdit()
  } catch (error) {
    ElMessage.error('保存接口名称失败')
  } finally {
    nameSavingIds.value.delete(row.id)
  }
}

function getRowClassName({ row }: { row: ApiMetadata }) {
  if (!isMetadataActive(row)) return 'metadata-disabled-row'
  return row.row_bg_color ? 'metadata-colored-row' : ''
}

function getRowStyle({ row }: { row: ApiMetadata }) {
  if (!row.row_bg_color) {
    return {}
  }

  return {
    '--metadata-row-bg': row.row_bg_color
  }
}

async function updateRowColor(row: ApiMetadata, value: string) {
  const normalizedColor = value || null
  const previousColor = row.row_bg_color || null

  if (normalizedColor === previousColor) {
    return
  }

  rowColorSavingIds.value.add(row.id)

  try {
    await metadataApi.update(row.id, { row_bg_color: normalizedColor })
    row.row_bg_color = normalizedColor
    ElMessage.success('行背景色已保存')
  } catch (error) {
    ElMessage.error('保存行背景色失败')
  } finally {
    rowColorSavingIds.value.delete(row.id)
  }
}

async function setInterfaceActive(row: ApiMetadata, isActive: boolean) {
  if (!isActive) {
    try {
      await ElMessageBox.confirm(
        `确定要停用「${row.cn_name}」吗？停用后会从首页默认列表隐藏，并跳过全部更新和字段检查。`,
        '停用接口',
        { type: 'warning', confirmButtonText: '停用', cancelButtonText: '取消' }
      )
    } catch (error) {
      return
    }
  }

  activeSavingIds.value.add(row.id)

  try {
    await metadataApi.update(row.id, { is_active: isActive ? 1 : 0 })
    row.is_active = isActive ? 1 : 0
    ElMessage.success(isActive ? '接口已恢复' : '接口已停用')
    if (!showDisabled.value && !isActive) {
      metadataList.value = metadataList.value.filter(item => item.id !== row.id)
      nextTick(() => { initSortable() })
    }
  } catch (error) {
    ElMessage.error(isActive ? '恢复接口失败' : '停用接口失败')
  } finally {
    activeSavingIds.value.delete(row.id)
  }
}

function viewData(row: ApiMetadata) { router.push(`/data/${row.id}`) }
function viewUpdateHistory(row: ApiMetadata) { router.push(`/update-history/${row.id}`) }
function openSource(url: string) { window.open(url, '_blank') }

async function triggerUpdate(row: ApiMetadata) {
  console.log('triggerUpdate called with row:', row.id, row.cn_name, row.table_name)
  try {
    // 先获取预览信息
    const previewRes = await updateApi.previewUpdate(row.id) as any
    const preview = previewRes.data

    // 构建确认消息
    let message = `接口：${row.cn_name}\n`

    if (preview.updateMode === 'DELETE_RANGE_INSERT') {
      message += `更新模式：删除最近 ${preview.dateRange} 天数据后重新插入\n`
      message += `当前数据库中最近 ${preview.dateRange} 天有 ${preview.currentCount} 条数据将被删除并重新获取\n`
    } else if (preview.updateMode === 'DELETE_INSERT') {
      message += `更新模式：全量删除后重新插入\n`
      message += `当前数据库中有 ${preview.currentCount} 条数据将被删除并重新获取\n`
    } else {
      message += `更新模式：增量更新\n`
      message += `当前数据库中有 ${preview.currentCount} 条数据\n`
    }

    message += `\n确定要执行更新吗？`

    // 显示确认对话框
    await ElMessageBox.confirm(message, '更新确认', {
      confirmButtonText: '确定更新',
      cancelButtonText: '取消',
      type: 'info',
      dangerouslyUseHTMLString: false
    })

    // 用户确认后执行更新
    updatingIds.value.add(row.id)
    await updateApi.triggerUpdate(row.id)
    ElMessage.success('更新任务已启动')
  } catch (error) {
    if ((error as any) !== 'cancel') {
      ElMessage.error('触发更新失败')
    }
    updatingIds.value.delete(row.id)
  }
}

async function triggerFullUpdate(row: ApiMetadata) {
  // 确认对话框
  await ElMessageBox.confirm(
    `确定要进行10年全量更新吗？这将采集最近10年的历史数据（约${row.cn_name.includes('高管') ? '16.5万' : '14.2万'}条），预计需要3-5分钟。`,
    '10年全量更新确认',
    {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning'
    }
  )

  updatingIds.value.add(row.id)
  try {
    await updateApi.triggerFullUpdate(row.id)
    ElMessage.success('10年全量更新任务已启动，请耐心等待')
  } catch (error) {
    ElMessage.error('触发10年全量更新失败')
    updatingIds.value.delete(row.id)
  }
}

async function triggerTrueFullUpdate(row: ApiMetadata) {
  // 确认对话框
  await ElMessageBox.confirm(
    `确定要进行真正全量更新吗？这将采集所有历史数据（不带日期限制，约16.5万条），预计需要10-15分钟。\n\n注意：这是真正的全量更新，会获取数据库中的所有记录！`,
    '真正全量更新确认',
    {
      confirmButtonText: '确定全量更新',
      cancelButtonText: '取消',
      type: 'error',
      dangerouslyUseHTMLString: false
    }
  )

  updatingIds.value.add(row.id)
  try {
    await updateApi.triggerTrueFullUpdate(row.id)
    ElMessage.success('真正全量更新任务已启动，请耐心等待（预计10-15分钟）')
  } catch (error) {
    ElMessage.error('触发真正全量更新失败')
    updatingIds.value.delete(row.id)
  }
}

async function triggerUpdateAll() {
  try {
    const total = activeMetadataCount.value
    if (total <= 0) {
      ElMessage.info('没有可更新的启用接口')
      return
    }
    await ElMessageBox.confirm(
      `确定要更新全部 ${total} 个启用接口吗？停用接口会跳过。`,
      '全部更新',
      { type: 'info', confirmButtonText: '开始更新', cancelButtonText: '取消' }
    )
    batchUpdating.value = true
    batchCurrent.value = 0
    batchTotal.value = total
    const res = await updateApi.triggerUpdateAll() as any
    ElMessage.success(res.message || '批量更新任务已启动')
  } catch (error) {
    if ((error as any) !== 'cancel') {
      ElMessage.error('触发批量更新失败')
      batchUpdating.value = false
    }
  }
}

async function stopAllUpdates() {
  try {
    await ElMessageBox.confirm('确定要停止当前所有采集任务吗？', '停止采集', {
      type: 'warning', confirmButtonText: '确定停止', cancelButtonText: '取消'
    })
    const res = await updateApi.stopAll() as any
    ElMessage.success(res.message || '已发送停止请求')
  } catch (error) {
    if ((error as any) !== 'cancel') {
      ElMessage.error('停止采集失败')
    }
  }
}

async function clearAllData() {
  try {
    await ElMessageBox.confirm('确定要清空所有接口的业务数据和更新日志吗？此操作不可恢复！', '危险操作', {
      type: 'warning', confirmButtonText: '确定清空', cancelButtonText: '取消', confirmButtonClass: 'el-button--danger'
    })
    clearing.value = true
    const res = await dataApi.clearAll() as any
    ElMessage.success(res.message || '清空成功')
    loadData()
  } catch (error) {
    if ((error as any) !== 'cancel') { ElMessage.error('清空失败') }
  } finally { clearing.value = false }
}

async function checkFields() {
  checking.value = true
  fieldsCheckDialogVisible.value = true
  fieldsCheckResult.value = null
  try {
    const res = await dataApi.checkFields() as any
    if (res.success) { fieldsCheckResult.value = res.data }
    else { ElMessage.error('检查失败') }
  } catch (error) { ElMessage.error('检查字段配置失败') }
  finally { checking.value = false }
}

function goToFieldsSync() {
  router.push('/fields-sync')
}

function goToDataCombos() {
  router.push('/data-combos')
}

onMounted(() => {
  loadData()
  unsubscribe = onMessage((message) => {
    // 处理批量更新消息
    if (message.type === 'batch_start') {
      batchUpdating.value = true
      batchTotal.value = message.total || 0
      batchCurrent.value = 0
      batchTotalElapsed.value = 0
      batchItemDurations.value.clear()
    } else if (message.type === 'batch_progress') {
      batchCurrent.value = message.current || 0
      batchTotal.value = message.total || 0
      // 标记当前正在更新的接口
      if (message.metadataId) {
        updatingIds.value.add(message.metadataId)
      }
    } else if ((message as any).type === 'batch_item_complete' || (message as any).type === 'batch_item_error') {
      // 单个接口完成/失败，记录耗时
      const msg = message as any
      if (msg.metadataId && msg.duration) {
        batchItemDurations.value.set(msg.metadataId, msg.duration)
      }
      if (msg.totalElapsed) {
        batchTotalElapsed.value = msg.totalElapsed
      }
      updatingIds.value.delete(msg.metadataId)
      // 批量更新时静默刷新数据
      loadData()
    } else if (message.type === 'batch_complete') {
      const msg = message as any
      batchUpdating.value = false
      batchCurrent.value = 0
      batchTotal.value = 0
      if (msg.duration) {
        batchTotalElapsed.value = msg.duration
        lastBatchDuration.value = msg.duration  // 保存上次批量更新总耗时
        saveLastTotalDuration(msg.duration, 'batch_update')
      }
      loadData()
      ElMessage.success(message.message || '批量更新完成')
    } else if (message.type === 'complete' || message.type === 'error') {
      updatingIds.value.delete(message.metadataId)
      // 只有非批量更新时才刷新和提示
      if (!batchUpdating.value) {
        loadData()
        if (message.type === 'complete') { ElMessage.success(message.message || '更新完成') }
        else { ElMessage.error(message.message || '更新失败') }
      } else {
        // 批量更新时静默刷新数据
        loadData()
      }
    }
  })
})

onUnmounted(() => {
  if (unsubscribe) { unsubscribe() }
  if (sortableInstance) { sortableInstance.destroy() }
})
</script>

<style scoped>
.dashboard { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 12px rgba(0, 0, 0, 0.1); }
.header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
.header h2 { margin: 0; color: #303133; }
.header-actions { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.show-disabled-switch { margin: 0 4px; }
.batch-progress-panel {
  width: 220px;
  padding: 8px 10px;
  border-radius: 8px;
  border: 1px solid #d9ecff;
  background: #f5f9ff;
}
.batch-progress-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 6px;
}
.batch-progress-title {
  color: #606266;
  font-size: 12px;
}
.batch-progress-ratio {
  color: #303133;
  font-size: 12px;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
}
.batch-progress-elapsed {
  margin-top: 6px;
  color: #67c23a;
  font-size: 12px;
  font-variant-numeric: tabular-nums;
}
.fields-check-container { padding: 0; }
.check-summary { margin-bottom: 15px; display: flex; gap: 10px; }
.text-success { color: #67c23a; }
.text-warning { color: #e6a23c; font-size: 12px; }
.text-danger { color: #f56c6c; }
.text-info { color: #909399; font-size: 12px; }
.drag-handle { display: inline-flex; align-items: center; justify-content: center; cursor: move; color: #909399; font-size: 16px; user-select: none; position: relative; top: 2px; }
.drag-handle:hover { color: #409eff; }
.duration-text { color: #67c23a; font-size: 12px; margin-top: 2px; }
:deep(.el-button) { display: inline-flex; align-items: center; }
:deep(.el-table) { caret-color: transparent; }
:deep(.el-table *) { caret-color: transparent; }
:deep(.el-table .metadata-colored-row td.el-table__cell) {
  background-color: var(--metadata-row-bg) !important;
}
:deep(.el-table .metadata-colored-row:hover td.el-table__cell) {
  background-color: var(--metadata-row-bg) !important;
}
:deep(.el-table .metadata-disabled-row td.el-table__cell) {
  background-color: #f5f7fa !important;
  color: #909399;
}
:deep(.el-table .metadata-disabled-row:hover td.el-table__cell) {
  background-color: #eef1f5 !important;
}

.name-display-row,
.name-edit-row {
  display: flex;
  align-items: center;
  gap: 8px;
}

.name-display-row {
  min-height: 28px;
}

.name-text {
  min-width: 0;
  flex: 1;
  line-height: 1.5;
}

.name-edit-row :deep(.el-input) {
  flex: 1;
}

.name-edit-row :deep(.el-input__inner) {
  caret-color: auto;
}

.color-option {
  display: inline-flex;
  align-items: center;
  gap: 8px;
}

.row-color-select :deep(.el-select__selected-item) {
  text-overflow: clip;
}

.row-color-select :deep(.el-select__selection) {
  gap: 6px;
}

.color-chip {
  width: 10px;
  height: 10px;
  border-radius: 3px;
  border: 1px solid rgba(0, 0, 0, 0.08);
  flex: 0 0 auto;
}

.flat-action-button {
  box-shadow: none !important;
  border-width: 1px;
  border-style: solid;
  background-image: none !important;
}

.name-inline-button {
  margin: 0;
  padding: 0 10px;
  min-height: 26px;
  border-radius: 999px;
  border: 1px solid transparent;
  background: transparent;
  box-shadow: none !important;
  font-size: 12px;
  font-weight: 500;
  flex: 0 0 auto;
}

.name-inline-button--save {
  color: #2f9b62;
  background: #eff9f2;
  border-color: #cfead9;
}

.name-inline-button--save:hover,
.name-inline-button--save:focus-visible {
  color: #257d4f;
  background: #e4f6eb;
  border-color: #b8dec7;
}

.name-inline-button--cancel {
  color: #7a818d;
  background: #f6f7f9;
  border-color: #e2e5ea;
}

.name-inline-button--cancel:hover,
.name-inline-button--cancel:focus-visible {
  color: #5d6470;
  background: #eef1f4;
  border-color: #d6dae0;
}


/* 上次批量更新耗时 */
.last-batch-duration {
  margin-left: 8px;
  padding: 6px 12px;
  display: inline-flex;
  align-items: center;
  gap: 8px;
  border-radius: 999px;
  border: 1px solid #d9ecff;
  background: linear-gradient(135deg, #ecf5ff 0%, #f0f9eb 100%);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.85);
  color: #606266;
  font-size: 12px;
  line-height: 1;
  white-space: nowrap;
}

.duration-label {
  color: #909399;
}

.duration-value {
  color: #67c23a;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
}
</style>
