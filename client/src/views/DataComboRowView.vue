<template>
  <div class="data-combo-row-view">
    <div class="page-header">
      <div class="page-intro">
        <div class="title-row">
          <el-button @click="closeWindow" :icon="CloseBold">关闭窗口</el-button>
          <div class="stock-summary">
            <span class="stock-name">{{ stockName || '未知标的' }}</span>
            <el-tag type="success">{{ rowCode }}</el-tag>
          </div>
        </div>
        <p class="page-desc">
          上面保留详情卡片用于快速阅读，下面保留表格样式用于横向比对。主表和每个子表都支持单独设置卡片区域的“几乘几”布局。
        </p>
      </div>
      <div class="page-actions">
        <el-button @click="copyForAiAnalysis">复制 AI 分析文本</el-button>
      </div>
    </div>

    <div v-loading="loading" class="content-wrap">
      <div class="main-section">
        <el-card shadow="never" class="detail-card">
          <template #header>
            <div class="section-header">
              <div class="section-header-main">
                <span>主表数据</span>
                <el-tag type="info">{{ combo?.mainTableLabel || '主表' }}</el-tag>
                <el-button size="small" text @click="openLayoutEditor('main')">
                  编辑布局 {{ getLayoutSummary(mainLayoutConfig) }}
                </el-button>
              </div>
              <span v-if="combo" class="header-helper">
                共 {{ combo.mainColumns.length }} 个字段，可直接拖动排序
              </span>
            </div>
          </template>

          <div
            v-if="rowData && combo"
            ref="mainFieldGridRef"
            class="main-field-grid main-field-grid-draggable"
            :style="getGridStyle(mainLayoutConfig)"
          >
            <div
              v-for="slot in visibleMainSlots"
              :key="slot.index"
              class="main-field-slot"
              :class="{
                'is-drag-over': dragOverMainSlotIndex === slot.index,
                'is-empty': !slot.column
              }"
              @dragover.prevent="handleMainSlotDragOver(slot.index)"
              @dragenter.prevent="handleMainSlotDragOver(slot.index)"
              @dragleave="handleMainSlotDragLeave(slot.index)"
              @drop.prevent="handleMainSlotDrop(slot.index)"
            >
              <div
                v-if="slot.column"
                class="field-card main-field-card"
                :data-field="slot.column.field"
                draggable="true"
                @dragstart="handleMainFieldDragStart(slot.index, $event)"
                @dragend="handleMainFieldDragEnd"
              >
                <div class="field-card-label">{{ slot.column.label }}</div>
                <div class="field-card-value-wrap">
                  <span
                    :class="[
                      'field-card-value',
                      getCellClass(slot.column, rowData[slot.column.field]),
                      {
                        'cell-content-clamp': shouldClampCell(slot.column, rowData[slot.column.field]),
                        'cell-content-expandable': shouldClampCell(slot.column, rowData[slot.column.field])
                      }
                    ]"
                    :title="shouldClampCell(slot.column, rowData[slot.column.field]) ? String(rowData[slot.column.field] ?? '') : ''"
                    @click="handleCellClick(slot.column, rowData[slot.column.field])"
                  >
                    {{ formatCell(slot.column, rowData[slot.column.field]) }}
                  </span>
                </div>
              </div>

              <div v-else class="main-field-slot-empty">
                拖到这里
              </div>
            </div>
          </div>

          <div v-if="hiddenMainColumnCount > 0" class="layout-hint">
            当前按 {{ getLayoutSummary(mainLayoutConfig) }} 展示，剩余 {{ hiddenMainColumnCount }} 个字段可在下方表格查看
          </div>

          <div v-if="rowData && combo" class="table-preview-block">
            <div class="table-preview-title">表格样式</div>
            <el-table :data="[rowData]" border stripe class="main-table">
              <el-table-column
                v-for="column in orderedMainColumns"
                :key="column.field"
                :prop="column.field"
                :min-width="getColumnWidth(column.label)"
                align="center"
                header-align="center"
                show-overflow-tooltip
              >
                <template #header>
                  <div class="table-header">
                    <div class="table-header-center-group">
                      <span class="table-header-label">{{ column.label }}</span>
                    </div>
                  </div>
                </template>
                <template #default="{ row }">
                  <span
                    :class="[
                      'combo-cell-value',
                      getCellClass(column, row[column.field]),
                      {
                        'cell-content-single-ellipsis': true,
                        'cell-content-expandable': shouldClampCell(column, row[column.field])
                      }
                    ]"
                    :title="String(row[column.field] ?? '')"
                    @click="handleCellClick(column, row[column.field])"
                  >
                    {{ formatCell(column, row[column.field]) }}
                  </span>
                </template>
              </el-table-column>
            </el-table>
          </div>
        </el-card>
      </div>

      <div
        v-for="child in combo?.childCollections || []"
        :key="child.key"
        class="child-section"
      >
        <el-card shadow="never" class="detail-card">
          <template #header>
            <div class="section-header">
              <div class="section-header-main">
                <span>{{ child.label }}</span>
                <el-tag v-if="getPositiveCount(rowData?.[child.countField]) !== null" type="warning">
                  {{ getPositiveCount(rowData?.[child.countField]) }} 条
                </el-tag>
                <el-tag v-else type="info" effect="plain">0 条</el-tag>
                <el-button size="small" text @click="openLayoutEditor('child', child.key)">
                  编辑布局 {{ getLayoutSummary(getChildLayoutConfig(child.key)) }}
                </el-button>
              </div>
              <div class="section-header-actions">
                <span class="header-helper">上方卡片用于阅读，下方表格用于横向比对</span>
              </div>
            </div>
          </template>

          <el-empty
            v-if="getChildRecords(child).length === 0"
            class="child-empty"
            :description="`该行暂无${child.label}记录`"
          />

          <div v-else class="child-record-list">
            <div
              v-for="(record, index) in getChildRecords(child)"
              :key="getChildRecordKey(child, record, index)"
              class="child-record-card"
            >
              <div class="record-card-top">
                <div class="record-title-group">
                  <div class="record-title">{{ getChildRecordTitle(child, record, index) }}</div>
                  <div v-if="getChildRecordSubtitle(child, record)" class="record-subtitle">
                    {{ getChildRecordSubtitle(child, record) }}
                  </div>
                </div>

                <el-button
                  v-if="shouldShowChildExpandToggle(child)"
                  type="primary"
                  link
                  @click="toggleChildRecordExpanded(child.key, index)"
                >
                  {{ isChildRecordExpanded(child.key, index) ? '收起字段' : `查看全部 ${child.columns.length} 个字段` }}
                </el-button>
              </div>

              <div
                class="child-field-grid"
                :style="getGridStyle(getChildLayoutConfig(child.key))"
              >
                <div
                  v-for="column in getVisibleChildColumns(child, index)"
                  :key="column.field"
                  class="field-card field-card-compact"
                  :class="{ 'field-card-wide': shouldRenderWideField(column, record[column.field]) }"
                >
                  <div class="field-card-label">{{ column.label }}</div>
                  <div class="field-card-value-wrap">
                    <span
                      :class="[
                        'field-card-value',
                        getCellClass(column, record[column.field]),
                        {
                          'cell-content-clamp': shouldClampCell(column, record[column.field]),
                          'cell-content-expandable': shouldClampCell(column, record[column.field])
                        }
                      ]"
                      :title="shouldClampCell(column, record[column.field]) ? String(record[column.field] ?? '') : ''"
                      @click="handleCellClick(column, record[column.field])"
                    >
                      {{ formatCell(column, record[column.field]) }}
                    </span>
                  </div>
                </div>
              </div>

              <div
                v-if="getHiddenChildColumnCount(child, index) > 0 && !isChildRecordExpanded(child.key, index)"
                class="record-more-hint"
              >
                当前按 {{ getLayoutSummary(getChildLayoutConfig(child.key)) }} 展示，另有
                {{ getHiddenChildColumnCount(child, index) }} 个字段已收起
              </div>
            </div>

            <div class="table-preview-block">
              <div class="table-preview-title">表格样式</div>
              <el-table
                :data="getChildRecords(child)"
                border
                stripe
                max-height="420px"
                class="child-table"
              >
                <el-table-column
                  v-for="column in child.columns"
                  :key="column.field"
                  :prop="column.field"
                  :min-width="getColumnWidth(column.label)"
                  align="center"
                  header-align="center"
                  show-overflow-tooltip
                >
                  <template #header>
                    <div class="table-header">
                      <div class="table-header-center-group">
                        <span class="table-header-label">{{ column.label }}</span>
                      </div>
                    </div>
                  </template>
                  <template #default="{ row }">
                    <span
                      :class="[
                        'combo-cell-value',
                        getCellClass(column, row[column.field]),
                        {
                          'cell-content-single-ellipsis': true,
                          'cell-content-expandable': shouldClampCell(column, row[column.field])
                        }
                      ]"
                      :title="String(row[column.field] ?? '')"
                      @click="handleCellClick(column, row[column.field])"
                    >
                      {{ formatCell(column, row[column.field]) }}
                    </span>
                  </template>
                </el-table-column>
              </el-table>
            </div>
          </div>
        </el-card>
      </div>
    </div>

    <el-dialog v-model="fullTextDialogVisible" :title="fullTextDialogTitle" width="760px">
      <div class="full-text-dialog-content">{{ fullTextDialogContent }}</div>
    </el-dialog>

    <el-dialog v-model="layoutEditorVisible" title="编辑布局" width="420px">
      <div class="layout-editor-body">
        <div class="layout-editor-grid">
          <div class="layout-editor-field">
            <div class="layout-editor-label">每排几个</div>
            <el-input-number
              v-model="layoutEditorCols"
              :min="1"
              :max="12"
              :step="1"
              controls-position="right"
            />
          </div>
          <div class="layout-editor-field">
            <div class="layout-editor-label">一共几排</div>
            <el-input-number
              v-model="layoutEditorRows"
              :min="1"
              :max="12"
              :step="1"
              controls-position="right"
            />
          </div>
        </div>
        <div class="layout-editor-helper">当前目标：{{ layoutEditorTitle }}</div>
      </div>
      <template #footer>
        <el-button @click="layoutEditorVisible = false">取消</el-button>
        <el-button type="primary" @click="saveLayoutEditor">确定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { CloseBold } from '@element-plus/icons-vue'
import { useRoute } from 'vue-router'
import { dataComboApi } from '../api'
import type { DataComboChildCollection, DataComboColumn, DataComboDetail } from '../types'
import { subscribeFieldDisplayConfigChange } from '../utils/fieldDisplaySync'
import { formatDisplayValue, getDisplayValueClass, shouldClampDisplayCell } from '../utils/fieldPresentation'

type LayoutConfig = {
  cols: number
  rows: number
}

const DEFAULT_CHILD_PREVIEW_COUNT = 6
const MAIN_FIELD_SLOT_STORAGE_KEY = 'data-combo-row-main-field-slots'
const LAYOUT_STORAGE_KEY = 'data-combo-row-layout'

const route = useRoute()

const combo = ref<DataComboDetail | null>(null)
const rowData = ref<Record<string, any> | null>(null)
const loading = ref(false)
const fullTextDialogVisible = ref(false)
const fullTextDialogTitle = ref('')
const fullTextDialogContent = ref('')
const expandedChildRecords = ref<Record<string, boolean>>({})
const mainFieldSlots = ref<Array<string | null>>([])
const mainFieldGridRef = ref<HTMLElement | null>(null)
const mainLayoutConfig = ref<LayoutConfig>({ cols: 6, rows: 1 })
const childLayoutConfigs = ref<Record<string, LayoutConfig>>({})
const layoutEditorVisible = ref(false)
const layoutEditorTitle = ref('')
const layoutEditorCols = ref(1)
const layoutEditorRows = ref(1)
const layoutEditorTarget = ref<{ type: 'main' | 'child'; key: string }>({ type: 'main', key: '' })
const draggingMainSlotIndex = ref<number | null>(null)
const dragOverMainSlotIndex = ref<number | null>(null)
let unsubscribeFieldDisplaySync: (() => void) | null = null

const comboId = computed(() => String(route.params.id || ''))
const rowCode = computed(() => String(route.params.code || ''))

const comboTableNames = computed(() => {
  if (!combo.value) {
    return new Set<string>()
  }

  return new Set([
    combo.value.mainTableName,
    ...combo.value.childCollections.map((child) => child.tableName)
  ])
})

const stockName = computed(() => {
  if (!combo.value || !rowData.value) {
    return ''
  }

  const nameColumn = combo.value.mainColumns.find((column) => /name|abbr|title|f14/i.test(column.field))
  if (!nameColumn) {
    return ''
  }

  const value = rowData.value[nameColumn.field]
  return value === null || value === undefined ? '' : String(value).trim()
})

const orderedMainColumns = computed(() => {
  if (!combo.value) {
    return []
  }

  const columnMap = new Map(combo.value.mainColumns.map((column) => [column.field, column]))
  const ordered = mainFieldSlots.value
    .map((field) => columnMap.get(field))
    .filter((column): column is DataComboColumn => !!column)
  const rest = combo.value.mainColumns.filter((column) => !mainFieldSlots.value.includes(column.field))

  return [...ordered, ...rest]
})

const visibleMainSlots = computed(() => {
  const totalSlots = getVisibleCount(mainLayoutConfig.value)
  const columnMap = new Map(combo.value?.mainColumns.map((column) => [column.field, column]) || [])

  return Array.from({ length: totalSlots }, (_, index) => {
    const field = mainFieldSlots.value[index]
    return {
      index,
      field,
      column: field ? columnMap.get(field) || null : null
    }
  })
})

const hiddenMainColumnCount = computed(() => {
  const visibleFieldCount = mainFieldSlots.value.filter(Boolean).length
  return Math.max((combo.value?.mainColumns.length || 0) - visibleFieldCount, 0)
})

function closeWindow() {
  window.close()
}

function clampLayoutValue(value: number, fallback: number): number {
  const nextValue = Number.isFinite(value) ? Math.floor(value) : fallback
  return Math.min(Math.max(nextValue, 1), 12)
}

function normalizeLayoutConfig(config: Partial<LayoutConfig>, fallback: LayoutConfig): LayoutConfig {
  return {
    cols: clampLayoutValue(Number(config.cols), fallback.cols),
    rows: clampLayoutValue(Number(config.rows), fallback.rows)
  }
}

function getMainFieldSlotStorageKey() {
  return `${MAIN_FIELD_SLOT_STORAGE_KEY}:${comboId.value}`
}

function getLayoutStorageKey(sectionKey: string) {
  return `${LAYOUT_STORAGE_KEY}:${comboId.value}:${sectionKey}`
}

function getVisibleCount(config: LayoutConfig): number {
  return Math.max(config.cols * config.rows, 1)
}

function getMainDefaultLayout(): LayoutConfig {
  const total = combo.value?.mainColumns.length || 0
  return {
    cols: 6,
    rows: Math.max(1, Math.ceil(total / 6))
  }
}

function getChildDefaultLayout(childKey: string): LayoutConfig {
  const child = combo.value?.childCollections.find((item) => item.key === childKey)
  const total = child?.columns.length || DEFAULT_CHILD_PREVIEW_COUNT
  const cols = 3
  const rows = Math.max(1, Math.ceil(Math.min(total, DEFAULT_CHILD_PREVIEW_COUNT) / cols))

  return { cols, rows }
}

function getLayoutSummary(config: LayoutConfig): string {
  return `${config.cols}x${config.rows}`
}

function getGridStyle(config: LayoutConfig) {
  return {
    '--grid-columns': String(config.cols)
  }
}

function saveMainFieldSlots(slots: Array<string | null>) {
  try {
    window.localStorage.setItem(getMainFieldSlotStorageKey(), JSON.stringify(slots))
  } catch {
    // Ignore local persistence failures.
  }
}

function syncMainFieldSlots(columns: DataComboColumn[]) {
  const visibleCount = getVisibleCount(mainLayoutConfig.value)
  const validFields = new Set(columns.map((column) => column.field))
  const normalized = mainFieldSlots.value
    .map((field) => (field && validFields.has(field) ? field : null))
    .slice(0, visibleCount)

  const usedFields = new Set(normalized.filter((field): field is string => !!field))
  const missingFields = columns
    .map((column) => column.field)
    .filter((field) => !usedFields.has(field))

  if (normalized.length === 0) {
    mainFieldSlots.value = Array.from({ length: visibleCount }, (_, index) => missingFields[index] || null)
    return
  }

  while (normalized.length < visibleCount) {
    normalized.push(missingFields.shift() || null)
  }

  mainFieldSlots.value = normalized
}

function restoreMainFieldSlots(columns: DataComboColumn[]) {
  try {
    const raw = window.localStorage.getItem(getMainFieldSlotStorageKey())
    const parsed = raw ? JSON.parse(raw) : []
    mainFieldSlots.value = Array.isArray(parsed)
      ? parsed.map((field) => (typeof field === 'string' ? field : null))
      : []
  } catch {
    mainFieldSlots.value = []
  }

  syncMainFieldSlots(columns)
}

function saveLayoutConfig(sectionKey: string, config: LayoutConfig) {
  try {
    window.localStorage.setItem(getLayoutStorageKey(sectionKey), JSON.stringify(config))
  } catch {
    // Ignore local persistence failures.
  }
}

function restoreLayoutConfig(sectionKey: string, fallback: LayoutConfig): LayoutConfig {
  try {
    const raw = window.localStorage.getItem(getLayoutStorageKey(sectionKey))
    const parsed = raw ? JSON.parse(raw) : {}
    return normalizeLayoutConfig(parsed, fallback)
  } catch {
    return fallback
  }
}

function getMainLayoutConfig(): LayoutConfig {
  return mainLayoutConfig.value
}

function getChildLayoutConfig(childKey: string): LayoutConfig {
  if (!childLayoutConfigs.value[childKey]) {
    childLayoutConfigs.value = {
      ...childLayoutConfigs.value,
      [childKey]: getChildDefaultLayout(childKey)
    }
  }

  return childLayoutConfigs.value[childKey]
}

function restoreAllLayouts() {
  mainLayoutConfig.value = restoreLayoutConfig('main', getMainDefaultLayout())

  const nextChildLayouts: Record<string, LayoutConfig> = {}
  for (const child of combo.value?.childCollections || []) {
    nextChildLayouts[child.key] = restoreLayoutConfig(child.key, getChildDefaultLayout(child.key))
  }
  childLayoutConfigs.value = nextChildLayouts
}

function openLayoutEditor(type: 'main' | 'child', key: string = '') {
  layoutEditorTarget.value = { type, key }

  if (type === 'main') {
    layoutEditorTitle.value = '主表数据'
    layoutEditorCols.value = getMainLayoutConfig().cols
    layoutEditorRows.value = getMainLayoutConfig().rows
  } else {
    const child = combo.value?.childCollections.find((item) => item.key === key)
    layoutEditorTitle.value = child?.label || '子表数据'
    layoutEditorCols.value = getChildLayoutConfig(key).cols
    layoutEditorRows.value = getChildLayoutConfig(key).rows
  }

  layoutEditorVisible.value = true
}

function saveLayoutEditor() {
  const nextConfig = normalizeLayoutConfig(
    {
      cols: layoutEditorCols.value,
      rows: layoutEditorRows.value
    },
    { cols: 1, rows: 1 }
  )

  if (layoutEditorTarget.value.type === 'main') {
    mainLayoutConfig.value = nextConfig
    if (combo.value) {
      syncMainFieldSlots(combo.value.mainColumns)
      saveMainFieldSlots(mainFieldSlots.value)
    }
    saveLayoutConfig('main', nextConfig)
  } else {
    childLayoutConfigs.value = {
      ...childLayoutConfigs.value,
      [layoutEditorTarget.value.key]: nextConfig
    }
    saveLayoutConfig(layoutEditorTarget.value.key, nextConfig)
  }

  layoutEditorVisible.value = false
}

function handleMainFieldDragStart(index: number, event: DragEvent) {
  draggingMainSlotIndex.value = index
  dragOverMainSlotIndex.value = index

  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = 'move'
    event.dataTransfer.setData('text/plain', String(index))
  }
}

function handleMainFieldDragEnd() {
  draggingMainSlotIndex.value = null
  dragOverMainSlotIndex.value = null
}

function handleMainSlotDragOver(index: number) {
  if (draggingMainSlotIndex.value === null) {
    return
  }

  dragOverMainSlotIndex.value = index
}

function handleMainSlotDragLeave(index: number) {
  if (dragOverMainSlotIndex.value === index) {
    dragOverMainSlotIndex.value = null
  }
}

function handleMainSlotDrop(index: number) {
  const sourceIndex = draggingMainSlotIndex.value
  dragOverMainSlotIndex.value = null

  if (sourceIndex === null || sourceIndex === index) {
    return
  }

  const nextSlots = [...mainFieldSlots.value]
  const sourceField = nextSlots[sourceIndex] || null
  const targetField = nextSlots[index] || null

  if (!sourceField) {
    return
  }

  nextSlots[index] = sourceField
  nextSlots[sourceIndex] = targetField
  mainFieldSlots.value = nextSlots
  saveMainFieldSlots(nextSlots)
}

function getPositiveCount(value: unknown): number | null {
  const count = typeof value === 'number' ? value : Number(value)
  if (!Number.isFinite(count) || count <= 0) {
    return null
  }

  return count
}

function getColumnWidth(label: string): number {
  const normalized = String(label || '')
  let width = 56

  for (const char of normalized) {
    width += /[A-Za-z0-9_]/.test(char) ? 10 : 16
  }

  return Math.max(width, 120)
}

function formatCell(column: DataComboColumn, value: unknown): string {
  return formatDisplayValue(value, column.type, column.field, column.display)
}

function getCellClass(column: DataComboColumn, value: unknown): string {
  return getDisplayValueClass(value, column.type)
}

function shouldClampCell(column: DataComboColumn, value: unknown): boolean {
  return shouldClampDisplayCell(value, column.display)
}

function shouldRenderWideField(column: DataComboColumn, value: unknown): boolean {
  const rawText = value === null || value === undefined ? '' : String(value)
  const displayText = formatCell(column, value)
  const hintText = `${column.label} ${column.field}`.toLowerCase()

  return (
    shouldClampCell(column, value) ||
    rawText.includes('\n') ||
    displayText.length > 36 ||
    /name|title|desc|text|remark|memo|reason|explain|content/.test(hintText) ||
    /名称|标题|说明|描述|内容|原因|备注|概念|范围|摘要/.test(column.label)
  )
}

function handleCellClick(column: DataComboColumn, value: unknown) {
  if (!shouldClampCell(column, value)) {
    return
  }

  fullTextDialogTitle.value = `${column.label} (${column.field})`
  fullTextDialogContent.value = value === null || value === undefined ? '' : String(value)
  fullTextDialogVisible.value = true
}

function getRawValueText(value: unknown): string {
  if (value === null || value === undefined || value === '') {
    return '-'
  }

  if (typeof value === 'string') {
    return value
  }

  return String(value)
}

function buildFieldLine(column: DataComboColumn, value: unknown): string {
  const displayValue = formatCell(column, value)
  const rawValue = getRawValueText(value)
  const unit = column.display.unit || ''
  const unitSuffix = unit ? `，单位: ${unit}` : ''
  return `- ${column.label}: ${displayValue} (raw: ${rawValue}${unitSuffix})`
}

function buildAiAnalysisText(): string {
  if (!combo.value || !rowData.value) {
    return ''
  }

  const sections: string[] = []
  sections.push('# 标的概览')
  sections.push(`- 股票名称: ${stockName.value || '-'}`)
  sections.push(`- 股票代码: ${rowCode.value || '-'}`)
  sections.push(`- 关联视图: ${combo.value.name}`)
  sections.push(`- 主表: ${combo.value.mainTableLabel}`)
  sections.push('')
  sections.push('# 主表数据')

  for (const column of combo.value.mainColumns) {
    sections.push(buildFieldLine(column, rowData.value[column.field]))
  }

  for (const child of combo.value.childCollections) {
    const records = getChildRecords(child)
    sections.push('')
    sections.push(`# 子表: ${child.label} (${records.length}条)`)

    if (records.length === 0) {
      sections.push('- 无记录')
      continue
    }

    records.forEach((record, index) => {
      sections.push(`## 第 ${index + 1} 条`)
      for (const column of child.columns) {
        sections.push(buildFieldLine(column, record[column.field]))
      }
    })
  }

  return sections.join('\n')
}

async function copyText(text: string) {
  if (!text) {
    throw new Error('没有可复制的内容')
  }

  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text)
    return
  }

  const textarea = document.createElement('textarea')
  textarea.value = text
  textarea.setAttribute('readonly', 'true')
  textarea.style.position = 'fixed'
  textarea.style.left = '-9999px'
  document.body.appendChild(textarea)
  textarea.select()
  document.execCommand('copy')
  document.body.removeChild(textarea)
}

async function copyForAiAnalysis() {
  try {
    await copyText(buildAiAnalysisText())
    ElMessage.success('已复制 AI 分析文本')
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '复制失败')
  }
}

function getChildRecords(child: DataComboChildCollection): Record<string, any>[] {
  const records = rowData.value?.[child.dataField]
  return Array.isArray(records) ? records : []
}

function getExpandedRecordKey(childKey: string, index: number): string {
  return `${childKey}:${index}`
}

function isChildRecordExpanded(childKey: string, index: number): boolean {
  return !!expandedChildRecords.value[getExpandedRecordKey(childKey, index)]
}

function toggleChildRecordExpanded(childKey: string, index: number) {
  const targetKey = getExpandedRecordKey(childKey, index)
  expandedChildRecords.value = {
    ...expandedChildRecords.value,
    [targetKey]: !expandedChildRecords.value[targetKey]
  }
}

function shouldShowChildExpandToggle(child: DataComboChildCollection): boolean {
  return child.columns.length > getVisibleCount(getChildLayoutConfig(child.key))
}

function getVisibleChildColumns(child: DataComboChildCollection, index: number): DataComboColumn[] {
  if (isChildRecordExpanded(child.key, index)) {
    return child.columns
  }

  return child.columns.slice(0, getVisibleCount(getChildLayoutConfig(child.key)))
}

function getHiddenChildColumnCount(child: DataComboChildCollection, index: number): number {
  return Math.max(child.columns.length - getVisibleChildColumns(child, index).length, 0)
}

function getPriorityColumns(child: DataComboChildCollection): DataComboColumn[] {
  const scored = child.columns.map((column, index) => {
    const target = `${column.field} ${column.label}`.toLowerCase()
    let score = 4

    if (/code|scode|symbol|secname|name|title/.test(target) || /代码|名称|标题/.test(column.label)) {
      score = 0
    } else if (/date|time/.test(target) || /日期|时间/.test(column.label)) {
      score = 1
    } else if (
      /price|amount|count|ratio|balance|value|profit/.test(target) ||
      /价|额|量|率|余额|市值|利润|收入/.test(column.label)
    ) {
      score = 2
    } else if (shouldClampCell(column, 'sample')) {
      score = 3
    }

    return { column, index, score }
  })

  return scored
    .sort((left, right) => {
      if (left.score !== right.score) {
        return left.score - right.score
      }

      return left.index - right.index
    })
    .map((item) => item.column)
}

function getChildRecordTitle(
  child: DataComboChildCollection,
  record: Record<string, any>,
  index: number
): string {
  const primaryColumn = getPriorityColumns(child)[0] || child.columns[0]
  const displayValue = primaryColumn ? formatCell(primaryColumn, record[primaryColumn.field]) : ''

  if (displayValue && displayValue !== '-') {
    return `${child.label} #${index + 1} · ${displayValue}`
  }

  return `${child.label} #${index + 1}`
}

function getChildRecordSubtitle(child: DataComboChildCollection, record: Record<string, any>): string {
  const columns = getPriorityColumns(child).slice(1, 4)
  const pairs = columns
    .map((column) => {
      const text = formatCell(column, record[column.field])
      if (!text || text === '-') {
        return ''
      }

      return `${column.label}: ${text}`
    })
    .filter(Boolean)

  return pairs.join(' · ')
}

function getChildRecordKey(
  child: DataComboChildCollection,
  record: Record<string, any>,
  index: number
): string {
  const code = record[child.codeField]
  const priority = getPriorityColumns(child).slice(0, 2)
  const suffix = priority
    .map((column) => record[column.field])
    .filter((value) => value !== null && value !== undefined && value !== '')
    .join('-')

  return [child.key, code, suffix, index].filter(Boolean).join(':')
}

function resetChildPresentationState() {
  expandedChildRecords.value = {}
}

async function loadRowDetail() {
  loading.value = true

  try {
    const res = await dataComboApi.getRow(comboId.value, rowCode.value) as any
    combo.value = res.data?.combo || null
    rowData.value = res.data?.row || null
    resetChildPresentationState()
    restoreMainFieldSlots(combo.value?.mainColumns || [])
    restoreAllLayouts()
    if (combo.value) {
      syncMainFieldSlots(combo.value.mainColumns)
    }
  } catch {
    ElMessage.error('加载单行子表数据失败')
  } finally {
    loading.value = false
  }
}

watch([comboId, rowCode], () => {
  loadRowDetail()
})

watch(
  () => mainLayoutConfig.value,
  () => {
    if (!combo.value) {
      return
    }

    syncMainFieldSlots(combo.value.mainColumns)
  },
  { deep: true }
)

watch(comboId, () => {
  draggingMainSlotIndex.value = null
  dragOverMainSlotIndex.value = null
})

onMounted(() => {
  loadRowDetail()

  unsubscribeFieldDisplaySync = subscribeFieldDisplayConfigChange((tableName) => {
    if (!comboTableNames.value.has(tableName)) {
      return
    }

    loadRowDetail()
  })
})

onUnmounted(() => {
  if (unsubscribeFieldDisplaySync) {
    unsubscribeFieldDisplaySync()
  }
})
</script>

<style scoped>
.data-combo-row-view {
  min-height: 100vh;
  padding: 20px;
  background: #f5f7fa;
}

.page-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 20px;
  padding: 20px 22px;
  border: 1px solid #e5e7eb;
  border-radius: 12px;
  background: #fff;
}

.page-intro {
  min-width: 0;
}

.page-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  justify-content: flex-end;
}

.title-row {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
  flex-wrap: wrap;
}

.stock-summary {
  display: flex;
  align-items: center;
  gap: 8px;
}

.stock-name {
  font-size: 18px;
  font-weight: 600;
  color: #1f2937;
}

.page-desc {
  margin: 0;
  max-width: 840px;
  padding-left: 12px;
  border-left: 3px solid #dbeafe;
  color: #6b7280;
  font-size: 13px;
  line-height: 1.7;
}

.content-wrap {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.detail-card {
  border: 1px solid #e5e7eb;
  border-radius: 12px;
  box-shadow: none;
}

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.section-header-main,
.section-header-actions {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  color: #1f2937;
  font-weight: 600;
}

.header-helper {
  font-size: 12px;
  font-weight: 400;
  color: #9ca3af;
}

.main-field-grid,
.child-field-grid {
  display: grid;
  gap: 8px;
  grid-template-columns: repeat(var(--grid-columns, 3), minmax(0, 1fr));
}

.main-field-grid-draggable {
  user-select: none;
}

.main-field-slot {
  min-width: 0;
  min-height: 74px;
  border-radius: 8px;
}

.main-field-slot.is-drag-over {
  outline: 2px dashed #60a5fa;
  outline-offset: 2px;
}

.main-field-slot.is-empty {
  border: 1px dashed transparent;
  background: transparent;
}

.main-field-slot-empty {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 74px;
  padding: 10px;
  color: #94a3b8;
  font-size: 12px;
  line-height: 1.4;
  text-align: center;
  opacity: 0;
  transition: opacity 0.15s ease;
  pointer-events: none;
}

.main-field-slot.is-empty:hover,
.main-field-slot.is-empty.is-drag-over {
  border-color: #cbd5e1;
  background: #f8fafc;
}

.main-field-slot.is-empty:hover .main-field-slot-empty,
.main-field-slot.is-empty.is-drag-over .main-field-slot-empty {
  opacity: 1;
}

.field-card {
  min-width: 0;
  border: 1px solid #eef2f7;
  border-radius: 8px;
  background: #fff;
}

.main-field-card {
  display: flex;
  flex-direction: column;
  gap: 6px;
  height: 100%;
  min-height: 74px;
  padding: 10px 10px 9px;
  cursor: grab;
}

.main-field-card:active {
  cursor: grabbing;
}

.field-card-compact {
  display: grid;
  grid-template-columns: 96px minmax(0, 1fr);
  column-gap: 10px;
  align-items: start;
  min-height: 56px;
  padding: 9px 11px;
}

.field-card-wide {
  grid-column: 1 / -1;
}

.field-card-label {
  min-width: 0;
  font-size: 12px;
  font-weight: 600;
  color: #6b7280;
  line-height: 1.5;
}

.field-card-value-wrap {
  min-width: 0;
  display: flex;
  align-items: center;
}

.field-card-value {
  min-width: 0;
  font-size: 13px;
  color: #111827;
  line-height: 1.55;
  white-space: pre-wrap;
  word-break: break-word;
}

.main-field-card .field-card-value-wrap {
  align-items: flex-start;
}

.main-field-card .field-card-value {
  display: -webkit-box;
  overflow: hidden;
  min-height: 2.8em;
  font-size: 12px;
  line-height: 1.4;
  text-align: left;
  text-overflow: ellipsis;
  white-space: normal;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.layout-hint {
  margin-top: 8px;
  font-size: 12px;
  color: #9ca3af;
}

.table-preview-block {
  margin-top: 14px;
  padding-top: 14px;
  border-top: 1px solid #eef2f7;
}

.table-preview-title {
  margin-bottom: 10px;
  font-size: 12px;
  font-weight: 600;
  color: #6b7280;
}

.child-record-list {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.child-record-card {
  padding: 12px;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  background: #fff;
}

.record-card-top {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 10px;
}

.record-title-group {
  min-width: 0;
}

.record-title {
  font-size: 14px;
  font-weight: 600;
  color: #111827;
  line-height: 1.5;
}

.record-subtitle {
  margin-top: 2px;
  font-size: 11px;
  color: #6b7280;
  line-height: 1.5;
}

.record-more-hint {
  margin-top: 8px;
  font-size: 11px;
  color: #9ca3af;
}

.child-empty {
  padding: 8px 0 4px;
}

.child-empty :deep(.el-empty__image) {
  width: 56px;
}

.child-empty :deep(.el-empty__description p) {
  color: #a0a8b5;
  font-size: 12px;
}

.child-table {
  width: 100%;
}

:deep(.el-table__header .cell) {
  white-space: nowrap;
  overflow: visible;
  text-overflow: clip;
}

:deep(.el-table__body .cell) {
  display: flex !important;
  align-items: center;
  justify-content: center !important;
  width: 100% !important;
  margin: 0 auto;
  padding-left: 0 !important;
  padding-right: 0 !important;
  text-align: center !important;
}

:deep(.el-table__body .cell.el-tooltip) {
  display: flex !important;
  align-items: center;
  justify-content: center !important;
  width: 100% !important;
  padding-left: 0 !important;
  padding-right: 0 !important;
  text-align: center !important;
}

.table-header {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  min-height: 24px;
  padding: 0 20px;
  box-sizing: border-box;
}

.table-header-center-group {
  position: relative;
  width: max-content;
  margin: 0 auto;
}

.table-header-label {
  white-space: nowrap;
  overflow: visible;
  text-overflow: clip;
  text-align: center;
}

.combo-cell-value {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  max-width: 100%;
  margin: 0 auto;
  text-align: center;
}

.combo-cell-value.cell-content-single-ellipsis {
  display: block;
  width: 100%;
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
}

.value-null {
  color: #c0c4cc;
  font-style: italic;
}

.value-number {
  color: #2563eb;
  font-family: Consolas, Monaco, monospace;
}

.cell-content-clamp {
  display: -webkit-box;
  overflow: hidden;
  max-height: 2.9em;
  line-height: 1.45;
  text-align: left;
  text-overflow: ellipsis;
  white-space: normal;
  word-break: break-word;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.cell-content-expandable {
  cursor: pointer;
}

.cell-content-expandable:hover {
  color: #409eff;
}

.full-text-dialog-content {
  overflow-y: auto;
  max-height: 60vh;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-word;
}

.layout-editor-body {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.layout-editor-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.layout-editor-field {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.layout-editor-label,
.layout-editor-helper {
  font-size: 12px;
  line-height: 1.6;
  color: #6b7280;
}

.layout-editor-field :deep(.el-input-number) {
  width: 100%;
}

@media (max-width: 1200px) {
  .main-field-grid {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }
}

@media (max-width: 900px) {
  .page-header,
  .record-card-top {
    flex-direction: column;
  }

  .page-actions {
    width: 100%;
    justify-content: flex-start;
  }

  .main-field-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .child-field-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .field-card-compact {
    grid-template-columns: 84px minmax(0, 1fr);
  }
}

@media (max-width: 640px) {
  .data-combo-row-view {
    padding: 12px;
  }

  .main-field-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .child-field-grid {
    grid-template-columns: 1fr;
  }

  .field-card-compact {
    grid-template-columns: 1fr;
    row-gap: 8px;
  }
}

@media (max-width: 480px) {
  .main-field-grid {
    grid-template-columns: 1fr;
  }
}
</style>
