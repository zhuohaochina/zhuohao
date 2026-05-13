<template>
  <div class="data-combo-view">
    <div class="page-header">
      <div class="page-intro">
        <div class="title-row">
          <el-button @click="goToList" :icon="ArrowLeft">返回关联视图</el-button>
          <h2>{{ combo?.name || '关联视图' }}</h2>
        </div>
        <p class="page-desc">
          {{ combo?.description || '点击任意主表行，会在新窗口打开这行对应的全部子表数据。' }}
        </p>
      </div>
      <div class="page-stats">
        <el-tag type="success">共 {{ total }} 条</el-tag>
      </div>
    </div>

    <div class="toolbar">
      <div class="search-box">
        <el-input
          v-model="keyword"
          size="large"
          clearable
          placeholder="按当前视图展示列搜索"
          @keyup.enter="handleSearch"
          @clear="handleClear"
        >
          <template #prefix>
            <el-icon><Search /></el-icon>
          </template>
        </el-input>
        <el-button
          type="primary"
          size="large"
          @click="handleSearch"
          :loading="loading"
          style="margin-left: 8px;"
        >
          搜索
        </el-button>
        <el-button
          size="large"
          @click="handleReset"
          :disabled="!hasActiveSearch"
          style="margin-left: 8px;"
        >
          重置
        </el-button>
      </div>
    </div>

    <el-table
      v-loading="loading"
      :data="rows"
      stripe
      border
      :row-key="getRowKey"
      row-class-name="clickable-row"
      max-height="calc(100vh - 280px)"
      empty-text="暂无数据"
      @row-click="openRowWindow"
    >
      <el-table-column
        v-for="child in combo?.childCollections || []"
        :key="child.countField"
        :min-width="getCountColumnWidth(child.label)"
        align="center"
      >
        <template #header>
          <div class="sortable-header" @click.stop="toggleSort(child.countField)">
            <div class="sortable-header-center-group">
              <span class="sortable-header-label">{{ child.label }}</span>
              <el-icon class="sort-icon sortable-header-icon" :class="getSortIconClass(child.countField)">
                <component :is="getSortIcon(child.countField)" />
              </el-icon>
            </div>
          </div>
        </template>
        <template #default="{ row }">
          <el-tag v-if="getPositiveCount(row[child.countField]) !== null" type="warning">
            {{ getPositiveCount(row[child.countField]) }}
          </el-tag>
          <span v-else class="empty-count-cell"></span>
        </template>
      </el-table-column>

      <el-table-column
        v-for="column in combo?.mainColumns || []"
        :key="column.field"
        :prop="column.field"
        :min-width="getColumnWidth(column.label)"
        show-overflow-tooltip
      >
        <template #header>
          <div class="sortable-header" @click.stop="toggleSort(column.field)">
            <div class="sortable-header-center-group">
              <span class="sortable-header-label">{{ column.label }}</span>
              <el-icon class="sort-icon sortable-header-icon" :class="getSortIconClass(column.field)">
                <component :is="getSortIcon(column.field)" />
              </el-icon>
            </div>
          </div>
        </template>
        <template #default="{ row }">
          <span
            :class="[
              'combo-cell-value',
              getCellClass(column, row[column.field]),
              {
                'cell-content-single-ellipsis': shouldUseSingleLineEllipsis(column),
                'cell-content-clamp': shouldClampCell(column, row[column.field]),
                'cell-content-expandable': shouldClampCell(column, row[column.field])
              }
            ]"
            :title="shouldClampCell(column, row[column.field]) || shouldUseSingleLineEllipsis(column) ? String(row[column.field] ?? '') : ''"
            @click="handleMainCellClick($event, column, row[column.field])"
          >
            {{ formatMainCell(column, row[column.field]) }}
          </span>
        </template>
      </el-table-column>
    </el-table>

    <div class="pagination">
      <el-pagination
        v-model:current-page="page"
        v-model:page-size="pageSize"
        :page-sizes="[20, 50, 100]"
        :total="total"
        layout="total, sizes, prev, pager, next, jumper"
        @current-change="handlePageChange"
        @size-change="handleSizeChange"
      />
    </div>

    <el-dialog v-model="fullTextDialogVisible" :title="fullTextDialogTitle" width="760px">
      <div class="full-text-dialog-content">{{ fullTextDialogContent }}</div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { ArrowLeft, Search, Sort, SortDown, SortUp } from '@element-plus/icons-vue'
import { dataComboApi } from '../api'
import { subscribeFieldDisplayConfigChange } from '../utils/fieldDisplaySync'
import { formatDisplayValue, getDisplayValueClass, shouldClampDisplayCell } from '../utils/fieldPresentation'
import type { DataComboColumn, DataComboDetail } from '../types'

const route = useRoute()
const router = useRouter()

const combo = ref<DataComboDetail | null>(null)
const rows = ref<Record<string, any>[]>([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(20)
const keyword = ref('')
const sortField = ref('')
const sortOrder = ref<'asc' | 'desc'>('asc')
const loading = ref(false)
const fullTextDialogVisible = ref(false)
const fullTextDialogTitle = ref('')
const fullTextDialogContent = ref('')
let unsubscribeFieldDisplaySync: (() => void) | null = null

const comboId = computed(() => String(route.params.id || ''))
const hasActiveSearch = computed(() => !!keyword.value.trim())
const comboTableNames = computed(() => {
  if (!combo.value) {
    return new Set<string>()
  }

  return new Set([
    combo.value.mainTableName,
    ...combo.value.childCollections.map((child) => child.tableName)
  ])
})

function goToList() {
  router.push('/data-combos')
}

function getRowKey(row: Record<string, any>) {
  const keyField = combo.value?.mainCodeField
  return keyField ? row[keyField] : JSON.stringify(row)
}

function getPositiveCount(value: unknown): number | null {
  const count = typeof value === 'number' ? value : Number(value)
  if (!Number.isFinite(count) || count <= 0) {
    return null
  }
  return count
}

function estimateHeaderWidth(text: string, extra: number = 48): number {
  const normalized = String(text || '')
  let width = extra

  for (const char of normalized) {
    width += /[A-Za-z0-9_]/.test(char) ? 10 : 16
  }

  return Math.max(width, 120)
}

function getColumnWidth(label: string): number {
  return estimateHeaderWidth(label, 56)
}

function getCountColumnWidth(label: string): number {
  return estimateHeaderWidth(`${label}条数`, 64)
}

function formatMainCell(column: DataComboColumn, value: unknown): string {
  return formatDisplayValue(value, column.type, column.field, column.display)
}

function getCellClass(column: DataComboColumn, value: unknown): string {
  return getDisplayValueClass(value, column.type)
}

function shouldClampCell(column: DataComboColumn, value: unknown): boolean {
  return shouldClampDisplayCell(value, column.display)
}

function shouldUseSingleLineEllipsis(column: DataComboColumn): boolean {
  return column.field === 'f103' || column.label === '所属概念板块'
}

function handleMainCellClick(event: MouseEvent, column: DataComboColumn, value: unknown) {
  if (!shouldClampCell(column, value)) {
    return
  }

  event.stopPropagation()
  fullTextDialogTitle.value = `${column.label} (${column.field})`
  fullTextDialogContent.value = value === null || value === undefined ? '' : String(value)
  fullTextDialogVisible.value = true
}

async function refreshComboView() {
  await loadRows()
}

function toggleSort(field: string) {
  if (sortField.value !== field) {
    sortField.value = field
    sortOrder.value = 'desc'
  } else if (sortOrder.value === 'desc') {
    sortOrder.value = 'asc'
  } else {
    sortField.value = ''
    sortOrder.value = 'asc'
  }

  page.value = 1
  loadRows()
}

function getSortIcon(field: string) {
  if (sortField.value !== field) {
    return Sort
  }

  return sortOrder.value === 'asc' ? SortUp : SortDown
}

function getSortIconClass(field: string) {
  return {
    'is-active': sortField.value === field,
    'is-desc': sortField.value === field && sortOrder.value === 'desc'
  }
}

function getSortLabel(field: string) {
  const child = combo.value?.childCollections.find((item) => item.countField === field)
  if (child) {
    return `${child.label}条数`
  }

  const mainColumn = combo.value?.mainColumns.find((item) => item.field === field)
  return mainColumn?.label || field
}

function openRowWindow(row: Record<string, any>) {
  const keyField = combo.value?.mainCodeField
  const code = keyField ? row[keyField] : ''
  if (!keyField || !code) {
    ElMessage.warning('当前行缺少主表关联值，无法打开子表明细')
    return
  }

  const target = router.resolve(`/data-combos/${comboId.value}/row/${encodeURIComponent(String(code))}`)
  window.open(target.href, '_blank', 'noopener,noreferrer')
}

async function loadComboDetail() {
  try {
    const res = await dataComboApi.getById(comboId.value) as any
    combo.value = res.data || null
  } catch {
    ElMessage.error('加载关联视图详情失败')
  }
}

async function loadRows() {
  loading.value = true

  try {
    const res = await dataComboApi.getData(
      comboId.value,
      page.value,
      pageSize.value,
      keyword.value,
      sortField.value,
      sortField.value ? sortOrder.value : ''
    ) as any

    const payload = res.data
    combo.value = payload.combo
    rows.value = payload.rows || []
    total.value = payload.total || 0
  } catch {
    ElMessage.error('加载关联视图数据失败')
  } finally {
    loading.value = false
  }
}

function handleSearch() {
  page.value = 1
  loadRows()
}

function handleClear() {
  keyword.value = ''
  page.value = 1
  loadRows()
}

function handleReset() {
  handleClear()
}

function handlePageChange(nextPage: number) {
  page.value = nextPage
  loadRows()
}

function handleSizeChange(nextPageSize: number) {
  pageSize.value = nextPageSize
  page.value = 1
  loadRows()
}

watch(comboId, () => {
  page.value = 1
  keyword.value = ''
  sortField.value = ''
  sortOrder.value = 'asc'
  loadComboDetail()
  loadRows()
})

onMounted(() => {
  loadComboDetail()
  loadRows()

  unsubscribeFieldDisplaySync = subscribeFieldDisplayConfigChange((tableName) => {
    if (!comboTableNames.value.has(tableName)) {
      return
    }

    refreshComboView()
  })
})

onUnmounted(() => {
  if (unsubscribeFieldDisplaySync) {
    unsubscribeFieldDisplaySync()
  }
})
</script>

<style scoped>
.data-combo-view {
  background: white;
  padding: 20px;
  border-radius: 8px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.1);
}

.page-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 20px;
  padding: 18px 20px;
  border: 1px solid #e8edf5;
  border-radius: 14px;
  background:
    radial-gradient(circle at top right, rgba(64, 158, 255, 0.09), transparent 34%),
    linear-gradient(135deg, #fcfdff 0%, #f7faff 100%);
}

.page-intro {
  min-width: 0;
}

.title-row {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
}

.title-row h2 {
  margin: 0;
  color: #303133;
}

.page-desc {
  margin: 0;
  max-width: 760px;
  padding-left: 12px;
  border-left: 3px solid #bfdbfe;
  color: #7b8794;
  font-size: 13px;
  line-height: 1.7;
}

.page-stats {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.toolbar {
  margin-bottom: 16px;
}

.search-box {
  display: flex;
  align-items: center;
}

.search-box .el-input {
  flex: 1;
}

.search-box :deep(.el-input-group__append) {
  background-color: #409eff;
  border-color: #409eff;
}

.search-box :deep(.el-input-group__append .el-button) {
  color: white;
}

.pagination {
  margin-top: 20px;
  display: flex;
  justify-content: flex-end;
}

.empty-count-cell {
  display: inline-block;
  min-height: 24px;
}

.sortable-header {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  user-select: none;
  width: 100%;
  min-height: 24px;
  padding: 0 20px;
  box-sizing: border-box;
  max-width: none;
  flex-wrap: nowrap;
}

.sortable-header-center-group {
  position: relative;
  width: max-content;
  margin: 0 auto;
}

.sortable-header-label {
  white-space: nowrap;
  overflow: visible;
  text-overflow: clip;
}

.sort-icon {
  color: #c0c4cc;
  transition: color 0.2s ease;
  flex-shrink: 0;
}

.sortable-header-icon {
  position: absolute;
  left: calc(100% + 10px);
  top: 50%;
  transform: translateY(-50%);
}

.sort-icon.is-active {
  color: #409eff;
}

.sort-icon.is-desc {
  color: #67c23a;
}

:deep(.clickable-row) {
  cursor: pointer;
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
  padding-left: 0 !important;
  padding-right: 0 !important;
  margin: 0 auto;
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
  font-family: monospace;
  color: #409eff;
}

.cell-content-clamp {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: normal;
  word-break: break-word;
  line-height: 1.45;
  max-height: 2.9em;
  text-align: center;
}

.cell-content-expandable {
  cursor: pointer;
}

.cell-content-expandable:hover {
  color: #409eff;
}

.full-text-dialog-content {
  white-space: pre-wrap;
  word-break: break-word;
  max-height: 60vh;
  overflow-y: auto;
  line-height: 1.6;
}
</style>
