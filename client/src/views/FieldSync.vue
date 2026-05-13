<template>
  <div class="field-sync">
    <div class="header">
      <div class="header-left">
        <el-button @click="goBack" :icon="ArrowLeft">返回</el-button>
        <h2>更新接口&字段</h2>
      </div>
      <div class="header-actions">
        <el-button type="primary" @click="refreshCheck" :loading="loading">
          <el-icon><Refresh /></el-icon>
          重新检查
        </el-button>
        <el-button
          v-if="checkResult && checkResult.summary.diff > 0 && !syncExecuted"
          type="warning"
          @click="executeSyncFields"
          :loading="syncing"
        >
          <el-icon><Check /></el-icon>
          执行同步 ({{ checkResult.summary.diff }} 个)
        </el-button>
      </div>
    </div>

    <!-- 统计摘要 -->
    <div v-if="checkResult" class="summary-cards">
      <div class="summary-card success">
        <div class="card-value">{{ checkResult.summary.ok }}</div>
        <div class="card-label">一致</div>
      </div>
      <div class="summary-card warning">
        <div class="card-value">{{ checkResult.summary.diff }}</div>
        <div class="card-label">需同步</div>
      </div>
      <div class="summary-card info">
        <div class="card-value">{{ checkResult.summary.skip }}</div>
        <div class="card-label">跳过</div>
      </div>
      <div class="summary-card danger">
        <div class="card-value">{{ checkResult.summary.failed }}</div>
        <div class="card-label">失败</div>
      </div>
      <div class="summary-card total">
        <div class="card-value">{{ checkResult.summary.total }}</div>
        <div class="card-label">总计</div>
      </div>
    </div>

    <!-- 同步警告 -->
    <div v-if="checkResult && checkResult.summary.diff > 0 && !syncExecuted" class="sync-warning">
      <el-alert type="warning" :closable="false" show-icon>
        <template #title>
          检测到 {{ checkResult.summary.diff }} 个接口字段有变化，同步将重建这些表，数据会被清空。
        </template>
      </el-alert>
    </div>

    <!-- 同步成功提示 -->
    <div v-if="syncExecuted" class="sync-success">
      <el-alert type="success" :closable="false" show-icon>
        <template #title>
          同步完成！已重建 {{ checkResult?.summary.synced || 0 }} 个表，请点击对应接口的"更新"按钮重新采集数据。
        </template>
      </el-alert>
    </div>

    <!-- 字段详情弹窗 -->
    <el-dialog v-model="detailDialogVisible" :title="`${currentDetail?.cn_name} - 字段对比`" width="900px">
      <div v-if="currentDetail" class="field-detail">
        <el-row :gutter="20">
          <el-col :span="12">
            <div class="field-list">
              <h4>API 字段 ({{ currentDetail.apiFields.length }})</h4>
              <div class="field-tags">
                <el-tag
                  v-for="field in currentDetail.apiFields"
                  :key="field"
                  :type="currentDetail.addedFields.includes(field) ? 'success' : ''"
                  size="small"
                  class="field-tag"
                >
                  {{ field }}
                  <span v-if="currentDetail.addedFields.includes(field)" class="tag-badge">新</span>
                </el-tag>
              </div>
            </div>
          </el-col>
          <el-col :span="12">
            <div class="field-list">
              <h4>数据库字段 ({{ currentDetail.dbFields.length }})</h4>
              <div class="field-tags">
                <el-tag
                  v-for="field in currentDetail.dbFields"
                  :key="field"
                  :type="currentDetail.removedFields.includes(field) ? 'danger' : ''"
                  size="small"
                  class="field-tag"
                >
                  {{ field }}
                  <span v-if="currentDetail.removedFields.includes(field)" class="tag-badge">删</span>
                </el-tag>
              </div>
            </div>
          </el-col>
        </el-row>
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { metadataApi } from '../api'
import { ArrowLeft, Refresh, Check } from '@element-plus/icons-vue'

const router = useRouter()

const loading = ref(false)
const syncing = ref(false)
const syncExecuted = ref(false)
const checkResult = ref<any>(null)
const detailDialogVisible = ref(false)
const currentDetail = ref<any>(null)

function goBack() {
  router.push('/')
}

async function refreshCheck() {
  loading.value = true
  syncExecuted.value = false
  try {
    const res = await metadataApi.checkFields() as any
    if (res.success) {
      checkResult.value = res.data
    } else {
      ElMessage.error('检查失败')
    }
  } catch (error) {
    ElMessage.error('检查字段失败')
  } finally {
    loading.value = false
  }
}

async function executeSyncFields() {
  syncing.value = true
  try {
    const res = await metadataApi.syncFields() as any
    if (res.success) {
      checkResult.value = res.data
      syncExecuted.value = true
      ElMessage.success(res.message || '同步完成')
    } else {
      ElMessage.error('同步失败')
    }
  } catch (error) {
    ElMessage.error('同步字段失败')
  } finally {
    syncing.value = false
  }
}

function getStatusType(status: string) {
  const types: Record<string, string> = {
    ok: 'success',
    diff: 'warning',
    skip: 'info',
    error: 'danger'
  }
  return types[status] || 'info'
}

function getStatusLabel(status: string) {
  const labels: Record<string, string> = {
    ok: '一致',
    diff: '需同步',
    skip: '跳过',
    error: '失败'
  }
  return labels[status] || status
}

function getRowClassName({ row }: { row: any }) {
  if (row.status === 'diff') return 'row-diff'
  if (row.status === 'error') return 'row-error'
  return ''
}

function formatFields(fields: string[]) {
  if (fields.length <= 3) {
    return fields.join(', ')
  }
  return fields.slice(0, 3).join(', ') + ` ... 等${fields.length}个`
}

function showFieldDetail(row: any) {
  currentDetail.value = row
  detailDialogVisible.value = true
}

function formatActualParams(params: Record<string, any>) {
  if (!params || Object.keys(params).length === 0) return '-'

  // 过滤掉分页参数和过长的值，只显示关键参数
  const skipKeys = ['pageSize', 'pageNumber', 'pn', 'pz', 'cb', 'np', 'po', 'ut', 'fltt', 'invt', 'wbp2u', 'fid', '_']
  const keyParams = Object.entries(params)
    .filter(([key, value]) => !skipKeys.includes(key))
    .map(([key, value]) => {
      const strValue = String(value)
      // 如果值太长，截断显示
      const displayValue = strValue.length > 30 ? strValue.substring(0, 30) + '...' : strValue
      return `${key}=${displayValue}`
    })

  if (keyParams.length === 0) {
    // 如果过滤后没有参数，显示前几个
    return Object.entries(params).slice(0, 3).map(([k, v]) => `${k}=${v}`).join(', ')
  }

  return keyParams.join(', ')
}

function formatParamValue(value: any) {
  const strValue = String(value)
  return strValue.length > 80 ? strValue.substring(0, 80) + '...' : strValue
}

onMounted(() => {
  refreshCheck()
})
</script>

<style scoped>
.field-sync {
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

.header-actions {
  display: flex;
  gap: 10px;
}

.summary-cards {
  display: flex;
  gap: 15px;
  margin-bottom: 20px;
}

.summary-card {
  flex: 1;
  padding: 15px 20px;
  border-radius: 8px;
  text-align: center;
}

.summary-card.success {
  background: #f0f9eb;
  border: 1px solid #e1f3d8;
}

.summary-card.warning {
  background: #fdf6ec;
  border: 1px solid #faecd8;
}

.summary-card.info {
  background: #f4f4f5;
  border: 1px solid #e9e9eb;
}

.summary-card.danger {
  background: #fef0f0;
  border: 1px solid #fde2e2;
}

.summary-card.total {
  background: #ecf5ff;
  border: 1px solid #d9ecff;
}

.card-value {
  font-size: 28px;
  font-weight: bold;
  line-height: 1.2;
}

.summary-card.success .card-value { color: #67c23a; }
.summary-card.warning .card-value { color: #e6a23c; }
.summary-card.info .card-value { color: #909399; }
.summary-card.danger .card-value { color: #f56c6c; }
.summary-card.total .card-value { color: #409eff; }

.card-label {
  font-size: 14px;
  color: #606266;
  margin-top: 5px;
}

.sync-warning, .sync-success {
  margin-bottom: 20px;
}

.text-success { color: #67c23a; }
.text-warning { color: #e6a23c; }
.text-danger { color: #f56c6c; }
.text-info { color: #909399; }
.text-muted { color: #c0c4cc; }

.field-changes {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.change-item {
  display: flex;
  align-items: center;
  gap: 8px;
}

.change-label {
  font-weight: 500;
  flex-shrink: 0;
}

.change-fields {
  cursor: pointer;
  font-size: 13px;
}

:deep(.row-diff) {
  background-color: #fdf6ec !important;
}

:deep(.row-error) {
  background-color: #fef0f0 !important;
}

.field-detail {
  padding: 10px 0;
}

.field-list h4 {
  margin: 0 0 15px 0;
  color: #303133;
  font-size: 15px;
}

.field-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  max-height: 400px;
  overflow-y: auto;
}

.field-tag {
  position: relative;
}

.tag-badge {
  position: absolute;
  top: -8px;
  right: -8px;
  background: #409eff;
  color: white;
  font-size: 10px;
  padding: 1px 4px;
  border-radius: 4px;
}

.field-tag.el-tag--success .tag-badge {
  background: #67c23a;
}

.field-tag.el-tag--danger .tag-badge {
  background: #f56c6c;
}

.api-url {
  font-family: monospace;
  font-size: 12px;
  color: #409eff;
  word-break: break-all;
}

.params-display {
  font-family: monospace;
  font-size: 12px;
}

.params-text {
  cursor: pointer;
  color: #606266;
  word-break: break-all;
}

.params-text:hover {
  color: #409eff;
}

.params-tooltip {
  max-width: 500px;
  max-height: 400px;
  overflow-y: auto;
}

.param-line {
  margin-bottom: 4px;
  line-height: 1.4;
}

.param-key {
  color: #409eff;
  font-weight: 500;
  margin-right: 4px;
}

.param-value {
  color: #303133;
  word-break: break-all;
}
</style>
