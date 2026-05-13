<template>
  <div class="update-history">
    <div class="header">
      <div class="header-left">
        <el-button @click="goBack" :icon="ArrowLeft">返回</el-button>
        <h2>{{ metadata?.cn_name }} - 更新历史</h2>
      </div>
    </div>

    <div class="content" v-loading="loading">
      <!-- 更新规则信息 -->
      <div class="rule-info" v-if="metadata">
        <el-tag type="info" size="large">
          当前更新规则：{{ getUpdateRuleDisplay(metadata) }}
        </el-tag>
      </div>

      <!-- 更新日志列表 -->
      <el-table :data="logs" stripe v-if="logs.length > 0" row-key="id" border>
        <el-table-column prop="update_time" label="更新时间" width="170" fixed>
          <template #default="{ row }">
            {{ formatDateTime(row.update_time) }}
          </template>
        </el-table-column>
        
        <el-table-column prop="total_count" label="总数" width="80" align="center">
          <template #default="{ row }">
            <el-tag :type="row.status === 'success' ? 'success' : 'danger'" size="small">
              {{ row.total_count }}
            </el-tag>
          </template>
        </el-table-column>
        
        <el-table-column prop="status" label="状态" width="70" align="center">
          <template #default="{ row }">
            <el-tag :type="row.status === 'success' ? 'success' : 'danger'" size="small">
              {{ row.status === 'success' ? '成功' : '失败' }}
            </el-tag>
          </template>
        </el-table-column>
        
        <!-- 动态日期列 -->
        <el-table-column 
          v-for="date in allDates" 
          :key="date" 
          :label="formatDateLabel(date)" 
          width="90" 
          align="center"
        >
          <template #default="{ row }">
            <span :class="getCountClass(row, date)">
              {{ getCountForDate(row, date) }}
            </span>
          </template>
        </el-table-column>
        
        <el-table-column prop="error_message" label="错误" width="120">
          <template #default="{ row }">
            <span v-if="row.error_message" class="error-text">{{ row.error_message }}</span>
            <span v-else class="no-error">-</span>
          </template>
        </el-table-column>
      </el-table>

      <!-- 空状态 -->
      <el-empty v-else description="暂无更新记录，点击首页的「更新」按钮后会自动记录" />

      <!-- 分页 -->
      <div class="pagination" v-if="total > 0">
        <el-pagination
          v-model:current-page="currentPage"
          v-model:page-size="pageSize"
          :total="total"
          :page-sizes="[10, 20, 50, 100]"
          layout="total, sizes, prev, pager, next, jumper"
          @size-change="loadData"
          @current-change="loadData"
        />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { ArrowLeft } from '@element-plus/icons-vue'
import { updateLogApi } from '../api'

const route = useRoute()
const router = useRouter()

// 数据状态
const loading = ref(false)
const metadata = ref<any>(null)
const logs = ref<any[]>([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)

// 从更新规则中提取天数
function extractDaysFromRule(rule: string | null): number {
  if (!rule) return 7 // 默认7天
  // 匹配 "取最近N天" 或 "取近N天" 的模式
  const match = rule.match(/取(?:最近)?(\d+)天/)
  if (match) {
    return parseInt(match[1])
  }
  // 默认返回7天
  return 7
}

// 获取日期字段的中文标签
function getDateFieldLabel(dateField: string | null): string {
  if (!dateField) return ''
  const labels: Record<string, string> = {
    'notice_date': '公告日期',
    'pubDate': '发布日期',
    'eitime': '入库时间',
    'tdate': '交易日期',
    'change_date': '变动日期',
    'ann_date': '公告日期',
    'trade_date': '交易日期',
    'attachedPubDate': '回复日期',
    'updateDate': '更新日期',
    'eitime': '入库时间'
  }
  return labels[dateField] || dateField
}

// 获取天数范围的展示标签
function getDateRangeLabel(dateRange: number | null, futureDays: number | null): string {
  if (!dateRange) return ''
  // 如果有未来天数，显示为 "-10天~+2天" 格式
  if (futureDays && futureDays > 0) {
    return `-${dateRange}天~+${futureDays}天`
  }
  // 否则只显示过去天数
  return `${dateRange}天`
}

// 获取更新规则的展示文本
function getUpdateRuleDisplay(meta: any): string {
  if (!meta) return '默认'
  
  const parts: string[] = []
  
  // 更新模式
  if (meta.update_mode === 'incremental') {
    parts.push('增量')
  } else if (meta.update_mode === 'full') {
    parts.push('全量')
  }
  
  // 日期字段
  if (meta.date_field) {
    parts.push(getDateFieldLabel(meta.date_field))
  }
  
  // 天数范围
  if (meta.date_range) {
    parts.push(getDateRangeLabel(meta.date_range, meta.future_days))
  }
  
  // 如果有结构化字段，返回组合结果
  if (parts.length > 0) {
    return parts.join(' | ')
  }
  
  // 回退到旧的 update_rule 字段
  return meta.update_rule || '默认'
}

// 计算需要显示的日期列表
// 基于最新一条更新记录的每日统计数据来确定日期范围
const allDates = computed(() => {
  const days = extractDaysFromRule(metadata.value?.update_rule)
  
  // 如果有日志数据，从最新的日志中获取日期
  if (logs.value.length > 0) {
    const latestLog = logs.value[0]
    const stats = parseDailyStats(latestLog)
    if (stats.length > 0) {
      // 获取最新日志中的所有日期，按降序排列
      const dates = stats.map(s => s.date).sort((a, b) => b.localeCompare(a))
      return dates
    }
  }
  
  // 如果没有日志数据，使用从今天开始往前的日期
  const dates: string[] = []
  const today = new Date()
  
  for (let i = 0; i < days; i++) {
    const date = new Date(today)
    date.setDate(date.getDate() - i)
    const year = date.getFullYear()
    const month = String(date.getMonth() + 1).padStart(2, '0')
    const day = String(date.getDate()).padStart(2, '0')
    const dateStr = `${year}-${month}-${day}`
    dates.push(dateStr)
  }
  
  return dates
})

// 加载数据
async function loadData() {
  const metadataId = parseInt(route.params.metadataId as string)
  if (isNaN(metadataId)) {
    ElMessage.error('无效的接口 ID')
    return
  }

  loading.value = true
  try {
    const res = await updateLogApi.getLogs(metadataId, currentPage.value, pageSize.value) as any
    if (res.success) {
      metadata.value = res.data.metadata
      logs.value = res.data.logs
      total.value = res.data.total
    }
  } catch (error) {
    ElMessage.error('加载更新历史失败')
  } finally {
    loading.value = false
  }
}

// 返回上一页
function goBack() {
  router.push('/')
}

// 格式化日期时间
function formatDateTime(dateStr: string): string {
  if (!dateStr) return '-'
  try {
    const date = new Date(dateStr)
    const year = date.getFullYear()
    const month = String(date.getMonth() + 1).padStart(2, '0')
    const day = String(date.getDate()).padStart(2, '0')
    const hour = String(date.getHours()).padStart(2, '0')
    const minute = String(date.getMinutes()).padStart(2, '0')
    return `${month}-${day} ${hour}:${minute}`
  } catch {
    return dateStr
  }
}

// 格式化日期标签（只显示月-日）
function formatDateLabel(dateStr: string): string {
  if (!dateStr) return '-'
  const parts = dateStr.split('-')
  if (parts.length === 3) {
    return `${parts[1]}-${parts[2]}`
  }
  return dateStr
}

// 解析每日统计数据
function parseDailyStats(row: any): { date: string; count: number }[] {
  if (!row.daily_stats) return []
  try {
    const stats = JSON.parse(row.daily_stats)
    return Array.isArray(stats) ? stats : []
  } catch {
    return []
  }
}

// 获取指定日期的数量
function getCountForDate(row: any, date: string): string {
  const stats = parseDailyStats(row)
  const item = stats.find(s => s.date === date)
  return item ? String(item.count) : '0'
}

// 获取数量的样式类
function getCountClass(row: any, date: string): string {
  const stats = parseDailyStats(row)
  const item = stats.find(s => s.date === date)
  if (!item || item.count === 0) return 'count-zero'
  return 'count-normal'
}

onMounted(() => {
  loadData()
})
</script>

<style scoped>
.update-history {
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
  padding-bottom: 15px;
  border-bottom: 1px solid #ebeef5;
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

.rule-info {
  margin-bottom: 20px;
}

.error-text {
  color: #f56c6c;
  font-size: 12px;
}

.no-error {
  color: #909399;
}

.pagination {
  margin-top: 20px;
  display: flex;
  justify-content: flex-end;
}

.count-normal {
  color: #409eff;
  font-weight: 600;
}

.count-zero {
  color: #e6a23c;
}

.count-empty {
  color: #c0c4cc;
}
</style>
