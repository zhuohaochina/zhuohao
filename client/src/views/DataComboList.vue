<template>
  <div class="data-combo-list">
    <div class="page-header">
      <div class="page-intro">
        <div class="title-row">
          <el-button @click="goHome" :icon="ArrowLeft">返回首页</el-button>
          <h2>关联视图</h2>
        </div>
        <p class="page-desc">
          把多个原始接口按固定规则组合成可复用视图，主表保留原始行数，子表命中结果聚合成数组。
        </p>
      </div>
      <div class="page-actions">
        <el-button type="primary" :icon="Plus" @click="openCreateDialog">新建组合</el-button>
      </div>
    </div>

    <div v-loading="loading" class="combo-grid">
      <el-empty v-if="!loading && combos.length === 0" description="暂无可用关联视图" />

      <el-card
        v-for="combo in combos"
        :key="combo.id"
        class="combo-card"
        shadow="hover"
      >
        <template #header>
          <div class="card-header">
            <div>
              <div class="card-title-row">
                <h3>{{ combo.name }}</h3>
              </div>
              <p v-if="combo.description">{{ combo.description }}</p>
            </div>
            <div class="card-actions">
              <el-button type="primary" @click="openCombo(combo.id)">进入视图</el-button>
              <el-button
                type="danger"
                plain
                :icon="Delete"
                @click="handleDelete(combo)"
              >
                删除
              </el-button>
            </div>
          </div>
        </template>

        <div class="card-body">
          <div class="meta-table">
            <div class="meta-row">
              <div class="meta-cell meta-cell-label">主表</div>
              <div class="meta-cell meta-cell-value">
                <el-tag>{{ combo.mainTableLabel || combo.mainTableName }}</el-tag>
              </div>
            </div>
            <div class="meta-row">
              <div class="meta-cell meta-cell-label">子表</div>
              <div class="meta-cell meta-cell-value">
                <div class="child-tags">
                  <el-tag
                    v-for="child in combo.children"
                    :key="child.key"
                    type="success"
                  >
                    {{ child.label }}
                  </el-tag>
                </div>
              </div>
            </div>
          </div>
        </div>
      </el-card>
    </div>

    <el-dialog
      v-model="createDialogVisible"
      title="新建关联视图"
      width="980px"
      destroy-on-close
    >
      <div v-loading="optionsLoading" class="create-dialog-body">
        <el-form label-width="110px">
          <el-form-item label="组合名称">
            <el-input v-model="form.name" placeholder="例如：A股列表关联机构调研" />
          </el-form-item>

          <div class="form-section">
            <div class="section-title">主表配置</div>

            <el-form-item label="主表">
              <el-select
                v-model="form.mainTableName"
                placeholder="请选择主表"
                filterable
                style="width: 100%;"
                @change="handleMainTableChange"
              >
                <el-option
                  v-for="option in metadataOptions"
                  :key="option.tableName"
                  :label="`${option.name} (${option.tableName})`"
                  :value="option.tableName"
                />
              </el-select>
            </el-form-item>

            <el-form-item label="主表关联键">
              <el-select
                v-model="form.mainCodeField"
                placeholder="请选择主表关联字段"
                filterable
                style="width: 100%;"
              >
                <el-option
                  v-for="field in mainTableFields"
                  :key="field.field"
                  :label="`${field.label} (${field.field})`"
                  :value="field.field"
                />
              </el-select>
            </el-form-item>

            <el-form-item label="展示字段">
              <div class="field-select-group">
                <el-select
                  v-model="form.mainDisplayFields"
                  multiple
                  collapse-tags
                  collapse-tags-tooltip
                  placeholder="请选择主表展示字段"
                  filterable
                  style="width: 100%;"
                >
                  <el-option
                  v-for="field in mainDisplayFieldOptions"
                    :key="field.field"
                    :label="`${field.label} (${field.field})`"
                    :value="field.field"
                  />
                </el-select>
                <el-button
                  plain
                  @click="selectAllMainDisplayFields"
                  :disabled="mainDisplayFieldOptions.length === 0"
                >
                  全选所有字段
                </el-button>
              </div>
            </el-form-item>
          </div>

          <div class="form-section">
            <div class="section-title section-title-with-action">
              <span>子表配置</span>
              <el-button type="primary" plain :icon="Plus" @click="addChild">添加子表</el-button>
            </div>

            <div
              v-for="(child, index) in form.children"
              :key="`child-${index}`"
              class="child-editor"
            >
              <div class="child-editor-header">
                <span>子表 {{ index + 1 }}</span>
                <el-button
                  v-if="form.children.length > 1"
                  type="danger"
                  link
                  :icon="Delete"
                  @click="removeChild(index)"
                >
                  删除
                </el-button>
              </div>

              <el-form-item label="子表">
                <el-select
                  v-model="child.tableName"
                  placeholder="请选择子表"
                  filterable
                  style="width: 100%;"
                  @change="handleChildTableChange(index)"
                >
                  <el-option
                    v-for="option in metadataOptions"
                    :key="`${index}-${option.tableName}`"
                    :label="`${option.name} (${option.tableName})`"
                    :value="option.tableName"
                  />
                </el-select>
              </el-form-item>

              <el-form-item label="子表关联键">
                <el-select
                  v-model="child.codeField"
                  placeholder="请选择子表关联字段"
                  filterable
                  style="width: 100%;"
                >
                  <el-option
                    v-for="field in getVisibleChildFields(child.tableName)"
                    :key="`${index}-${field.field}`"
                    :label="`${field.label} (${field.field})`"
                    :value="field.field"
                  />
                </el-select>
              </el-form-item>

              <el-form-item label="展示字段">
                <div class="field-select-group">
                  <el-select
                    v-model="child.displayFields"
                    multiple
                    collapse-tags
                    collapse-tags-tooltip
                    placeholder="请选择子表展示字段"
                    filterable
                    style="width: 100%;"
                  >
                    <el-option
                      v-for="field in getVisibleChildFields(child.tableName)"
                      :key="`${index}-display-${field.field}`"
                      :label="`${field.label} (${field.field})`"
                      :value="field.field"
                    />
                  </el-select>
                  <el-button
                    plain
                    @click="selectAllChildDisplayFields(index)"
                    :disabled="getVisibleChildFields(child.tableName).length === 0"
                  >
                    全选所有字段
                  </el-button>
                </div>
              </el-form-item>

              <div class="child-sort-row">
                <el-form-item label="排序字段" class="child-sort-item">
                  <el-select
                    v-model="child.sortField"
                    clearable
                    placeholder="可选"
                    filterable
                    style="width: 100%;"
                  >
                    <el-option
                      v-for="field in getVisibleChildFields(child.tableName)"
                      :key="`${index}-sort-${field.field}`"
                      :label="`${field.label} (${field.field})`"
                      :value="field.field"
                    />
                  </el-select>
                </el-form-item>

                <el-form-item label="排序方向" class="child-sort-item">
                  <el-select v-model="child.sortOrder" style="width: 100%;">
                    <el-option label="降序" value="desc" />
                    <el-option label="升序" value="asc" />
                  </el-select>
                </el-form-item>
              </div>
            </div>
          </div>
        </el-form>
      </div>

      <template #footer>
        <el-button @click="createDialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="submitting" @click="submitCreate">
          创建并打开
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { ArrowLeft, Delete, Plus } from '@element-plus/icons-vue'
import { dataComboApi } from '../api'
import type {
  CreateDataComboChildInput,
  CreateDataComboInput,
  DataComboColumn,
  DataComboMetadataOption,
  DataComboSummary
} from '../types'

const router = useRouter()

const combos = ref<DataComboSummary[]>([])
const metadataOptions = ref<DataComboMetadataOption[]>([])
const loading = ref(false)
const optionsLoading = ref(false)
const createDialogVisible = ref(false)
const submitting = ref(false)

const form = reactive<CreateDataComboInput>({
  name: '',
  description: '',
  mainTableName: '',
  mainCodeField: '',
  mainDisplayFields: [],
  children: []
})

const metadataOptionMap = computed(() => {
  return new Map(metadataOptions.value.map((item) => [item.tableName, item]))
})

const mainTableFields = computed<DataComboColumn[]>(() => {
  return metadataOptionMap.value.get(form.mainTableName)?.fields || []
})

const mainDisplayFieldOptions = computed<DataComboColumn[]>(() => {
  return getVisibleFields(form.mainTableName)
})

function createEmptyChild(): CreateDataComboChildInput {
  return {
    tableName: '',
    codeField: '',
    displayFields: [],
    sortField: '',
    sortOrder: 'desc'
  }
}

function resetForm() {
  form.name = ''
  form.description = ''
  form.mainTableName = ''
  form.mainCodeField = ''
  form.mainDisplayFields = []
  form.children = [createEmptyChild()]
}

function goHome() {
  router.push('/')
}

function openCombo(id: string) {
  router.push(`/data-combos/${id}`)
}

function getOption(tableName: string) {
  return metadataOptionMap.value.get(tableName)
}

function getChildFields(tableName: string): DataComboColumn[] {
  return getOption(tableName)?.fields || []
}

function getVisibleFields(tableName: string): DataComboColumn[] {
  const option = getOption(tableName)
  if (!option) return []

  if (!option.defaultDisplayFields?.length) {
    return option.fields
  }

  const fieldMap = new Map(option.fields.map((field) => [field.field, field]))
  return option.defaultDisplayFields
    .map((fieldName) => fieldMap.get(fieldName))
    .filter((field): field is DataComboColumn => Boolean(field))
}

function getVisibleChildFields(tableName: string): DataComboColumn[] {
  return getVisibleFields(tableName)
}

function pickDefaultDisplayFields(option?: DataComboMetadataOption): string[] {
  if (!option) return []
  if (option.defaultDisplayFields?.length) {
    return [...option.defaultDisplayFields]
  }
  return option.fields.map((field) => field.field)
}

function handleMainTableChange(tableName: string) {
  const option = getOption(tableName)
  const recommendedCodeField = option?.recommendedCodeFields[0] || ''

  form.mainCodeField = recommendedCodeField
  form.mainDisplayFields = pickDefaultDisplayFields(option)
}

function selectAllMainDisplayFields() {
  form.mainDisplayFields = mainDisplayFieldOptions.value.map((field) => field.field)
}

function selectAllChildDisplayFields(index: number) {
  const child = form.children[index]
  if (!child) return

  child.displayFields = getVisibleChildFields(child.tableName).map((field) => field.field)
}

function handleChildTableChange(index: number) {
  const child = form.children[index]
  if (!child) return

  const option = getOption(child.tableName)
  if (!option) return

  child.codeField = option.recommendedCodeFields[0] || ''
  child.displayFields = pickDefaultDisplayFields(option)
  child.sortField = child.displayFields.find((field) => /date|time/i.test(field)) || ''
  child.sortOrder = 'desc'
}

function addChild() {
  form.children.push(createEmptyChild())
}

function removeChild(index: number) {
  form.children.splice(index, 1)
}

async function loadCombos() {
  loading.value = true

  try {
    const res = await dataComboApi.getList() as any
    combos.value = res.data || []
  } catch {
    ElMessage.error('加载关联视图失败')
  } finally {
    loading.value = false
  }
}

async function loadMetadataOptions() {
  optionsLoading.value = true

  try {
    const res = await dataComboApi.getMetadataOptions() as any
    metadataOptions.value = res.data || []
  } catch {
    ElMessage.error('加载配置项失败')
    throw new Error('load metadata options failed')
  } finally {
    optionsLoading.value = false
  }
}

async function openCreateDialog() {
  if (metadataOptions.value.length === 0) {
    await loadMetadataOptions()
  }

  resetForm()
  createDialogVisible.value = true
}

function validateForm(): string | null {
  if (!form.name.trim()) return '请填写组合名称'
  if (!form.mainTableName) return '请选择主表'
  if (!form.mainCodeField) return '请选择主表关联键'
  if (form.mainDisplayFields.length === 0) return '请至少选择一个主表展示字段'
  if (form.children.length === 0) return '请至少添加一个子表'

  for (let index = 0; index < form.children.length; index += 1) {
    const child = form.children[index]
    if (!child.tableName) return `请选择第 ${index + 1} 个子表`
    if (!child.codeField) return `请选择第 ${index + 1} 个子表关联键`
    if (child.displayFields.length === 0) return `请至少选择第 ${index + 1} 个子表的一个展示字段`
  }

  return null
}

async function submitCreate() {
  const errorMessage = validateForm()
  if (errorMessage) {
    ElMessage.warning(errorMessage)
    return
  }

  submitting.value = true

  try {
    const payload: CreateDataComboInput = {
      name: form.name.trim(),
      description: '',
      mainTableName: form.mainTableName,
      mainCodeField: form.mainCodeField,
      mainDisplayFields: [...form.mainDisplayFields],
      children: form.children.map((child) => ({
        tableName: child.tableName,
        codeField: child.codeField,
        displayFields: [...child.displayFields],
        sortField: child.sortField || '',
        sortOrder: child.sortOrder || 'desc'
      }))
    }

    const res = await dataComboApi.create(payload) as any
    createDialogVisible.value = false
    ElMessage.success(res.message || '关联视图创建成功')
    await loadCombos()
    if (res.data?.id) {
      router.push(`/data-combos/${res.data.id}`)
    }
  } catch (error: any) {
    ElMessage.error(error?.response?.data?.message || '创建关联视图失败')
  } finally {
    submitting.value = false
  }
}

async function handleDelete(combo: DataComboSummary) {
  try {
    await ElMessageBox.confirm(
      `确定要删除关联视图“${combo.name}”吗？`,
      '删除关联视图',
      {
        type: 'warning',
        confirmButtonText: '删除',
        cancelButtonText: '取消'
      }
    )

    await dataComboApi.delete(combo.id)
    ElMessage.success('关联视图已删除')
    await loadCombos()
  } catch (error: any) {
    if (error !== 'cancel') {
      ElMessage.error(error?.response?.data?.message || '删除关联视图失败')
    }
  }
}

onMounted(() => {
  loadCombos()
})
</script>

<style scoped>
.data-combo-list {
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

.page-actions {
  display: flex;
  align-items: center;
  gap: 10px;
}

.combo-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
  gap: 16px;
}

.combo-card {
  border-radius: 12px;
}

.card-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
}

.card-title-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 6px;
}

.card-header h3 {
  margin: 0;
  color: #303133;
}

.card-header p {
  margin: 0;
  color: #606266;
  line-height: 1.6;
}

.card-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.card-body {
  display: flex;
  flex-direction: column;
}

.meta-table {
  display: grid;
  border: 1px solid #ebeef5;
  border-radius: 10px;
  overflow: hidden;
}

.meta-row {
  display: grid;
  grid-template-columns: 88px minmax(0, 1fr);
}

.meta-row + .meta-row {
  border-top: 1px solid #ebeef5;
}

.meta-cell {
  min-width: 0;
  padding: 12px 14px;
}

.meta-cell-label {
  display: flex;
  align-items: center;
  justify-content: center;
  background: #f8fafc;
  color: #606266;
  font-weight: 600;
}

.meta-cell-value {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  background: #fff;
}

.child-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.field-select-group {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  width: 100%;
}

.field-select-group :deep(.el-select) {
  flex: 1;
}

.create-dialog-body {
  max-height: 70vh;
  overflow: auto;
  padding-right: 8px;
}

.form-section {
  margin-top: 22px;
  padding-top: 18px;
  border-top: 1px solid #ebeef5;
}

.section-title {
  margin-bottom: 16px;
  color: #303133;
  font-size: 15px;
  font-weight: 600;
}

.section-title-with-action {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.child-editor {
  margin-bottom: 16px;
  padding: 16px;
  border-radius: 10px;
  border: 1px solid #ebeef5;
  background: #fafcff;
}

.child-editor-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
  color: #303133;
  font-weight: 600;
}

.child-sort-row {
  display: flex;
  gap: 16px;
}

.child-sort-item {
  flex: 1;
}
</style>
