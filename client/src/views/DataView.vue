<template>
  <div class="data-view">
    <!-- 页面头部 -->
    <div class="header">
      <div class="header-left">
        <el-button @click="goBack" :icon="ArrowLeft">返回</el-button>
        <h2>{{ metadata?.cn_name || '数据详情' }}</h2>
      </div>
      <div class="header-right">
        <el-tag type="info">共 {{ total }} 条数据</el-tag>
        <el-button
          size="small"
          @click="downloadDataExport('csv')"
          :loading="exportDataLoading === 'csv'"
        >
          <el-icon><Download /></el-icon>
          导出 CSV
        </el-button>
        <el-button
          size="small"
          @click="downloadDataExport('json')"
          :loading="exportDataLoading === 'json'"
        >
          <el-icon><Download /></el-icon>
          导出 JSON
        </el-button>
        <el-button type="primary" @click="triggerUpdate" :loading="updating">
          <el-icon><Refresh /></el-icon>
          一键更新
        </el-button>
      </div>
    </div>

    <!-- 字段统计信息 -->
    <div class="field-stats">
      <el-tag type="primary" class="clickable-tag" @click="showFieldsDialog('接口字段', apiFields)">
        接口字段数: {{ apiFieldCount }}
      </el-tag>
      <el-tag type="success" class="clickable-tag" @click="showFieldsDialog('表字段', tableFields)">
        表字段数: {{ tableFieldCount }}
      </el-tag>
      <el-tag type="info">显示列数: {{ sortedColumns.length }}</el-tag>
      <el-tag v-if="hiddenColumns.length > 0" type="warning" class="clickable-tag" @click="showHiddenColumnsDialog">
        已隐藏: {{ hiddenColumns.length }}
      </el-tag>
      <el-tag v-if="apiFieldCount !== tableFieldCount" type="warning">字段不一致</el-tag>
      <el-tag v-else type="success">字段一致</el-tag>
      <el-button type="warning" size="small" @click="showRawFieldsDialog" :loading="rawFieldsLoading">
        <el-icon><View /></el-icon>
        查看官方API字段
      </el-button>
      <el-button v-if="hiddenColumns.length > 0" size="small" @click="showHiddenColumnsDialog" :loading="hiddenColumnsLoading">
        <el-icon><View /></el-icon>
        已隐藏的列 ({{ hiddenColumns.length }})
      </el-button>
      <el-button type="success" size="small" @click="exportFieldMappings" :loading="exportLoading">
        <el-icon><Download /></el-icon>
        导出映射
      </el-button>
      <el-button type="primary" size="small" @click="triggerImportFile">
        <el-icon><Upload /></el-icon>
        导入映射
      </el-button>
      <el-button type="danger" size="small" @click="deleteAllMappings" :loading="deleteLoading">
        <el-icon><Delete /></el-icon>
        删除映射
      </el-button>
      <el-button v-if="isRawTable" type="info" size="small" @click="showColumnOrderDialog">
        <el-icon><Rank /></el-icon>
        表头排序
      </el-button>
      <el-button v-if="isRawTable" type="warning" size="small" @click="showCrawlRulesDialog">
        <el-icon><InfoFilled /></el-icon>
        采集规则
      </el-button>
      <input 
        ref="importFileInput" 
        type="file" 
        accept=".json" 
        style="display: none" 
        @change="handleImportFile"
      />
    </div>

    <!-- 搜索框 -->
    <div class="search-box">
      <el-select
        v-model="searchColumn"
        placeholder="所有列"
        size="large"
        clearable
        style="width: 160px; margin-right: 8px;"
        @change="handleSearchColumnChange"
      >
        <el-option label="所有列" value="" />
        <el-option 
          v-for="col in sortedColumns" 
          :key="col.name" 
          :label="getSearchColumnLabel(col)" 
          :value="col.name" 
        />
      </el-select>
      <el-input
        v-model="searchKeyword"
        :placeholder="searchPlaceholder"
        size="large"
        clearable
        @keyup.enter="handleSearch"
        @clear="handleClearSearch"
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

    <!-- 字段列表弹窗 -->
    <el-dialog v-model="fieldsDialogVisible" :title="fieldsDialogTitle" width="600px">
      <el-table :data="fieldsDialogList" size="small" stripe max-height="400">
        <el-table-column type="index" label="#" width="50" />
        <el-table-column prop="name" label="字段名" width="180">
          <template #default="{ row }">
            <span class="field-name">{{ row.name }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="type" label="类型" width="100">
          <template #default="{ row }">
            <el-tag size="small" type="info">{{ row.type }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="description" label="描述" min-width="150" />
      </el-table>
    </el-dialog>

    <!-- 官方API原始字段弹窗 -->
    <el-dialog v-model="rawFieldsDialogVisible" title="官方API原始字段" width="900px">
      <div v-if="!rawFieldsSupported" class="raw-fields-unsupported">
        <el-alert type="warning" :closable="false" show-icon>
          {{ rawFieldsMessage }}
        </el-alert>
      </div>
      <div v-else>
        <div class="raw-fields-header">
          <el-tag type="info">共 {{ rawFieldsTotal }} 个字段</el-tag>
          <el-tag type="success">有效字段: {{ rawFieldsValidCount }}</el-tag>
          <el-tag type="warning">空值字段: {{ rawFieldsEmptyCount }}</el-tag>
          <el-input
            v-model="rawFieldsSearch"
            placeholder="搜索字段名或含义..."
            size="small"
            clearable
            style="width: 200px; margin-left: auto;"
          >
            <template #prefix>
              <el-icon><Search /></el-icon>
            </template>
          </el-input>
        </div>
        <el-table :data="filteredRawFields" size="small" stripe max-height="500" border>
          <el-table-column type="index" label="#" width="60" />
          <el-table-column prop="name" label="字段名" width="100" sortable>
            <template #default="{ row }">
              <span class="field-name">{{ row.name }}</span>
            </template>
          </el-table-column>
          <el-table-column prop="description" label="字段含义" width="180">
            <template #default="{ row }">
              <span :class="{ 'unknown-field': row.description === '(未知)' || !row.description }">
                {{ row.description || '-' }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="value" label="示例值" min-width="300">
            <template #default="{ row }">
              <span :class="{ 'empty-value': row.value === '(空)' }">
                {{ formatRawValue(row.value) }}
              </span>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </el-dialog>

    <!-- 字段映射编辑弹窗 -->
    <el-dialog v-model="fieldMappingDialogVisible" title="编辑字段中文名" width="450px">
      <el-form label-width="80px">
        <el-form-item label="字段名">
          <el-input :value="editingField" disabled />
        </el-form-item>
        <el-form-item label="中文名">
          <el-input 
            v-model="editingCnName" 
            placeholder="请输入字段的中文名称"
            clearable
            @keyup.enter="saveFieldMapping"
          />
        </el-form-item>
        <el-form-item label="单位">
          <el-input 
            v-model="editingUnit" 
            placeholder="如：元、%、手、股"
            clearable
            style="width: 150px"
          />
          <span class="unit-hint">设置后会显示在表头中文名后面</span>
        </el-form-item>
        <!-- 系统推断提示 -->
        <el-form-item v-if="getFieldSuggestion(editingField)" label="系统推断">
          <div class="field-suggestion">
            <el-tag type="info" size="small">{{ getFieldSuggestion(editingField) }}</el-tag>
            <el-button 
              type="primary" 
              link 
              size="small" 
              @click="editingCnName = getFieldSuggestion(editingField) || ''"
            >
              使用此名称
            </el-button>
          </div>
        </el-form-item>
        <el-form-item v-else label="系统推断">
          <span class="no-suggestion">暂无推断结果</span>
        </el-form-item>
        <!-- 值分布分析 -->
        <el-form-item label="值分布">
          <div v-if="fieldValuesLoading" class="field-values-loading">
            <el-icon class="is-loading"><Loading /></el-icon>
            <span>分析中...</span>
          </div>
          <div v-else-if="fieldValuesData" class="field-values-analysis">
            <div class="values-summary">
              <el-tag size="small" type="info">{{ fieldValuesData.uniqueCount }} 个不同值</el-tag>
              <span class="values-hint-click">点击值可筛选列表</span>
            </div>
            <div v-if="fieldValuesData.uniqueCount <= 3" class="values-list">
              <div v-for="item in fieldValuesData.topValues" :key="item.value" class="value-item clickable" @click="filterByFieldValue(item.value)">
                <el-tag size="small" :type="fieldValuesData.uniqueCount === 1 ? 'success' : 'warning'" class="clickable-value-tag">
                  {{ formatFieldValue(item.value) }}
                </el-tag>
                <span class="value-count">({{ item.count }} 条)</span>
              </div>
            </div>
            <div v-else class="values-list">
              <div class="values-hint">前 {{ Math.min(fieldValuesData.topValues.length, 5) }} 个最常见值：</div>
              <div v-for="item in fieldValuesData.topValues.slice(0, 5)" :key="item.value" class="value-item clickable" @click="filterByFieldValue(item.value)">
                <el-tag size="small" type="info" class="clickable-value-tag">{{ formatFieldValue(item.value) }}</el-tag>
                <span class="value-count">({{ item.count }} 条)</span>
              </div>
            </div>
          </div>
          <div v-else class="no-suggestion">暂无数据</div>
        </el-form-item>
        <!-- 数值格式化开关 -->
        <el-form-item label="数值格式">
          <div class="format-switch">
            <el-switch 
              v-model="editingFormatAsWanYi" 
              active-text="万/亿"
              inactive-text="原始值"
            />
            <span class="format-hint">开启后，大数值将显示为万/亿格式（如 12.69亿、3856.03万）</span>
          </div>
        </el-form-item>
        <!-- 时间戳转换开关 -->
        <el-form-item label="时间戳">
          <div class="format-switch">
            <el-switch 
              v-model="editingFormatAsTimestamp" 
              active-text="转换"
              inactive-text="原始值"
            />
            <span class="format-hint">开启后，Unix时间戳将转换为日期时间格式</span>
          </div>
        </el-form-item>
        <!-- 长文本折叠开关 -->
        <el-form-item label="长文本">
          <div class="format-switch">
            <el-switch 
              v-model="editingTextClamp" 
              active-text="2行折叠"
              inactive-text="不折叠"
            />
            <span class="format-hint">开启后，该列在表格中显示2行省略，悬停看原文，点击单元格弹窗查看全文</span>
          </div>
        </el-form-item>
        <!-- 值映射配置（当唯一值数量 <= 10 时显示） -->
        <el-form-item v-if="fieldValuesData && fieldValuesData.uniqueCount <= 10 && fieldValuesData.uniqueCount > 0" label="值映射">
          <div class="value-mapping-config">
            <div class="value-mapping-hint">将原始值映射为中文显示（留空则显示原始值）</div>
            <div class="value-mapping-list">
              <div v-for="item in fieldValuesData.topValues" :key="item.value" class="value-mapping-item">
                <el-tag size="small" type="info" class="original-value">{{ formatFieldValue(item.value) }}</el-tag>
                <span class="mapping-arrow">→</span>
                <el-input 
                  v-model="editingValueMapping[String(item.value)]" 
                  placeholder="中文名称"
                  size="small"
                  style="width: 120px"
                />
                <span class="value-count">({{ item.count }} 条)</span>
              </div>
            </div>
          </div>
        </el-form-item>
      </el-form>
      <template #footer>
        <div class="dialog-footer">
          <el-button 
            type="danger" 
            plain 
            @click="toggleColumnHidden(editingField, true)" 
            :loading="hidingColumn"
          >
            <el-icon><Hide /></el-icon>
            隐藏该列
          </el-button>
          <div class="footer-right">
            <el-button @click="fieldMappingDialogVisible = false">取消</el-button>
            <el-button type="primary" @click="saveFieldMapping" :loading="savingMapping">保存</el-button>
          </div>
        </div>
      </template>
    </el-dialog>

    <!-- 全文查看弹窗 -->
    <el-dialog v-model="fullTextDialogVisible" :title="fullTextDialogTitle" width="760px">
      <div class="full-text-dialog-content">{{ fullTextDialogContent }}</div>
    </el-dialog>

    <!-- 已隐藏的列弹窗 -->
    <el-dialog v-model="hiddenColumnsDialogVisible" title="已隐藏的列" width="800px">
      <div v-if="hiddenColumnsLoading" class="hidden-columns-loading">
        <el-icon class="is-loading"><Loading /></el-icon>
        <span>加载中...</span>
      </div>
      <div v-else-if="hiddenColumnsDetail.length === 0" class="hidden-columns-empty">
        <el-empty description="没有隐藏的列" />
      </div>
      <el-table v-else :data="hiddenColumnsDetail" size="small" stripe border max-height="400">
        <el-table-column prop="fieldName" label="字段名" width="100">
          <template #default="{ row }">
            <span class="field-name">{{ row.fieldName }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="cnName" label="中文名" width="120">
          <template #default="{ row }">
            <span v-if="row.cnName">{{ row.cnName }}</span>
            <span v-else class="no-cn-name">-</span>
          </template>
        </el-table-column>
        <el-table-column label="系统推断" width="150">
          <template #default="{ row }">
            <el-tag v-if="getFieldSuggestion(row.fieldName)" size="small" type="info">
              {{ getFieldSuggestion(row.fieldName) }}
            </el-tag>
            <span v-else class="no-suggestion">-</span>
          </template>
        </el-table-column>
        <el-table-column label="值分布" min-width="200">
          <template #default="{ row }">
            <div class="hidden-column-values">
              <el-tag size="small" type="info">{{ row.uniqueCount }} 个不同值</el-tag>
              <span v-if="row.topValues.length > 0" class="top-values">
                <span v-for="(item, index) in row.topValues.slice(0, 3)" :key="index" class="top-value-item">{{ formatFieldValue(item.value) }}<span class="value-count">({{ item.count }})</span>{{ index < Math.min(row.topValues.length, 3) - 1 ? ', ' : '' }}</span>
              </span>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="100" fixed="right">
          <template #default="{ row }">
            <el-button 
              type="primary" 
              size="small" 
              @click="showHiddenColumn(row.fieldName)"
              :loading="showingColumn === row.fieldName"
            >
              显示
            </el-button>
          </template>
        </el-table-column>
      </el-table>
      <template #footer v-if="hiddenColumnsDetail.length > 0">
        <div class="hidden-columns-footer">
          <el-button type="primary" @click="showAllHiddenColumns" :loading="showingAllColumns">
            <el-icon><View /></el-icon>
            一键全显示 ({{ hiddenColumnsDetail.length }})
          </el-button>
          <el-button @click="hiddenColumnsDialogVisible = false">关闭</el-button>
        </div>
      </template>
    </el-dialog>

    <!-- 表头排序弹窗 -->
    <el-dialog v-model="columnOrderDialogVisible" title="表头排序" width="600px">
      <div class="column-order-hint">
        <el-icon><InfoFilled /></el-icon>
        <span>点击选择行，Shift+点击范围选择，Ctrl+点击多选，拖动选中的行调整顺序</span>
      </div>
      <div class="column-order-actions" v-if="selectedColumnIndices.size > 0">
        <el-tag type="primary">已选择 {{ selectedColumnIndices.size }} 项</el-tag>
        <el-button size="small" @click="clearColumnSelection">清除选择</el-button>
      </div>
      <el-table 
        ref="columnOrderTableRef"
        :data="columnOrderList" 
        size="small" 
        stripe 
        border 
        max-height="500"
        row-key="fieldName"
        class="column-order-table"
        :row-class-name="getColumnOrderRowClass"
        @row-click="handleColumnOrderRowClick"
      >
        <el-table-column width="50" align="center">
          <template #default>
            <el-icon class="drag-handle"><Rank /></el-icon>
          </template>
        </el-table-column>
        <el-table-column type="index" label="#" width="60" />
        <el-table-column prop="fieldName" label="字段名" width="100">
          <template #default="{ row }">
            <span class="field-name">{{ row.fieldName }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="cnName" label="中文名" min-width="150">
          <template #default="{ row }">
            <span v-if="row.cnName">{{ row.cnName }}</span>
            <span v-else class="no-cn-name">-</span>
          </template>
        </el-table-column>
        <el-table-column label="系统推断" width="150">
          <template #default="{ row }">
            <el-tag v-if="getFieldSuggestion(row.fieldName)" size="small" type="info">
              {{ getFieldSuggestion(row.fieldName) }}
            </el-tag>
            <span v-else class="no-suggestion">-</span>
          </template>
        </el-table-column>
      </el-table>
      <template #footer>
        <div class="column-order-footer">
          <el-button type="primary" @click="sortColumnsByFieldNumber" :loading="sortingByFieldNumber">
            <el-icon><Sort /></el-icon>
            按字段编号排序 (f1, f2, ... f300)
          </el-button>
          <el-button @click="columnOrderDialogVisible = false">关闭</el-button>
        </div>
      </template>
    </el-dialog>

    <!-- 采集规则弹窗 -->
    <el-dialog v-model="crawlRulesDialogVisible" title="采集规则说明" width="700px">
      <div class="crawl-rules-content">
        <h4>数据来源</h4>
        <p>东方财富行情中心 A 股列表 API，采集沪深京三市所有 A 股股票的实时行情数据。</p>
        
        <h4>API 筛选参数 (fs)</h4>
        <el-table :data="fsParamsData" size="small" stripe border>
          <el-table-column prop="param" label="参数" width="180" />
          <el-table-column prop="meaning" label="含义" />
        </el-table>
        
        <h4 style="margin-top: 20px;">f1 字段说明（证券类型）</h4>
        <el-table :data="f1FieldData" size="small" stripe border>
          <el-table-column prop="value" label="f1 值" width="80" />
          <el-table-column prop="meaning" label="含义" width="200" />
          <el-table-column prop="status" label="采集情况" />
        </el-table>
        
        <h4 style="margin-top: 20px;">过滤规则</h4>
        <el-alert type="info" :closable="false" show-icon>
          <template #title>
            采集时会自动过滤掉 <strong>f1=3</strong> 的记录（北交所定向转让股票，代码以 810 开头），只保留普通 A 股股票。
          </template>
        </el-alert>
        
        <h4 style="margin-top: 20px;">f13 字段说明（市场代码）</h4>
        <el-table :data="f13FieldData" size="small" stripe border>
          <el-table-column prop="value" label="f13 值" width="80" />
          <el-table-column prop="meaning" label="含义" />
        </el-table>
      </div>
      <template #footer>
        <el-button @click="crawlRulesDialogVisible = false">关闭</el-button>
      </template>
    </el-dialog>

    <!-- 数据表格 -->
    <el-table 
      ref="tableRef"
      :key="tableKey"
      :data="dataList" 
      v-loading="loading || !pageReady" 
      stripe 
      border
      style="width: 100%"
      :row-key="getRowKey"
      @expand-change="handleExpandChange"
    >
      <!-- 展开列（仅大宗交易明细显示） -->
      <el-table-column v-if="isDzjyDetail" type="expand">
        <template #default="{ row }">
          <div class="expand-content">
            <div v-if="expandLoading[getRowKey(row)]" class="expand-loading">
              <el-icon class="is-loading"><Loading /></el-icon>
              加载中...
            </div>
            <div v-else-if="expandData[getRowKey(row)]?.length > 0">
              <el-table :data="expandData[getRowKey(row)]" size="small" border>
                <el-table-column prop="F002V" label="买方营业部" min-width="250" />
                <el-table-column prop="F003V" label="卖方营业部" min-width="250" />
                <el-table-column prop="F005N" label="成交数量(万股)" width="130" align="right">
                  <template #default="{ row: detailRow }">
                    <span class="value-number">{{ formatNumber(detailRow.F005N) }}</span>
                  </template>
                </el-table-column>
                <el-table-column prop="F006N" label="成交金额(万元)" width="130" align="right">
                  <template #default="{ row: detailRow }">
                    <span class="value-number">{{ formatNumber(detailRow.F006N) }}</span>
                  </template>
                </el-table-column>
                <el-table-column prop="F004N" label="成交价格(元/股)" width="130" align="right">
                  <template #default="{ row: detailRow }">
                    <span class="value-number">{{ formatNumber(detailRow.F004N) }}</span>
                  </template>
                </el-table-column>
              </el-table>
            </div>
            <div v-else class="expand-empty">
              暂无细项数据
            </div>
          </div>
        </template>
      </el-table-column>
      
      <!-- 动态列（只有页面准备好后才渲染，避免闪烁） -->
      <template v-if="pageReady">
        <el-table-column 
          v-for="col in sortedColumns" 
          :key="col.name"
          :prop="col.name"
          :min-width="getColumnWidth(col)"
        >
          <template #header>
            <div class="custom-header">
              <div class="header-center-group">
                <el-icon 
                  v-if="isEditableField(col.name)" 
                  class="header-edit-icon header-side-icon header-side-icon-left"
                  @click.stop="openFieldMappingDialog(col.name)"
                  title="编辑中文名"
                ><Edit /></el-icon>
                <div class="header-text">
                  <span v-if="getColumnCnName(col)" class="header-cn-name">{{ getColumnCnName(col) }}</span>
                  <span class="header-field-name" :class="{ 'has-cn-name': getColumnCnName(col) }">{{ getColumnFieldName(col) }}</span>
                </div>
                <el-icon 
                  class="header-sort-icon header-side-icon header-side-icon-right"
                  :class="{ 
                    'is-active': sortField === col.name,
                    'is-asc': sortField === col.name && sortOrder === 'ascending',
                    'is-desc': sortField === col.name && sortOrder === 'descending'
                  }"
                  @click.stop="handleHeaderSort(col.name)"
                  title="点击排序"
                >
                  <template v-if="sortField === col.name && sortOrder === 'ascending'"><SortUp /></template>
                  <template v-else-if="sortField === col.name && sortOrder === 'descending'"><SortDown /></template>
                  <template v-else><Sort /></template>
                </el-icon>
              </div>
            </div>
          </template>
          <template #default="{ row }">
            <span
              :class="[
                getValueClass(row[col.name], col.type),
                {
                  'cell-content-clamp': shouldClampCell(col.name, row[col.name]),
                  'cell-content-expandable': shouldClampCell(col.name, row[col.name])
                }
              ]"
              :title="shouldClampCell(col.name, row[col.name]) ? String(row[col.name] ?? '') : ''"
              @click="handleCellContentClick(col.name, row[col.name])"
              v-html="highlightText(formatValue(row[col.name], col.type, col.name))"
            ></span>
          </template>
        </el-table-column>
      </template>
    </el-table>

    <!-- 分页 -->
    <div class="pagination">
      <el-pagination
        v-model:current-page="currentPage"
        v-model:page-size="pageSize"
        :page-sizes="[10, 20, 50, 100]"
        :total="total"
        layout="total, sizes, prev, pager, next, jumper"
        @size-change="handleSizeChange"
        @current-change="handlePageChange"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, computed, nextTick, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { ArrowLeft, Refresh, Loading, Search, View, Edit, Sort, SortUp, SortDown, Download, Upload, Delete, Hide, Rank, InfoFilled } from '@element-plus/icons-vue'
import { metadataApi, dataApi, updateApi, fieldMappingApi } from '../api'
import { onMessage } from '../utils/websocket'
import { notifyFieldDisplayConfigChanged } from '../utils/fieldDisplaySync'
import Sortable from 'sortablejs'
import type { ApiMetadata, OutputConfig } from '../types'

const route = useRoute()
const router = useRouter()

// WebSocket 消息取消订阅函数
let unsubscribe: (() => void) | null = null

// Sortable 实例
let columnSortable: Sortable | null = null

// 数据状态
const metadata = ref<ApiMetadata | null>(null)
const dataList = ref<any[]>([])
const columns = ref<OutputConfig[]>([])
const loading = ref(false)
const updating = ref(false)

// 页面初始化状态（用于控制表格显示时机）
const pageReady = ref(false)

// 字段信息类型
interface FieldInfo {
  name: string
  type: string
  description: string
}

// 字段统计
const apiFieldCount = ref(0)
const tableFieldCount = ref(0)
const apiFields = ref<FieldInfo[]>([])
const tableFields = ref<FieldInfo[]>([])

// 字段列表弹窗
const fieldsDialogVisible = ref(false)
const fieldsDialogTitle = ref('')
const fieldsDialogList = ref<FieldInfo[]>([])

// 官方API原始字段弹窗
const rawFieldsDialogVisible = ref(false)
const rawFieldsLoading = ref(false)
const rawFieldsSupported = ref(true)
const rawFieldsMessage = ref('')
const rawFieldsTotal = ref(0)
const rawFieldsList = ref<{ name: string; value: any; description: string }[]>([])
const rawFieldsSearch = ref('')

// 字段映射编辑弹窗
const fieldMappingDialogVisible = ref(false)
const editingField = ref('')
const editingCnName = ref('')
const editingUnit = ref('')  // 字段单位
const editingFormatAsWanYi = ref(false)  // 是否启用万/亿格式化
const editingFormatAsTimestamp = ref(false)  // 是否启用时间戳转换
const editingTextClamp = ref(false)  // 是否启用长文本2行折叠
const editingValueMapping = ref<Record<string, string>>({})  // 值映射配置
const fieldMappings = ref<Record<string, string>>({})
const fieldFormatSettings = ref<Record<string, boolean>>({})  // 字段格式化设置
const fieldTimestampSettings = ref<Record<string, boolean>>({})  // 字段时间戳转换设置
const fieldValueMappings = ref<Record<string, Record<string, string>>>({})  // 字段值映射设置
const fieldUnitSettings = ref<Record<string, string>>({})  // 字段单位设置
const fieldTextClampSettings = ref<Record<string, boolean>>({})  // 字段长文本折叠设置
const savingMapping = ref(false)

// 全文查看弹窗
const fullTextDialogVisible = ref(false)
const fullTextDialogTitle = ref('')
const fullTextDialogContent = ref('')

// 字段值分布分析
const fieldValuesLoading = ref(false)
const fieldValuesData = ref<{
  totalRows: number
  uniqueCount: number
  topValues: { value: any; count: number }[]
} | null>(null)

// 导出导入状态
const exportLoading = ref(false)
const exportDataLoading = ref<'' | 'csv' | 'json'>('')
const importFileInput = ref<HTMLInputElement | null>(null)
const deleteLoading = ref(false)

// 隐藏列状态
const hiddenColumns = ref<string[]>([])
const hidingColumn = ref(false)

// 隐藏列弹窗状态
const hiddenColumnsDialogVisible = ref(false)
const hiddenColumnsLoading = ref(false)
const hiddenColumnsDetail = ref<Array<{
  fieldName: string
  cnName: string
  uniqueCount: number
  topValues: Array<{ value: any; count: number }>
}>>([])
const showingColumn = ref<string | null>(null)
const showingAllColumns = ref(false)

// 表头排序弹窗状态
const columnOrderDialogVisible = ref(false)
const columnOrderTableRef = ref<any>(null)
const columnOrderList = ref<Array<{
  fieldName: string
  cnName: string
}>>([])
let columnOrderSortable: Sortable | null = null

// 表头排序多选状态
const selectedColumnIndices = ref<Set<number>>(new Set())
const lastClickedColumnIndex = ref<number | null>(null)

// 按字段编号排序状态
const sortingByFieldNumber = ref(false)

// 采集规则弹窗状态
const crawlRulesDialogVisible = ref(false)

// 采集规则数据
const fsParamsData = [
  { param: 'm:0+t:6', meaning: '深市主板' },
  { param: 'm:0+t:80', meaning: '深市创业板' },
  { param: 'm:1+t:2', meaning: '沪市主板' },
  { param: 'm:1+t:23', meaning: '沪市科创板' },
  { param: 'm:0+t:81+s:2048', meaning: '北交所（包含定向转让股票）' }
]

const f1FieldData = [
  { value: '2', meaning: '普通股票', status: '采集（沪深A股、B股、北交所普通股）' },
  { value: '3', meaning: '特殊品种', status: '过滤掉（北交所定向转让股票，代码810开头）' }
]

const f13FieldData = [
  { value: '0', meaning: '深市（股票代码 000xxx, 002xxx, 300xxx, 83xxxx, 92xxxx）' },
  { value: '1', meaning: '沪市（股票代码 600xxx, 601xxx, 603xxx, 605xxx, 688xxx）' }
]

// 显示采集规则弹窗
function showCrawlRulesDialog() {
  crawlRulesDialogVisible.value = true
}

// 系统推断的字段含义映射（来自东方财富行情中心表头验证）
const fieldMeanings: Record<string, string> = {
  // 基础信息
  f1: '证券类型(2=A股,3=定向转让)',
  f12: '股票代码',
  f13: '市场代码',
  f14: '股票名称',
  // 价格相关
  f2: '最新价(元)',
  f4: '涨跌额(元)',
  f15: '最高价',
  f16: '最低价',
  f17: '开盘价(今开)',
  f18: '昨收价',
  // 涨跌幅相关
  f3: '涨跌幅(%)',
  f7: '振幅(%)',
  // 板块分类
  f19: '板块类型代码(2=沪市主板,6=深市主板,23=科创板,80=创业板,81=北交所)',
  f20: '总市值(元)',
  f21: '流通市值(元)',
  f22: '涨速(%)',
  // 成交相关
  f5: '成交量(手)',
  f6: '成交额(元)',
  f8: '换手率(%)',
  f10: '量比',
  f11: '5分钟涨跌(%)',
  // 估值相关
  f9: '市盈率(动态)',
  f23: '市净率',
  f24: '年初至今涨跌幅(%)',
  f25: '近10日涨跌幅(%)',
  f26: '上市日期(YYYYMMDD)',
  f27: '市场代码(同f13,0=深市,1=沪市)',
  f28: '预留字段(无数据)',
  f29: '特别表决权标识(1=普通股,512=WD特别表决权)',
  f30: '尾盘成交量(手,15:00:00最后一笔成交量,正=买入,负=卖出)',
  f31: '买一价格(元)',
  f32: '卖一价格(元)',
  f33: '委比(%,买卖盘力量对比,(委买-委卖)/(委买+委卖)×100)',
  f34: '外盘(手,主动买入成交量,以卖一价或更高价成交)',
  f35: '内盘(手,主动卖出成交量,以买一价或更低价成交)',
  f36: '成交笔数(笔,当日成交总笔数,成交量的细分统计)',
  f37: 'ROE(%,净资产收益率,净利润/净资产×100)',
  f38: '总股本(股)',
  f39: '流通股本(股)',
  f40: '营业收入(元)',
  f41: '营业总收入同比增长率(%,最新报告期)',
  f42: '营业利润(元)',
  f43: '投资收益(元)',
  f44: '利润总额(元)',
  f45: '净利润(元)',
  f46: '净利润同比增长率(%)',
  f47: '未分配利润(元)',
  f48: '每股未分配利润(元/股)',
  f49: '毛利率(%)',
  f50: '总资产(元)',
  f51: '流动资产合计(元)',
  f52: '固定资产(元)',
  f53: '无形资产(元)',
  f54: '负债合计(元)',
  f55: '总资产(元)',
  f56: '非流动负债合计(元)',
  f57: '资产负债率(%)',
  f58: '股东权益合计(元,净资产=资产总计-负债合计)',
  f59: '权益比率(%,股东权益占总资产比例=100-资产负债率)',
  f60: '资本公积(元)',  // 资产负债表中的资本公积,验证通过(96%匹配率)
  f61: '每股公积金(元/股)',  // 每股资本公积 = f60/f38, 验证通过(100%匹配率)
  // 板块信息
  f100: '所属行业(行业板块分类,如:银行、房地产开发、软件开发等)',
  f101: '所属行业领涨股(当日该行业涨幅最高的股票,准确率85%)',
  f102: '地域板块',
  // 交易状态
  f103: '股票概念标签/主题分类(多个概念用逗号分隔)',
  f104: '预留字段(无数据)',
  f105: '预留字段(无数据)',
  f106: '预留字段(无数据)',
  f107: '预留字段(固定值5)',
  f108: '预留字段(无数据)',
  f109: '未知字段(待验证)',
  f110: '近20日涨跌幅(%)',
  f111: '股票状态位标志(位标志组合:Bit17=科创板/创业板,Bit19=主板,Bit21=未盈利-U,Bit23=特殊标记-UW,0=退市/停牌)',
  f112: '第三季度每股收益(元/股)',
  f113: '每股净资产(元/股)',  // 验证通过:在个股行情页→公司核心数据栏→第2行显示,已验证5只股票均完全匹配
  f114: '市盈率(静态,PE(静),倍)',  // 官网估值分析页面验证: 表格列PE(静)
  f115: '市盈率(TTM,PE(TTM),倍)',  // 官网估值分析页面验证: 表格列PE(TTM)
  f116: 'GDR最新价(英镑)',
  f117: 'GDR最新价(英镑)-冗余字段',
  f118: 'GDR涨跌幅(%)',
  f119: 'A股/GDR溢价率(%)',
  f120: 'GDR存托凭证标识(0=无GDR,3=已发行GDR)',
  f121: 'GDR证券代码',
  f122: 'GDR市场代码(156=伦敦证交所,-1=无)',
  f123: 'GDR中文简称',
  f124: '数据更新时间(Unix时间戳,秒)',
  f125: '交易状态(0=已收盘/正常,1=退市,2=停牌,4=未上市)',
  f126: '预留字段(无数据)',
  f127: '3日涨跌幅(%)',
  f128: '预留字段(值为null)',
  f129: '净利率(%)',
  f130: '市销率(倍)',
  f131: '市现率(倍)',
  f132: '营业收入TTM(元)',
  f133: '股息率TTM(%)',
  f135: '股东权益合计(元)',
  f134: '预留字段(无数据)',
  f136: '预留字段(无数据)',
  f137: '预留字段(无数据)',
  f138: '预留字段(无数据)',
  f139: '板块类型标志',
  f140: '预留字段(API不返回)',
  f141: '预留字段(API不返回)',
  f142: '买二价',
  f143: '卖二价',
  f144: '最新价(元)',
  f145: '北交所标识(2=北交所股票)',
  f146: '行业领涨股代码',
  f147: '市场分类标识(0=深市相关,1=沪市相关)',
  f148: '股票属性位标志',
  f149: '年初至今涨跌幅(%,⚠️与f25数据不匹配,需人工验证)',
  f152: '预留字段(固定值2)',
  f153: '预留字段(固定值3)',
  f154: '预留字段(固定值4)',
  // 资金流向(今日实时)
  f62: '今日主力净流入(元)',
  f63: '未知字段(待验证-无对应页面显示)',
  f64: '超大单流入(元)',
  f65: '超大单流出(元)',
  f66: '超大单净流入(元)',
  f67: '超大单流入占比(%)',
  f68: '超大单流出占比(%)',
  f69: '超大单净占比(%)',
  f70: '大单流入(元)',
  f71: '大单流出(元)',
  f72: '大单净流入(元)',
  f73: '大单流入占比(%)',
  f74: '大单流出占比(%)',
  f75: '大单净占比(%)',
  f76: '中单流入(元)',
  f77: '中单流出(元)',
  f78: '中单净流入(元)',
  f79: '中单流入占比(%)',
  f80: '中单流出占比(%)',
  f81: '中单净占比(%)',
  f82: '小单流入(元)',
  f83: '小单流出(元)',
  f84: '小单净流入(元)',
  f85: '小单流入占比(%)',
  f86: '小单流出占比(%)',
  f87: '小单净占比(%)',
  f88: '未知字段(待验证)',
  f89: '未知字段(待验证)',
  f90: '未知字段(待验证)',
  f91: '未知字段(待验证)',
  f92: '未知字段(待验证)',
  f94: '未知字段(待验证)',
  f95: '总资产收益率(加权)(%,ROA,净利润/平均总资产×100)',
  f97: '未知字段(待验证)',
  f98: '未知字段(待验证)',
  f99: '未知字段(待验证)',
  f160: '超大单净流入(亿元)',
  f161: '预留字段(无数据)',
  f162: '预留字段(无数据)',
  f163: '预留字段(无数据)',
  f164: '5日主力净流入(元)',
  f165: '5日主力净占比(%)',
  f166: '5日超大单净流入(元)',
  f167: '5日超大单净占比(%)',
  f168: '5日大单净流入(元)',
  f169: '5日大单净占比(%)',
  f170: '5日中单净流入(元)',
  f171: '5日中单净占比(%)',
  f172: '5日小单净流入(元)',
  f173: '5日小单净占比(%)',
  f174: '10日主力净流入(元)',
  f175: '10日主力净占比(%)',
  f176: '10日超大单净流入(元)',
  f177: '10日超大单净占比(%)',
  f178: '10日大单净流入(元)',
  f179: '10日大单净占比(%)',
  f180: '10日中单净流入(元)',
  f181: '10日中单净占比(%)',
  f182: '10日小单净流入(元)',
  f183: '10日小单净占比(%)',
  f184: '主力净占比(%)',
  f185: '对应H股昨收价(港币)',
  f186: '对应H股最新价(港币)',
  f187: '对应H股涨跌幅(%)',
  f188: 'A/H溢价率(%)',
  f189: 'A/H比价',
  f190: 'A+H股标识(0=无H股,3=有H股)',
  f191: '对应H股代码',
  f192: 'A+H股市场标识(-1=无H股,116=有H股)',
  f193: '对应H股中文名称',
  f194: '预留字段(无数据)',
  f195: '对应B股昨收价',
  f196: '对应B股最新价',
  f197: '对应B股涨跌幅(%)',
  f199: 'A/B股比价',
  f200: 'B股市场标识(0=无B股,2=深市B股,3=沪市B股)',
  f201: '对应B股代码',

  // A股公告（东方财富原始）字段
  art_code: '公告编码(唯一标识,格式:AN+年月日+时间戳)',
  title: '公告标题(完整标题)',
  title_ch: '公告标题(中文)',
  title_en: '公告标题(英文)',
  notice_date: '公告日期(YYYY-MM-DD)',
  display_time: '显示时间(页面展示时间,精确到毫秒)',
  ei_time: '发布时间(EI系统时间,精确到秒)',
  sort_date: '排序日期(用于列表排序,统一为当日12:00:00)',
  stock_code: '股票代码(6位数字或A+5位数字)',
  stock_name: '股票简称',
  market_code: '市场代码(0=深市,1=沪市)',
  ann_type: '公告类型(A=A股,INV=投资者关系,WA=未上市)',
  column_code: '栏目代码(多级分类编码)',
  column_name: '栏目名称(公告分类名称)',
  source_type: '来源类型(321=深交所,324=投资者关系,数字编码)',
  language: '语言标识(0=中文)',
  listing_state: '上市状态(0=已上市,9=未上市)',
  product_code: '产品代码(预留字段)',
  pdf_url: 'PDF链接(公告PDF文件下载地址)',
  f202: 'B股市场标识(-1=无B股,0=深市B股,1=沪市B股)',
  f203: '对应B股名称',
  f204: '预留字段(值为null)',
  f205: '预留字段(值为null)',
  f206: '预留字段(值为null)',
  f207: '预留字段(值为null)',
  f208: '预留字段(值为null)',
  f209: '预留字段(值为null)',
  f210: '预留字段(无数据)',
  f211: '买一量(手,盘口五档)',
  f212: '卖一量(手,盘口五档)',
  f213: '预留字段(值为空)',
  f214: '预留字段(值为-1)',
  f215: '预留字段(值为空)',
  f216: '预留字段(无数据)',
  f217: '预留字段(无数据)',
  f218: '预留字段(无数据)',
  f219: '预留字段(无数据)',
  f220: '预留字段(无数据)',
  f221: '财报日期(YYYYMMDD,最新报告期截止日期)',
  f222: '预留字段(无数据)',
  f223: '股票列表序号(按股票代码升序,内部字段)',
  f225: '主力净占比排名(按f184降序)',
  f226: '主力净占比排名变化(昨日排名-今日排名,正值=进步,负值=退步)',
  f227: '预留字段(无数据)',
  f228: '可转债昨收价',
  f229: '可转债现价',
  f230: '可转债涨跌幅(%)',
  f231: '可转债标识(0=无,3=有)',
  f232: '可转债代码',
  f233: '可转债市场标识(-1=无,0=深市,1=沪市)',
  f234: '可转债简称',
  f235: '预留字段(无数据)',
  f236: '预留字段(无数据)',
  f237: '预留字段(无数据)',
  f238: '预留字段(无数据)',
  f239: '预留字段(无数据)',
  f240: '预留字段(无数据)',
  f241: '预留字段(无数据)',
  f242: '预留字段(无数据)',
  f243: '预留字段(无数据)',
  f244: '预留字段(无数据)',
  f245: '预留字段(无数据)',
  f246: '预留字段(无数据)',
  f247: '预留字段(无数据)',
  f248: '预留字段(无数据)',
  f249: '预留字段(无数据)',
  f250: '预留字段(无数据)',
  f251: '预留字段(无数据)',
  f252: '20日主力净流入累计(元)',
  f253: '20日超大单净流入累计(元)',
  f254: '20日大单净流入累计(元)',
  f255: '20日中单净流入累计(元)',
  f256: '20日小单净流入累计(元)',
  f257: '预留字段(值为null)',
  f258: '预留字段(值为null)',
  f259: '预留字段(值为null)',
  f260: '预留字段(值为null)',
  f261: '预留字段(值为null)',
  f262: '预留字段(值为null)',
  f263: '3日主力净占比排名(按f268降序)',
  f264: '3日主力净占比昨日排名(API内部字段,推测值,官网不显示)',
  f265: '主板块代码',
  f266: '交易状态(90=正常,-1=停牌/退市)',
  f267: '3日主力净流入(元)',
  f268: '3日主力净占比(%)',
  f269: '3日超大单净流入(元)',
  f270: '3日超大单净占比(%)',
  f271: '3日大单净流入(元)',
  f272: '3日大单净占比(%)',
  f273: '3日中单净流入(元)',
  f274: '3日中单净占比(%)',
  f275: '3日小单净流入(元)',
  f276: '3日小单净占比(%)',
  f277: '最新价(元)',
  f278: '5日主力净流入(元)-冗余字段',
  f279: '5日超大单净流入(元)-冗余字段',
  f280: '5日大单净流入(元)-冗余字段',
  f281: '5日中单净流入(元)-冗余字段',
  f282: '5日小单净流入(元)-冗余字段',
  f292: '交易状态分类(13=正常,7=停牌/退市,6=临时停牌,9=新股)',
  f293: '每股发行价(元)',
  // 无数据字段
  f294: '预留字段(无数据)',
  f295: '预留字段(无数据)',
  f296: '预留字段(无数据)',
  f297: '数据日期(YYYYMMDD)',
  f298: '预留字段(无数据)',
  f299: '预留字段(无数据)',
  f300: '预留字段(无数据)',

  // 高管增减持字段（东方财富原始）
  security_code: '证券代码',
  derive_security_code: '证券代码（完整）',
  security_name: '证券简称',
  change_date: '变动日期',
  person_name: '变动人',
  change_shares: '变动股数（股）',
  average_price: '成交均价',
  change_amount: '变动金额',
  change_reason: '变动方式',
  change_ratio: '变动比例',
  change_after_holdnum: '变动后持股数',
  hold_type: '股份类型',
  dse_person_name: '董监高姓名',
  position_name: '职务',
  person_dse_relation: '与董监高关系',
  org_code: '机构代码',
  ggeid: '公告ID',
  begin_hold_num: '变动前持股数',
  end_hold_num: '变动后持股数',

  // 股东增减持字段（东方财富原始）
  holder_name: '股东名称',
  direction: '变动方向（增持/减持）',
  change_num: '变动数量（万股，绝对值）',
  change_num_symbol: '变动数量（万股，带符号：正数=增持，负数=减持）',
  change_rate: '变动截止日股票涨跌幅（%）',
  after_holder_num: '变动后持股总数（万股）',
  after_change_rate: '变动占总股本比例（%）',
  hold_ratio: '变动后占总股本比例（%）',
  trade_average_price: '成交均价（元）',
  close_price: '收盘价（元）',
  real_price: '实际成交价格（元）',
  newest_price: '最新价（元）',
  change_rate_quotes: '股价涨跌幅（%）',
  start_date: '变动开始日期',
  end_date: '变动截止日期',
  notice_date: '公告日期',
  trade_date: '交易日期',
  market: '交易市场类型',
  free_shares: '变动后持流通股数（万股）',
  free_shares_ratio: '变动后占流通股比例（%）',
  change_free_ratio: '变动占流通股比例（%）',
  eitime: '数据更新时间',
}

// 补充字段含义映射（覆盖各接口中非 fxx 字段）
const supplementalFieldMeanings: Record<string, string> = {
  actualLastTwoYearEps: '实际前两年每股收益(EPS)',
  actualLastYearEps: '实际去年每股收益(EPS)',
  ADD_AMP_LOWER: '业绩变动幅度下限(%)',
  ADD_AMP_UPPER: '业绩变动幅度上限(%)',
  AFTER_CHANGE_RATE: '变动后占总股本比例(%)',
  AFTER_HOLDER_NUM: '变动后持股总数(万股)',
  answer: '回复内容',
  answererName: '回复人',
  answerSource: '回复来源',
  answerTime: '回复时间',
  api_id: '源接口记录ID',
  askerName: '提问人',
  askSource: '提问来源',
  askTime: '提问时间',
  ASSIGNDSCRPT: '分配方案说明',
  attachedAuthor: '回复人',
  attachedContent: '回复内容',
  attachedId: '回复ID',
  attachedPubDate: '回复时间',
  attachPages: '附件页数',
  attachSize: '附件大小',
  attachType: '附件类型',
  attentionCompany: '是否关注公司',
  author: '作者/提问人ID',
  authorID: '作者ID',
  authorName: '提问人名称',
  AVG_HOLD_NUM: '户均持股数量(股)',
  AVG_MARKET_CAP: '户均持股市值(元)',
  BASIC_EPS: '基本每股收益(元)',
  BOARD_CODE: '板块代码',
  BOARD_NAME: '板块名称',
  boardType: '板块类型',
  BPS: '每股净资产(元)',
  BVPS_AFTER: '发行后每股净资产(元)',
  BVPS_BEFORE: '发行前每股净资产(元)',
  CHANGE_FREE_RATIO: '变动占流通股比例(%)',
  CHANGE_NUM: '变动数量(万股)',
  CHANGE_NUM_SYMBOL: '变动数量(带符号,万股)',
  CHANGE_RATE: '变动截止日涨跌幅(%)',
  CHANGE_RATE_QUOTES: '行情涨跌幅(%)',
  CHANGE_REASON: '变化原因',
  CHANGE_REASON_EXPLAIN: '业绩变动原因说明',
  CHANGE_SHARES: '股本变化(股)',
  CLOSE_PRICE: '收盘价(元)',
  column: '栏目/分类',
  companyLogo: '公司Logo链接',
  companyShortName: '公司简称',
  contentType: '内容类型',
  count: '统计数量',
  DATATYPE: '数据类型',
  DATAYEAR: '数据年份',
  DATEMMDD: '日期(月日)',
  DATE: '数据日期',
  DEDUCT_BASIC_EPS: '扣非每股收益(元)',
  DIRECTION: '增减持方向',
  DJDJLHZ: '单季度净利润环比(%)',
  DJDYSHZ: '单季度营收环比(%)',
  EITIME: '入库时间',
  emIndustryCode: '东方财富行业代码',
  emRatingCode: '东方财富评级代码',
  emRatingName: '东方财富评级名称',
  emRatingValue: '东方财富评级数值',
  encodeUrl: '报告链接编码',
  END_DATE: '截止日期',
  EQUITY_RECORD_DATE: '股权登记日',
  esId: 'ES文档ID',
  EUTIME: '更新时间',
  favoriteStatus: '收藏状态',
  FIN_BALANCE_GR: '融资余额增长率(%)',
  FORECAST_JZ: '业绩预告基准值',
  FORECAST_STATE: '预告状态',
  FREE_MARKET_CAP: '流通市值(元)',
  FREE_SHARES: '流通股本/流通股数',
  FREE_SHARES_RATIO: '占流通股比例(%)',
  FREESHARES_RATIO: '占流通股比例(%)',
  FREESHARES_RATIO_CHANGE: '流通股比例变化方向',
  HOLD_N_DATE: '公告日期(标准化)',
  HOLD_NOTICE_DATE: '公告日期',
  HOLD_RATIO: '变动后占总股本比例(%)',
  HOLD_VALUE: '持股市值(元)',
  HOLDCHA: '持仓变化类型(增/减/新进/不变)',
  HOLDCHA_NUM: '持仓变化数量(股)',
  KCB: '科创板标识(1=科创板,0=非科创板)',
  RCHANGE3DCP: '3日涨跌幅(%)',
  RCHANGE5DCP: '5日涨跌幅(%)',
  RCHANGE10DCP: '10日涨跌幅(%)',
  RQCHL: '融券偿还量(股)',
  RQCHL3D: '3日融券偿还量(股)',
  RQCHL5D: '5日融券偿还量(股)',
  RQCHL10D: '10日融券偿还量(股)',
  RQJMG: '融券净卖出量(股)',
  RQJMG3D: '3日融券净卖出量(股)',
  RQJMG5D: '5日融券净卖出量(股)',
  RQJMG10D: '10日融券净卖出量(股)',
  RQMCL: '融券卖出量(股)',
  RQMCL3D: '3日融券卖出量(股)',
  RQMCL5D: '5日融券卖出量(股)',
  RQMCL10D: '10日融券卖出量(股)',
  RQYE: '融券余额(元)',
  RQYL: '融券余量(股)',
  RZCHE: '融资偿还额(元)',
  RZCHE3D: '3日融资偿还额(元)',
  RZCHE5D: '5日融资偿还额(元)',
  RZCHE10D: '10日融资偿还额(元)',
  RZJME: '融资净买入额(元)',
  RZJME3D: '3日融资净买入额(元)',
  RZJME5D: '5日融资净买入额(元)',
  RZJME10D: '10日融资净买入额(元)',
  RZMRE: '融资买入额(元)',
  RZMRE3D: '3日融资买入额(元)',
  RZMRE5D: '5日融资买入额(元)',
  RZMRE10D: '10日融资买入额(元)',
  RZRQYE: '融资融券余额(元)',
  RZRQYECZ: '融资融券余额差值(元)',
  RZYE: '融资余额(元)',
  RZYEZB: '融资余额占流通市值比(%)',
  SCODE: '证券代码',
  SECNAME: '证券简称',
  SPJ: '收盘价(元)',
  SZ: '流通市值(元)',
  ZDF: '涨跌幅(%)',
  HOLDCHA_RATIO: '持仓变化比例(%)',
  HOLDCHA_VALUE: '持仓变化市值(元)',
  HOLDER_NAME: '股东名称',
  HOLDER_NUM: '股东户数',
  HOLDER_NUM_CHANGE: '股东户数变化',
  HOLDER_NUM_RATIO: '股东户数变化比例(%)',
  HOULD_NUM: '持有基金数',
  INCREASE_JZ: '业绩变动基准值',
  indexId: '问题ID',
  industryCode: '行业代码',
  industryName: '行业名称',
  indvAimPriceL: '目标价下限',
  indvAimPriceT: '目标价上限',
  indvInduCode: '细分行业代码',
  indvInduName: '细分行业名称',
  indvIsNew: '是否新覆盖',
  infoCode: '研报编码',
  INTERVAL_CHRATE: '区间涨跌幅(%)',
  interviewLive: '是否访谈直播',
  INVESTIGATORS: '调研人员',
  IS_LATEST: '是否最新公告',
  IS_MAX_REPORT: '是否最新报告',
  IS_SOURCE: '是否来源标记',
  isCheck: '是否审核',
  ISNEW: '是否最新',
  ISSUE_DATE: '发行日期',
  ISSUE_LISTING_DATE: '上市日期',
  ISSUE_NUM: '发行数量(股)',
  ISSUE_OBJECT: '发行对象',
  ISSUE_PRICE: '发行价格(元)',
  ISSUE_SHARE_AFTER: '发行后总股本(股)',
  ISSUE_SHARE_BEFORE: '发行前总股本(股)',
  ISSUE_WAY: '发行方式',
  JLRTBZCL: '净利润同比变动值',
  lastEmRatingCode: '上次东方财富评级代码',
  lastEmRatingName: '上次评级名称',
  lastEmRatingValue: '上次评级数值',
  LISTING_STATE: '上市状态',
  LOCKIN_PERIOD: '锁定期',
  mainContent: '提问内容',
  MARKET: '市场类型/市场标签',
  MGJYXJJE: '每股经营现金流(元)',
  NET_RAISE_FUNDS: '募集资金净额(元)',
  NEWEST_PRICE: '最新价(元)',
  newIssuePrice: '新股发行价(元)',
  newListingDate: '新股上市日期',
  newPeIssueA: '发行市盈率(发行A)',
  newPurchaseDate: '新股申购日期',
  NOTICE_DATE: '公告日期',
  NUM: '序号',
  NUMBERNEW: '新序号',
  OBJECT_CODE: '接待对象代码',
  ORG_CODE: '机构/公司代码',
  ORG_NAME: '机构/公司名称',
  ORG_TYPE: '机构类型',
  ORG_TYPE_CODE: '机构类型代码',
  ORG_TYPE_NAME: '机构类型名称',
  orgCode: '机构代码',
  orgName: '机构名称',
  orgSName: '机构简称',
  orgType: '机构类型',
  ORI_BOARD_CODE: '原始板块代码',
  packageDate: '打包日期',
  PARENT_BVPS: '归母每股净资产(元)',
  PARENT_NETPROFIT: '归母净利润(元)',
  PARENT_NETPROFIT_SQ: '归母净利润上年同期(元)',
  PAYYEAR: '分红年度',
  praiseCount: '点赞数',
  praiseStatus: '点赞状态',
  PRE_E_DATE: '上期公告日期',
  PRE_END_DATE: '上期截止日期',
  PRE_HOLDER_NUM: '上期股东户数',
  PREDICT_AMT_LOWER: '业绩预告金额下限',
  PREDICT_AMT_UPPER: '业绩预告金额上限',
  PREDICT_CONTENT: '业绩预告内容',
  PREDICT_FINANCE: '业绩预告财务指标',
  PREDICT_FINANCE_CODE: '业绩预告指标代码',
  PREDICT_HBMEAN: '业绩预告平均值',
  PREDICT_RATIO_LOWER: '业绩变动幅度下限(%)',
  PREDICT_RATIO_UPPER: '业绩变动幅度上限(%)',
  PREDICT_TYPE: '业绩预告类型',
  predictLastYearEps: '预测去年EPS',
  predictLastYearPe: '预测去年PE',
  predictNextTwoYearEps: '预测后年EPS',
  predictNextTwoYearPe: '预测后年PE',
  predictNextYearEps: '预测明年EPS',
  predictNextYearPe: '预测明年PE',
  predictThisYearEps: '预测今年EPS',
  predictThisYearPe: '预测今年PE',
  PREYEAR_SAME_PERIOD: '上年同期值',
  PRICE_PRINCIPLE: '定价原则',
  pubClient: '发布客户端',
  pubDate: '发布时间',
  publishDate: '发布日期',
  PUBLISHNAME: '公告名称',
  qaId: '问答ID',
  qaStatus: '问答状态',
  QCHANGE_RATE: '季度涨跌幅(%)',
  QDATE: '季度日期',
  question: '提问内容',
  ratingChange: '评级变动',
  REAL_PRICE: '实际成交价格(元)',
  RECEIVE_END_DATE: '接待结束日期',
  RECEIVE_OBJECT: '接待对象',
  RECEIVE_OBJECT_TYPE: '接待对象类型',
  RECEIVE_PLACE: '接待地点',
  RECEIVE_START_DATE: '接待开始日期',
  RECEIVE_TIME_EXPLAIN: '接待时间说明',
  RECEIVE_WAY: '接待方式代码',
  RECEIVE_WAY_EXPLAIN: '接待方式说明',
  RECEPTIONIST: '接待人员',
  REMARK: '备注',
  remindStatus: '提醒状态',
  REPORT_DATE: '报告期',
  REPORTDATE: '报告期',
  reportType: '报告类型',
  researcher: '研究员',
  score: '评分',
  secid: '证券ID',
  SECUCODE: '证券代码(带市场后缀)',
  SECURITY_CODE: '证券代码',
  SECURITY_INNER_CODE: '证券内部代码',
  SECURITY_NAME_ABBR: '证券简称',
  SECURITY_TYPE: '证券类型',
  SECURITY_TYPE_CODE: '证券类型代码',
  SJLHZ: '单季度利润环比(%)',
  SJLTZ: '实际净利润同比(%)',
  SOURCE: '数据来源',
  sRatingCode: '评级简称代码',
  sRatingName: '评级简称',
  START_DATE: '开始日期',
  stockCode: '股票代码',
  stockName: '股票名称',
  SUM: '合计/总数',
  topStatus: '置顶状态',
  TOTAL_A_SHARES: '总股本(股)',
  TOTAL_MARKET_CAP: '总市值(元)',
  TOTAL_OPERATE_INCOME: '营业总收入(元)',
  TOTAL_OPERATE_INCOME_SQ: '营业总收入上年同期(元)',
  TOTAL_RAISE_FUNDS: '募集资金总额(元)',
  TOTAL_SHARES: '持股总数(股)',
  TOTALSHARES_RATIO: '占总股本比例(%)',
  trade: '所属行业',
  TRADE_AVERAGE_PRICE: '成交均价(元)',
  TRADE_DATE: '交易日期',
  TRADE_MARKET: '交易市场',
  TRADE_MARKET_CODE: '交易市场代码',
  TRADE_MARKET_ZJG: '证监会市场分类',
  TYPE_NUM: '类型编号',
  UPDATE_DATE: '更新日期',
  updateDate: '更新时间',
  WEIGHTAVG_ROE: '加权净资产收益率(%)',
  XSMLL: '销售毛利率(%)',
  YSHZ: '营收环比(%)',
  YSTZ: '营收同比(%)',
  ZXGXL: '最新股息率(%)'
}

function getMetadataDescriptionSuggestion(fieldName: string): string | null {
  const tableField = tableFields.value.find(field => field.name === fieldName)
  const tableDescription = tableField?.description?.trim()
  if (tableDescription && tableDescription !== fieldName) {
    return tableDescription
  }

  const outputField = columns.value.find(field => field.name === fieldName)
  const outputDescription = outputField?.description?.trim()
  if (outputDescription && outputDescription !== fieldName) {
    return outputDescription
  }

  return null
}

// 列顺序（用于保存用户自定义的列顺序）
const columnOrder = ref<string[]>([])

// 计算有效字段和空值字段数量
const rawFieldsValidCount = computed(() => 
  rawFieldsList.value.filter(f => f.value !== '(空)').length
)
const rawFieldsEmptyCount = computed(() => 
  rawFieldsList.value.filter(f => f.value === '(空)').length
)

// 过滤后的原始字段列表
const filteredRawFields = computed(() => {
  if (!rawFieldsSearch.value) return rawFieldsList.value
  const keyword = rawFieldsSearch.value.toLowerCase()
  return rawFieldsList.value.filter(f => 
    f.name.toLowerCase().includes(keyword) || 
    (f.description && f.description.toLowerCase().includes(keyword))
  )
})

// 展开行相关状态
const expandData = ref<Record<string, any[]>>({})
const expandLoading = ref<Record<string, boolean>>({})

// 分页状态
const currentPage = ref(1)
const pageSize = ref(10)
const total = ref(0)

// 搜索状态
const searchKeyword = ref('')
const activeKeyword = ref('')  // 当前生效的搜索关键词
const searchColumn = ref('')   // 搜索列（空表示所有列）
const exactMatchMode = ref(false)  // 是否精确匹配模式（从值分布点击触发时为true）

// 排序状态
const sortField = ref('')
const sortOrder = ref<'ascending' | 'descending' | null>(null)
const defaultSortField = ref('')
const defaultSortOrder = ref<'ascending' | 'descending' | null>(null)

// 获取元数据 ID
const metadataId = computed(() => parseInt(route.params.metadataId as string, 10))

// 判断是否是大宗交易明细接口
const isDzjyDetail = computed(() => metadata.value?.table_name === 'dzjy_detail')

// 所有接口都支持列拖拽和表头排序（原来只有东方财富原始接口支持）
const isRawTable = computed(() => true)

// 排序后的列（根据用户自定义顺序，并过滤隐藏的列）
const sortedColumns = computed(() => {
  // 所有接口都优先使用 tableFields
  const sourceColumns = tableFields.value.length > 0
    ? tableFields.value.map(f => ({ name: f.name, label: f.description, type: f.type }))
    : columns.value

  // 先过滤掉隐藏的列
  let visibleColumns = sourceColumns.filter(col => !hiddenColumns.value.includes(col.name))

  if (columnOrder.value.length === 0) {
    return visibleColumns
  }
  // 按照 columnOrder 的顺序排列
  const orderMap = new Map(columnOrder.value.map((name, index) => [name, index]))
  return [...visibleColumns].sort((a, b) => {
    const orderA = orderMap.get(a.name) ?? Infinity
    const orderB = orderMap.get(b.name) ?? Infinity
    return orderA - orderB
  })
})

// 获取行的唯一标识
function getRowKey(row: any): string {
  return `${row.SECCODE}_${row.TRADEDATE}_${row.id}`
}

// 加载元数据
async function loadMetadata() {
  try {
    const res = await metadataApi.getById(metadataId.value) as any
    metadata.value = res.data
    // 元数据加载完成后，加载字段映射、列顺序和隐藏列
    await loadFieldMappings()
    await loadColumnOrder()
    await loadHiddenColumns()
  } catch (error) {
    ElMessage.error('加载接口信息失败')
  }
}

// 加载数据
async function loadData() {
  loading.value = true
  try {
    // 转换排序方向为后端格式
    const order = sortOrder.value === 'ascending' ? 'asc' : sortOrder.value === 'descending' ? 'desc' : ''
    const res = await dataApi.getData(
      metadataId.value,
      currentPage.value,
      pageSize.value,
      activeKeyword.value,
      sortField.value,
      order,
      searchColumn.value,
      exactMatchMode.value
    ) as any
    dataList.value = res.data.list || []
    total.value = res.data.total || 0
    columns.value = res.data.columns || []
    apiFieldCount.value = res.data.apiFieldCount || 0
    tableFieldCount.value = res.data.tableFieldCount || 0
    apiFields.value = res.data.apiFields || []
    tableFields.value = res.data.tableFields || []

    defaultSortField.value = res.data.defaultSortField || ''
    defaultSortOrder.value =
      res.data.defaultSortOrder === 'asc'
        ? 'ascending'
        : res.data.defaultSortOrder === 'desc'
          ? 'descending'
          : null

    // 首次加载时，如果没有设置排序且后端返回了默认排序配置，则应用默认排序
    if (!sortField.value && res.data.defaultSortField) {
      sortField.value = res.data.defaultSortField
      sortOrder.value = res.data.defaultSortOrder === 'asc' ? 'ascending' : 'descending'
    }

    // 清空展开数据
    expandData.value = {}
    expandLoading.value = {}
  } catch (error) {
    ElMessage.error('加载数据失败')
  } finally {
    loading.value = false
  }
}

// 处理展开行变化
async function handleExpandChange(row: any, expandedRows: any[]) {
  const rowKey = getRowKey(row)
  
  // 如果是展开操作且还没有加载过数据
  if (expandedRows.includes(row) && !expandData.value[rowKey]) {
    expandLoading.value[rowKey] = true
    
    try {
      const res = await dataApi.getDetail(
        metadataId.value,
        row.TRADEDATE,
        row.SECCODE
      ) as any
      
      expandData.value[rowKey] = res.data || []
    } catch (error) {
      console.error('加载细项数据失败:', error)
      expandData.value[rowKey] = []
      ElMessage.error('加载细项数据失败')
    } finally {
      expandLoading.value[rowKey] = false
    }
  }
}

// 格式化数字
function formatNumber(value: any): string {
  if (value === null || value === undefined) return '-'
  return typeof value === 'number' ? value.toFixed(2) : String(value)
}

// 显示字段列表弹窗
function showFieldsDialog(title: string, fields: FieldInfo[]) {
  fieldsDialogTitle.value = title
  fieldsDialogList.value = fields
  fieldsDialogVisible.value = true
}

// 显示官方API原始字段弹窗
async function showRawFieldsDialog() {
  rawFieldsLoading.value = true
  rawFieldsSearch.value = ''
  
  try {
    const res = await dataApi.getRawFields(metadataId.value) as any
    
    if (res.data.supported) {
      rawFieldsSupported.value = true
      rawFieldsTotal.value = res.data.totalFields
      rawFieldsList.value = res.data.fields
    } else {
      rawFieldsSupported.value = false
      rawFieldsMessage.value = res.data.message
      rawFieldsList.value = []
    }
    
    rawFieldsDialogVisible.value = true
  } catch (error) {
    ElMessage.error('获取官方API字段失败')
  } finally {
    rawFieldsLoading.value = false
  }
}

// 格式化原始字段值
function formatRawValue(value: any): string {
  if (value === '(空)') return '(空)'
  if (typeof value === 'object') return JSON.stringify(value)
  if (typeof value === 'string' && value.length > 100) {
    return value.substring(0, 100) + '...'
  }
  return String(value)
}

// 触发更新
async function triggerUpdate() {
  updating.value = true
  try {
    await updateApi.triggerUpdate(metadataId.value)
    ElMessage.success('更新任务已启动')
  } catch (error) {
    ElMessage.error('触发更新失败')
    updating.value = false
  }
  // 更新完成后由 WebSocket 消息处理刷新
}

// 返回上一页
function goBack() {
  router.push('/')
}

// 分页处理
function handleSizeChange(size: number) {
  pageSize.value = size
  currentPage.value = 1
  loadData()
}

function handlePageChange(page: number) {
  currentPage.value = page
  loadData()
}

// 搜索处理
function handleSearch() {
  activeKeyword.value = searchKeyword.value.trim()
  exactMatchMode.value = false  // 普通搜索使用模糊匹配
  currentPage.value = 1
  loadData()
}

// 清除搜索
function handleClearSearch() {
  activeKeyword.value = ''
  exactMatchMode.value = false  // 清除搜索时重置精确匹配模式
  currentPage.value = 1
  loadData()
}

// 搜索列变化时的处理
function handleSearchColumnChange() {
  // 如果有搜索关键词，重新搜索
  if (activeKeyword.value) {
    currentPage.value = 1
    loadData()
  }
}

// 通过字段值筛选列表（从编辑弹窗的值分布点击触发）
function filterByFieldValue(value: any) {
  // 关闭弹窗
  fieldMappingDialogVisible.value = false
  
  // 设置搜索列为当前编辑的字段
  searchColumn.value = editingField.value
  
  // 设置搜索关键词为点击的值
  const searchValue = value === null ? '' : String(value)
  searchKeyword.value = searchValue
  activeKeyword.value = searchValue
  
  // 启用精确匹配模式
  exactMatchMode.value = true
  
  // 重置分页并加载数据
  currentPage.value = 1
  loadData()
  
  ElMessage.success(`已筛选 ${getColumnDisplayName(editingField.value)} = "${formatFieldValue(value)}"`)
}

// 获取列的显示名称（中文名或字段名）
function getColumnDisplayName(fieldName: string): string {
  return fieldMappings.value[fieldName] || fieldName
}

// 获取搜索列选择器的显示标签
function getSearchColumnLabel(col: OutputConfig): string {
  const cnName = fieldMappings.value[col.name]
  if (cnName) {
    return `${cnName} (${col.name})`
  }
  return col.name
}

// 搜索框占位符
const searchPlaceholder = computed(() => {
  if (!searchColumn.value) {
    return '输入关键词搜索所有列...'
  }
  const cnName = fieldMappings.value[searchColumn.value]
  if (cnName) {
    return `在 ${cnName} 列中搜索...`
  }
  return `在 ${searchColumn.value} 列中搜索...`
})

// 是否有激活的搜索条件
const hasActiveSearch = computed(() => {
  return !!activeKeyword.value || !!searchColumn.value
})

// 重置搜索（恢复到初始状态）
function handleReset() {
  searchKeyword.value = ''
  activeKeyword.value = ''
  searchColumn.value = ''
  exactMatchMode.value = false
  currentPage.value = 1
  loadData()
}

// 处理排序变化（Element Plus 内置排序回调，保留兼容）
function handleSortChange({ prop, order }: { prop: string; order: 'ascending' | 'descending' | null }) {
  sortField.value = prop || ''
  sortOrder.value = order
  currentPage.value = 1
  loadData()
}

function getOppositeSortOrder(order: 'ascending' | 'descending' | null): 'ascending' | 'descending' {
  return order === 'ascending' ? 'descending' : 'ascending'
}

// 处理表头排序图标点击
function handleHeaderSort(fieldName: string) {
  if (sortField.value === fieldName) {
    const isDefaultField = defaultSortField.value === fieldName
    const isDefaultState = isDefaultField && defaultSortOrder.value === sortOrder.value

    if (isDefaultField) {
      sortOrder.value = isDefaultState
        ? getOppositeSortOrder(defaultSortOrder.value)
        : defaultSortOrder.value
    } else {
      // 非默认列：切换排序方向 descending -> ascending -> null
      if (sortOrder.value === 'descending') {
        sortOrder.value = 'ascending'
      } else if (sortOrder.value === 'ascending') {
        sortOrder.value = null
        sortField.value = ''
      } else {
        sortOrder.value = 'descending'
      }
    }
  } else {
    // 不同列：设置为降序
    sortField.value = fieldName
    sortOrder.value = 'descending'
  }
  currentPage.value = 1
  loadData()
}

// 高亮匹配文本
function highlightText(text: string): string {
  // 精确匹配模式时不高亮
  if (exactMatchMode.value) return text
  if (!activeKeyword.value || !text) return text
  const keyword = activeKeyword.value
  const regex = new RegExp(`(${keyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi')
  return text.replace(regex, '<span class="highlight">$1</span>')
}

// 获取列宽度（根据表头文字长度自适应）
function getColumnWidth(col: OutputConfig): number {
  // 获取中文名和字段名
  const cnName = fieldMappings.value[col.name] || ''
  const fieldName = col.name
  
  // 计算表头需要的宽度
  // 单行显示时，同时考虑中文名和字段名的总宽度
  // 中文字符约 16px，英文/数字约 10px，分隔间距约 14px
  // 表头有编辑图标(20px) + 排序图标(20px) + 间距(18px) + 内边距(30px) = 88px
  const iconAndPadding = 88
  const cnNameWidth = cnName.length * 16
  const fieldNameWidth = fieldName.length * 10
  const inlineGapWidth = cnName ? 10 : 0
  const headerWidth = cnNameWidth + fieldNameWidth + inlineGapWidth + iconAndPadding
  
  // 根据数据类型设置基础最小宽度
  let baseWidth = 100
  if (col.type === 'INTEGER' || col.type === 'REAL') {
    baseWidth = 100
  } else {
    baseWidth = 120
  }
  
  // 返回表头宽度和基础宽度中较大的值
  return Math.max(headerWidth, baseWidth)
}

// 格式化数值为万/亿格式
function formatAsWanYi(value: number): string {
  const absValue = Math.abs(value)
  const sign = value < 0 ? '-' : ''
  
  if (absValue >= 100000000) {
    // 亿级别
    return sign + (absValue / 100000000).toFixed(2) + '亿'
  } else if (absValue >= 10000) {
    // 万级别
    return sign + (absValue / 10000).toFixed(2) + '万'
  } else {
    // 小于万，保留两位小数
    return value.toFixed(2)
  }
}

// 格式化Unix时间戳为日期时间
function formatTimestamp(value: number): string {
  // Unix时间戳通常是10位（秒级）
  const timestamp = value > 9999999999 ? value : value * 1000
  const date = new Date(timestamp)
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    timeZone: 'Asia/Shanghai'
  })
}

// 格式化值
function formatValue(value: any, type: string, colName?: string): string {
  if (value === null || value === undefined) {
    return '-'
  }

  // 处理 ggeid 字段的科学计数法显示
  if (colName === 'ggeid' && value) {
    // 将科学计数法转换为完整数字字符串
    const numValue = typeof value === 'number' ? value : parseFloat(value)
    if (!isNaN(numValue)) {
      return numValue.toFixed(0)
    }
  }

  // 获取该字段的单位设置
  const unit = colName ? fieldUnitSettings.value[colName] : ''
  
  // 检查是否有值映射（优先级最高，值映射不加单位）
  // 注意：只有当映射值非空时才使用，空字符串的映射会被跳过
  if (colName && fieldValueMappings.value[colName]) {
    const mapping = fieldValueMappings.value[colName]
    const key = String(value)
    if (key in mapping && mapping[key]) {
      return mapping[key]
    }
  }
  
  // 检查是否需要转换为时间戳格式（时间戳不加单位）
  if (colName && fieldTimestampSettings.value[colName]) {
    const numValue = typeof value === 'number' ? value : parseInt(value, 10)
    // 检查是否是合理的Unix时间戳（大于2000年1月1日的时间戳）
    if (!isNaN(numValue) && numValue > 946684800) {
      return formatTimestamp(numValue)
    }
  }
  
  // 检测时间戳字段（毫秒级时间戳，通常是13位数字字符串，不加单位）
  const timestampFields = ['pubDate', 'updateDate', 'attachedPubDate']
  if (colName && timestampFields.includes(colName)) {
    const timestamp = typeof value === 'string' ? parseInt(value, 10) : value
    if (!isNaN(timestamp) && timestamp > 1000000000000) {
      const date = new Date(timestamp)
      return date.toLocaleString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
      })
    }
  }
  
  // 检查是否需要格式化为万/亿
  if (colName && fieldFormatSettings.value[colName]) {
    // 支持数字和字符串类型的数值
    const numValue = typeof value === 'number' ? value : parseFloat(value)
    if (!isNaN(numValue)) {
      const formatted = formatAsWanYi(numValue)
      return unit ? `${formatted}${unit}` : formatted
    }
  }
  
  // 如果有单位设置，尝试在数值后面添加单位
  if (unit) {
    // 检查是否是数值类型
    if (typeof value === 'number') {
      // 浮点数保留2位小数
      if (type === 'REAL' || !Number.isInteger(value)) {
        return `${value.toFixed(2)}${unit}`
      }
      return `${value}${unit}`
    }
    // 字符串类型的数值也添加单位
    const numValue = parseFloat(value)
    if (!isNaN(numValue)) {
      return `${value}${unit}`
    }
  }
  
  if (type === 'REAL' && typeof value === 'number') {
    return value.toFixed(2)
  }
  
  // 尝试解析 JSON 字符串
  if (typeof value === 'string' && (value.startsWith('{') || value.startsWith('['))) {
    try {
      return JSON.stringify(JSON.parse(value), null, 2)
    } catch {
      return value
    }
  }
  return String(value)
}

// 获取值的样式类
function getValueClass(value: any, type: string): string {
  if (value === null || value === undefined) {
    return 'value-null'
  }
  if (type === 'INTEGER' || type === 'REAL') {
    return 'value-number'
  }
  return ''
}

function shouldClampCell(fieldName: string, value: any): boolean {
  if (!fieldTextClampSettings.value[fieldName]) {
    return false
  }

  if (value === null || value === undefined) {
    return false
  }

  return String(value).length > 0
}

function handleCellContentClick(fieldName: string, value: any) {
  if (!shouldClampCell(fieldName, value)) {
    return
  }

  fullTextDialogTitle.value = `${getColumnDisplayName(fieldName)} (${fieldName})`
  fullTextDialogContent.value = value === null || value === undefined ? '' : String(value)
  fullTextDialogVisible.value = true
}

// 加载字段映射
async function loadFieldMappings() {
  if (!metadata.value?.table_name) return
  try {
    const res = await fieldMappingApi.getMappings(metadata.value.table_name) as any
    fieldMappings.value = res.data || {}
    fieldFormatSettings.value = res.formatSettings || {}
    fieldTimestampSettings.value = res.timestampSettings || {}
    fieldValueMappings.value = res.valueMappings || {}
    fieldUnitSettings.value = res.unitSettings || {}
    fieldTextClampSettings.value = res.textClampSettings || {}
  } catch (error) {
    console.error('加载字段映射失败:', error)
  }
}

// 打开字段映射编辑弹窗
async function openFieldMappingDialog(fieldName: string) {
  editingField.value = fieldName
  editingCnName.value = fieldMappings.value[fieldName] || ''
  editingUnit.value = fieldUnitSettings.value[fieldName] || ''
  editingFormatAsWanYi.value = fieldFormatSettings.value[fieldName] || false
  editingFormatAsTimestamp.value = fieldTimestampSettings.value[fieldName] || false
  editingTextClamp.value = fieldTextClampSettings.value[fieldName] || false
  editingValueMapping.value = fieldValueMappings.value[fieldName] ? { ...fieldValueMappings.value[fieldName] } : {}
  fieldValuesData.value = null
  fieldMappingDialogVisible.value = true
  
  // 异步加载字段值分布
  if (metadata.value?.table_name) {
    fieldValuesLoading.value = true
    try {
      const res = await dataApi.getFieldValues(metadata.value.table_name, fieldName) as any
      fieldValuesData.value = res.data
      
      // 如果值分布数量较少（<=10），自动初始化值映射配置
      if (res.data && res.data.uniqueCount <= 10 && res.data.topValues) {
        // 只初始化还没有映射的值
        for (const item of res.data.topValues) {
          const key = String(item.value)
          if (!(key in editingValueMapping.value)) {
            editingValueMapping.value[key] = ''  // 默认为空，用户可以填写
          }
        }
      }
    } catch (error) {
      console.error('加载字段值分布失败:', error)
    } finally {
      fieldValuesLoading.value = false
    }
  }
}

// 保存字段映射
async function saveFieldMapping() {
  if (!metadata.value?.table_name || !editingField.value) return
  
  savingMapping.value = true
  try {
    await fieldMappingApi.updateMapping(
      metadata.value.table_name,
      editingField.value,
      editingCnName.value.trim(),
      editingFormatAsWanYi.value,
      editingFormatAsTimestamp.value,
      editingValueMapping.value,
      editingUnit.value.trim(),
      editingTextClamp.value
    )
    
    // 更新本地缓存
    if (editingCnName.value.trim()) {
      fieldMappings.value[editingField.value] = editingCnName.value.trim()
    } else {
      delete fieldMappings.value[editingField.value]
    }
    
    // 更新单位设置缓存
    if (editingUnit.value.trim()) {
      fieldUnitSettings.value[editingField.value] = editingUnit.value.trim()
    } else {
      delete fieldUnitSettings.value[editingField.value]
    }
    
    // 更新格式化设置缓存
    if (editingFormatAsWanYi.value) {
      fieldFormatSettings.value[editingField.value] = true
    } else {
      delete fieldFormatSettings.value[editingField.value]
    }
    
    // 更新时间戳转换设置缓存
    if (editingFormatAsTimestamp.value) {
      fieldTimestampSettings.value[editingField.value] = true
    } else {
      delete fieldTimestampSettings.value[editingField.value]
    }

    // 更新长文本折叠设置缓存
    if (editingTextClamp.value) {
      fieldTextClampSettings.value[editingField.value] = true
    } else {
      delete fieldTextClampSettings.value[editingField.value]
    }
    
    // 更新值映射缓存
    const hasValueMapping = Object.values(editingValueMapping.value).some(v => v.trim() !== '')
    if (hasValueMapping) {
      // 只保存有值的映射
      const cleanedMapping: Record<string, string> = {}
      for (const [key, value] of Object.entries(editingValueMapping.value)) {
        if (value.trim()) {
          cleanedMapping[key] = value.trim()
        }
      }
      fieldValueMappings.value[editingField.value] = cleanedMapping
    } else {
      delete fieldValueMappings.value[editingField.value]
    }
    
    // 触发表格数据更新（不使用 tableKey++ 以保持滚动位置）
    // 通过浅拷贝 dataList 触发 Vue 响应式更新
    dataList.value = [...dataList.value]
    notifyFieldDisplayConfigChanged(metadata.value.table_name)
    
    fieldMappingDialogVisible.value = false
    ElMessage.success('保存成功')
  } catch (error) {
    ElMessage.error('保存失败')
  } finally {
    savingMapping.value = false
  }
}

// 获取列的中文名（如果有映射）
function getColumnCnName(col: OutputConfig): string | null {
  return fieldMappings.value[col.name] || null
}

// 获取列的原始字段名
function getColumnFieldName(col: OutputConfig): string {
  return col.name
}

// 判断字段是否可编辑映射
function isEditableField(fieldName: string): boolean {
  // System fields that should not be editable
  if (fieldName === 'id' || fieldName === 'metadata_id' || fieldName === 'collected_at' || fieldName === 'created_at') {
    return false
  }

  // All other fields are editable
  return true
}

// 获取系统推断的字段含义
function getFieldSuggestion(fieldName: string): string | null {
  const staticSuggestion = fieldMeanings[fieldName]
  if (staticSuggestion) {
    return staticSuggestion
  }

  const supplementalSuggestion = supplementalFieldMeanings[fieldName]
  if (supplementalSuggestion) {
    return supplementalSuggestion
  }

  return getMetadataDescriptionSuggestion(fieldName)
}

// 格式化字段值（用于值分布显示）
function formatFieldValue(value: any): string {
  if (value === null || value === undefined) return '(空)'
  if (typeof value === 'string' && value.length > 30) {
    return value.substring(0, 30) + '...'
  }
  return String(value)
}

function sanitizeDownloadName(name: string): string {
  return name.replace(/[<>:"/\\|?*]+/g, '-').trim() || 'export'
}

function escapeCsvCell(value: any): string {
  if (value === null || value === undefined) return ''
  const text = String(value)
  if (/[",\r\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`
  }
  return text
}

function getExportColumnHeader(col: OutputConfig): string {
  return getColumnCnName(col) || getColumnFieldName(col)
}

async function downloadDataExport(format: 'csv' | 'json') {
  exportDataLoading.value = format
  try {
    const order = sortOrder.value === 'ascending' ? 'asc' : sortOrder.value === 'descending' ? 'desc' : ''
    const result = await dataApi.getExportData(
      metadataId.value,
      true,
      activeKeyword.value,
      sortField.value,
      order,
      searchColumn.value,
      exactMatchMode.value
    ) as any

    const sourceRows = result.rows || []
    const exportColumns = sortedColumns.value.filter(col => !sourceRows[0] || Object.prototype.hasOwnProperty.call(sourceRows[0], col.name))
    const columnHeaders = exportColumns.map(col => getExportColumnHeader(col))
    const formattedRows = sourceRows.map((row: Record<string, any>) =>
      exportColumns.map(col => formatValue(row[col.name], col.type, col.name))
    )

    let blob: Blob
    if (format === 'csv') {
      const lines = [
        columnHeaders.map(header => escapeCsvCell(header)).join(','),
        ...formattedRows.map(row => row.map(cell => escapeCsvCell(cell)).join(','))
      ]
      blob = new Blob([`\uFEFF${lines.join('\r\n')}`], { type: 'text/csv;charset=utf-8' })
    } else {
      const jsonPayload = {
        metadata: {
          id: metadataId.value,
          cnName: metadata.value?.cn_name || '',
          tableName: metadata.value?.table_name || '',
          total: sourceRows.length,
          exportedAt: new Date().toISOString(),
          keyword: activeKeyword.value,
          searchColumn: searchColumn.value || null,
          exactMatch: exactMatchMode.value,
          sortField: sortField.value || null,
          sortOrder: order || null
        },
        columns: exportColumns.map((col, index) => ({
          field: col.name,
          header: columnHeaders[index],
          type: col.type
        })),
        rows: formattedRows
      }
      blob = new Blob([JSON.stringify(jsonPayload, null, 2)], { type: 'application/json;charset=utf-8' })
    }

    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    const now = new Date()
    const timestamp = now.toISOString().slice(0, 19).replace(/:/g, '-').replace('T', '-')
    const fileBaseName = sanitizeDownloadName(metadata.value?.cn_name || `metadata-${metadataId.value}`)

    a.href = url
    a.download = `${fileBaseName}-visible-${timestamp}.${format}`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)

    ElMessage.success(`已导出 ${total.value} 条数据到 ${format.toUpperCase()}`)
  } catch (error) {
    ElMessage.error(`导出 ${format.toUpperCase()} 失败`)
  } finally {
    exportDataLoading.value = ''
  }
}

// 导出字段映射
async function exportFieldMappings() {
  exportLoading.value = true
  try {
    const res = await fieldMappingApi.exportAll() as any
    const data = res.data
    
    // 创建下载
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    // 格式：field-mappings-2026-01-08-10-15-30.json
    const now = new Date()
    const timestamp = now.toISOString().slice(0, 19).replace(/:/g, '-').replace('T', '-')
    a.download = `field-mappings-${timestamp}.json`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
    
    ElMessage.success(`导出成功，共 ${data.totalCount} 条记录（${data.mappingCount} 条映射，${data.orderCount} 条排序）`)
  } catch (error) {
    ElMessage.error('导出失败')
  } finally {
    exportLoading.value = false
  }
}

// 触发文件选择
function triggerImportFile() {
  importFileInput.value?.click()
}

// 处理导入文件
async function handleImportFile(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  
  try {
    const text = await file.text()
    const data = JSON.parse(text)
    
    if (!data.mappings) {
      ElMessage.error('无效的映射文件格式')
      return
    }
    
    const res = await fieldMappingApi.importAll(data.mappings, 'merge') as any
    ElMessage.success(res.message || '导入成功')
    
    // 重新加载字段映射、列顺序和隐藏列
    await loadFieldMappings()
    await loadColumnOrder()
    await loadHiddenColumns()
    
    // 强制重新渲染表格
    tableKey.value++
    
    // 重新初始化拖拽
    nextTick(() => {
      initColumnDrag()
    })
  } catch (error) {
    ElMessage.error('导入失败：文件格式错误')
  } finally {
    // 清空文件选择，允许再次选择同一文件
    input.value = ''
  }
}

// 删除所有字段映射
async function deleteAllMappings() {
  // 二次确认
  const confirmed = await ElMessageBox.confirm(
    '确定要删除所有字段映射、列顺序和隐藏设置吗？此操作不可恢复，建议先导出备份。删除后列顺序将恢复为 f1, f2, ..., f300 的默认顺序，所有隐藏的列也会重新显示。',
    '删除确认',
    {
      confirmButtonText: '确定删除',
      cancelButtonText: '取消',
      type: 'warning'
    }
  ).catch(() => false)
  
  if (!confirmed) return
  
  deleteLoading.value = true
  try {
    const res = await fieldMappingApi.deleteAll() as any
    ElMessage.success(res.message || '删除成功')
    
    // 清空本地的字段映射、列顺序和隐藏列
    fieldMappings.value = {}
    fieldFormatSettings.value = {}
    fieldTimestampSettings.value = {}
    fieldValueMappings.value = {}
    fieldUnitSettings.value = {}
    fieldTextClampSettings.value = {}
    columnOrder.value = []
    hiddenColumns.value = []
    
    // 强制重新渲染表格
    tableKey.value++
    
    // 重新初始化拖拽
    nextTick(() => {
      initColumnDrag()
    })
  } catch (error) {
    ElMessage.error('删除失败')
  } finally {
    deleteLoading.value = false
  }
}

// 表格引用
const tableRef = ref<any>(null)

// 列顺序是否已加载
const columnOrderLoaded = ref(false)

// 表格 key（用于强制重新渲染）
const tableKey = ref(0)

// 初始化列拖拽排序
function initColumnDrag() {
  if (!isRawTable.value) return
  
  nextTick(() => {
    const table = tableRef.value?.$el
    if (!table) return
    
    const headerRow = table.querySelector('.el-table__header-wrapper tr')
    if (!headerRow) return
    
    // 销毁旧的 Sortable 实例
    if (columnSortable) {
      columnSortable.destroy()
      columnSortable = null
    }
    
    // 在表头单元格上添加 data-field 属性，用于识别被拖动的字段
    const headerCells = headerRow.querySelectorAll('.el-table__cell')
    sortedColumns.value.forEach((col, index) => {
      // eastmoney_stock_raw 表没有展开列，数据列从索引 0 开始
      const cell = headerCells[index] as HTMLElement
      if (cell) {
        cell.setAttribute('data-field', col.name)
        cell.classList.add('draggable-column')
      }
    })

    // 获取表格的滚动容器
    // Element Plus 表格的滚动是通过 .el-table__body-wrapper 内部的 .el-scrollbar__wrap 实现的
    const bodyWrapper = table.querySelector('.el-table__body-wrapper') as HTMLElement
    const scrollWrapper = table.querySelector('.el-table__body-wrapper .el-scrollbar__wrap') as HTMLElement
    const scrollContainer = scrollWrapper || bodyWrapper
    
    // 自动滚动相关变量
    let scrollInterval: ReturnType<typeof setInterval> | null = null
    let isDragging = false
    let draggedElement: HTMLElement | null = null
    const scrollSensitivity = 200 // 距离边缘多少像素时开始滚动（扩大范围）
    const minScrollSpeed = 3 // 最小滚动速度（边缘区域外侧）
    const maxScrollSpeed = 50 // 最大滚动速度（紧贴边缘时，大幅提升）
    
    // 停止自动滚动
    const stopAutoScroll = () => {
      if (scrollInterval) {
        clearInterval(scrollInterval)
        scrollInterval = null
      }
    }
    
    // 开始自动滚动循环
    const startAutoScroll = () => {
      if (scrollInterval) return // 已经在滚动了
      console.log('开始自动滚动')
      
      scrollInterval = setInterval(() => {
        if (!isDragging || !scrollContainer) {
          stopAutoScroll()
          return
        }
        
        // 尝试获取 Sortable.js 的拖拽克隆元素
        const ghostElement = document.querySelector('.sortable-ghost') as HTMLElement
        const chosenElement = document.querySelector('.sortable-chosen') as HTMLElement
        const dragElement = ghostElement || chosenElement || draggedElement
        
        if (!dragElement) {
          return
        }
        
        // 使用拖拽元素的位置来判断是否需要滚动
        const tableRect = table.getBoundingClientRect()
        const draggedRect = dragElement.getBoundingClientRect()
        const draggedCenterX = draggedRect.left + draggedRect.width / 2
        
        const distanceFromLeft = draggedCenterX - tableRect.left
        const distanceFromRight = tableRect.right - draggedCenterX
        
        let scrollDirection: 'left' | 'right' | null = null
        let intensity = 0 // 0-1，越靠近边缘越大
        
        if (distanceFromLeft < scrollSensitivity && distanceFromLeft > 0) {
          scrollDirection = 'left'
          // 越靠近边缘，intensity 越大（接近1）
          intensity = 1 - (distanceFromLeft / scrollSensitivity)
        } else if (distanceFromRight < scrollSensitivity && distanceFromRight > 0) {
          scrollDirection = 'right'
          intensity = 1 - (distanceFromRight / scrollSensitivity)
        }
        
        if (scrollDirection && intensity > 0) {
          // 速度随距离变化：越靠近边缘速度越快
          // intensity=0 时速度为 minScrollSpeed，intensity=1 时速度为 maxScrollSpeed
          const speed = minScrollSpeed + (maxScrollSpeed - minScrollSpeed) * intensity
          const scrollAmount = scrollDirection === 'left' ? -speed : speed
          
          console.log(`滚动方向: ${scrollDirection}, 距离强度: ${intensity.toFixed(2)}, 速度: ${speed.toFixed(1)}`)
          scrollContainer.scrollLeft += scrollAmount
        }
      }, 16) // 约60fps
    }
    
    // 创建新的 Sortable 实例
    columnSortable = Sortable.create(headerRow, {
      animation: 150,
      delay: 0,
      onStart: (evt: any) => {
        isDragging = true
        // 获取正在拖拽的元素
        draggedElement = evt.item
        // 开始自动滚动循环
        startAutoScroll()
      },
      onMove: (evt: any) => {
        // 拖拽移动时，使用 draggedEl 获取实际拖拽元素位置
        if (evt.dragged) {
          draggedElement = evt.dragged
        }
      },
      onChange: (evt: any) => {
        // 拖拽过程中更新拖拽元素引用
        draggedElement = evt.item
      },
      onEnd: (evt: any) => {
        // 停止拖拽状态
        isDragging = false
        draggedElement = null
        // 停止自动滚动
        stopAutoScroll()
        
        const { oldIndex, newIndex } = evt
        if (oldIndex === newIndex) {
          return
        }
        
        // eastmoney_stock_raw 表没有前置列，索引直接使用
        const adjustedOldIndex = oldIndex
        const adjustedNewIndex = newIndex
        
        if (adjustedOldIndex < 0 || adjustedNewIndex < 0) return
        if (adjustedOldIndex >= sortedColumns.value.length || adjustedNewIndex >= sortedColumns.value.length) return
        
        // 更新列顺序（Vue 响应式会自动更新视图）
        const newOrder = sortedColumns.value.map(col => col.name)
        const [removed] = newOrder.splice(adjustedOldIndex, 1)
        newOrder.splice(adjustedNewIndex, 0, removed)
        columnOrder.value = newOrder
        
        // 【关键】同步移动数据列的 DOM，使其与表头保持一致
        // Sortable.js 只移动了表头，我们需要手动移动每一行的数据单元格
        const table = tableRef.value?.$el
        if (table) {
          const bodyRows = table.querySelectorAll('.el-table__body-wrapper tbody tr')
          bodyRows.forEach((row: Element) => {
            const cells = row.querySelectorAll('td')
            if (cells.length > adjustedOldIndex && cells.length > adjustedNewIndex) {
              const cell = cells[adjustedOldIndex]
              if (adjustedNewIndex > adjustedOldIndex) {
                // 向后移动
                const refCell = cells[adjustedNewIndex + 1] || null
                row.insertBefore(cell, refCell)
              } else {
                // 向前移动
                const refCell = cells[adjustedNewIndex]
                row.insertBefore(cell, refCell)
              }
            }
          })
        }
        
        // 保存到数据库
        saveColumnOrder()
        
        ElMessage.success('列顺序已更新')
      }
    })
  })
}

// 保存列顺序到数据库
async function saveColumnOrder() {
  if (!metadata.value?.table_name) return
  try {
    await fieldMappingApi.saveColumnOrder(metadata.value.table_name, columnOrder.value)
    notifyFieldDisplayConfigChanged(metadata.value.table_name)
  } catch (error) {
    console.error('保存列顺序失败:', error)
  }
}

// 从数据库加载列顺序
async function loadColumnOrder() {
  if (!metadata.value?.table_name) return
  try {
    const res = await fieldMappingApi.getColumnOrder(metadata.value.table_name) as any
    if (res.data && res.data.length > 0) {
      columnOrder.value = res.data
    }
    columnOrderLoaded.value = true
  } catch (error) {
    console.error('加载列顺序失败:', error)
    columnOrder.value = []
    columnOrderLoaded.value = true
  }
}

// 从数据库加载隐藏列
async function loadHiddenColumns() {
  if (!metadata.value?.table_name) return
  try {
    const res = await fieldMappingApi.getHiddenColumns(metadata.value.table_name) as any
    hiddenColumns.value = res.data || []
  } catch (error) {
    console.error('加载隐藏列失败:', error)
    hiddenColumns.value = []
  }
}

// 隐藏/显示列
async function toggleColumnHidden(fieldName: string, hidden: boolean) {
  if (!metadata.value?.table_name) return
  
  hidingColumn.value = true
  try {
    // 如果是隐藏操作，先保存当前编辑的中文名和单位
    if (hidden && fieldMappingDialogVisible.value) {
      await fieldMappingApi.updateMapping(
        metadata.value.table_name,
        fieldName,
        editingCnName.value || '',
        editingFormatAsWanYi.value,
        editingFormatAsTimestamp.value,
        Object.keys(editingValueMapping.value).length > 0 ? editingValueMapping.value : undefined,
        editingUnit.value || undefined,
        editingTextClamp.value
      )
      // 更新本地映射
      if (editingCnName.value) {
        fieldMappings.value[fieldName] = editingCnName.value
      }
      if (editingUnit.value) {
        fieldUnitSettings.value[fieldName] = editingUnit.value
      }
      if (editingTextClamp.value) {
        fieldTextClampSettings.value[fieldName] = true
      } else {
        delete fieldTextClampSettings.value[fieldName]
      }
    }
    
    await fieldMappingApi.setColumnHidden(metadata.value.table_name, fieldName, hidden)
    
    // 更新本地状态
    if (hidden) {
      if (!hiddenColumns.value.includes(fieldName)) {
        hiddenColumns.value.push(fieldName)
      }
    } else {
      hiddenColumns.value = hiddenColumns.value.filter(f => f !== fieldName)
    }
    
    fieldMappingDialogVisible.value = false
    ElMessage.success(hidden ? '已保存并隐藏该列' : '已显示该列')
    
    // 触发表格更新（不使用 tableKey++ 以保持滚动位置）
    // hiddenColumns 是响应式的，sortedColumns 会自动重新计算
    // 通过浅拷贝 dataList 触发 Vue 响应式更新
    dataList.value = [...dataList.value]
    
    // 重新初始化拖拽
    nextTick(() => {
      initColumnDrag()
    })
    notifyFieldDisplayConfigChanged(metadata.value.table_name)
  } catch (error) {
    ElMessage.error('操作失败')
  } finally {
    hidingColumn.value = false
  }
}

// 从字段名中提取数字（用于排序）
function extractNumberFromFieldName(fieldName: string): number {
  const match = fieldName.match(/\d+/)
  return match ? parseInt(match[0], 10) : Infinity
}

// 显示隐藏列弹窗
async function showHiddenColumnsDialog() {
  if (!metadata.value?.table_name) return
  
  hiddenColumnsDialogVisible.value = true
  hiddenColumnsLoading.value = true
  
  try {
    const res = await fieldMappingApi.getHiddenColumnsDetail(metadata.value.table_name) as any
    // 按字段名中的数字从小到大排序
    const data = res.data || []
    data.sort((a: any, b: any) => {
      return extractNumberFromFieldName(a.fieldName) - extractNumberFromFieldName(b.fieldName)
    })
    hiddenColumnsDetail.value = data
  } catch (error) {
    ElMessage.error('获取隐藏列信息失败')
    hiddenColumnsDetail.value = []
  } finally {
    hiddenColumnsLoading.value = false
  }
}

// 显示某个隐藏的列
async function showHiddenColumn(fieldName: string) {
  if (!metadata.value?.table_name) return
  
  showingColumn.value = fieldName
  try {
    await fieldMappingApi.setColumnHidden(metadata.value.table_name, fieldName, false)
    
    // 更新本地状态
    hiddenColumns.value = hiddenColumns.value.filter(f => f !== fieldName)
    
    // 从弹窗列表中移除
    hiddenColumnsDetail.value = hiddenColumnsDetail.value.filter(item => item.fieldName !== fieldName)
    
    ElMessage.success('已显示该列')
    
    // 触发表格更新（不使用 tableKey++ 以保持滚动位置）
    dataList.value = [...dataList.value]
    
    // 重新初始化拖拽
    nextTick(() => {
      initColumnDrag()
    })
    notifyFieldDisplayConfigChanged(metadata.value.table_name)
    
    // 如果没有隐藏的列了，关闭弹窗
    if (hiddenColumnsDetail.value.length === 0) {
      hiddenColumnsDialogVisible.value = false
    }
  } catch (error) {
    ElMessage.error('操作失败')
  } finally {
    showingColumn.value = null
  }
}

// 一键显示所有隐藏的列
async function showAllHiddenColumns() {
  if (!metadata.value?.table_name || hiddenColumnsDetail.value.length === 0) return
  
  showingAllColumns.value = true
  try {
    // 获取所有隐藏列的字段名
    const fieldNames = hiddenColumnsDetail.value.map(item => item.fieldName)
    
    // 逐个显示（后端没有批量接口，所以逐个调用）
    for (const fieldName of fieldNames) {
      await fieldMappingApi.setColumnHidden(metadata.value.table_name, fieldName, false)
    }
    
    // 更新本地状态
    hiddenColumns.value = []
    hiddenColumnsDetail.value = []
    
    ElMessage.success(`已显示全部 ${fieldNames.length} 列`)
    
    // 关闭弹窗
    hiddenColumnsDialogVisible.value = false
    
    // 触发表格更新
    dataList.value = [...dataList.value]
    
    // 重新初始化拖拽
    nextTick(() => {
      initColumnDrag()
    })
    notifyFieldDisplayConfigChanged(metadata.value.table_name)
  } catch (error) {
    ElMessage.error('操作失败')
  } finally {
    showingAllColumns.value = false
  }
}

// 显示表头排序弹窗
function showColumnOrderDialog() {
  // 构建当前列顺序列表
  columnOrderList.value = sortedColumns.value.map(col => ({
    fieldName: col.name,
    cnName: fieldMappings.value[col.name] || ''
  }))
  
  // 清除选择状态
  selectedColumnIndices.value.clear()
  lastClickedColumnIndex.value = null
  
  columnOrderDialogVisible.value = true
  
  // 等待弹窗渲染后初始化拖拽
  nextTick(() => {
    initColumnOrderDrag()
  })
}

// 按字段编号排序 (f1, f2, ... f300)
async function sortColumnsByFieldNumber() {
  sortingByFieldNumber.value = true
  
  try {
    // 对 columnOrderList 按字段编号排序
    const sortedList = [...columnOrderList.value].sort((a, b) => {
      const numA = extractNumberFromFieldName(a.fieldName)
      const numB = extractNumberFromFieldName(b.fieldName)
      return numA - numB
    })
    
    // 更新列表
    columnOrderList.value = sortedList
    
    // 更新 columnOrder
    columnOrder.value = sortedList.map(item => item.fieldName)
    
    // 保存到数据库
    await saveColumnOrder()
    
    // 清除选择状态
    selectedColumnIndices.value.clear()
    lastClickedColumnIndex.value = null
    
    // 触发表格更新
    dataList.value = [...dataList.value]
    
    // 重新初始化主表格的拖拽
    nextTick(() => {
      initColumnDrag()
    })
    
    ElMessage.success('已按字段编号排序')
  } catch (error) {
    console.error('排序失败:', error)
    ElMessage.error('排序失败')
  } finally {
    sortingByFieldNumber.value = false
  }
}

// 获取表头排序行的样式类
function getColumnOrderRowClass({ row, rowIndex }: { row: any; rowIndex: number }): string {
  return selectedColumnIndices.value.has(rowIndex) ? 'selected-row' : ''
}

// 处理表头排序行点击（支持 Shift 和 Ctrl 多选）
function handleColumnOrderRowClick(row: any, column: any, event: MouseEvent) {
  const rowIndex = columnOrderList.value.findIndex(item => item.fieldName === row.fieldName)
  if (rowIndex === -1) return
  
  if (event.shiftKey && lastClickedColumnIndex.value !== null) {
    // Shift+点击：范围选择
    const start = Math.min(lastClickedColumnIndex.value, rowIndex)
    const end = Math.max(lastClickedColumnIndex.value, rowIndex)
    for (let i = start; i <= end; i++) {
      selectedColumnIndices.value.add(i)
    }
  } else if (event.ctrlKey || event.metaKey) {
    // Ctrl+点击：切换选中状态
    if (selectedColumnIndices.value.has(rowIndex)) {
      selectedColumnIndices.value.delete(rowIndex)
    } else {
      selectedColumnIndices.value.add(rowIndex)
    }
    lastClickedColumnIndex.value = rowIndex
  } else {
    // 普通点击：单选
    selectedColumnIndices.value.clear()
    selectedColumnIndices.value.add(rowIndex)
    lastClickedColumnIndex.value = rowIndex
  }
  
  // 触发响应式更新
  selectedColumnIndices.value = new Set(selectedColumnIndices.value)
  
  // 更新 DOM 上的选中样式
  updateColumnOrderRowSelection()
}

// 清除列选择
function clearColumnSelection() {
  selectedColumnIndices.value.clear()
  selectedColumnIndices.value = new Set()
  lastClickedColumnIndex.value = null
  updateColumnOrderRowSelection()
}

// 更新表格行的选中样式
function updateColumnOrderRowSelection() {
  const table = columnOrderTableRef.value?.$el
  if (!table) return
  
  const rows = table.querySelectorAll('.el-table__body-wrapper tbody tr')
  rows.forEach((row: Element, index: number) => {
    if (selectedColumnIndices.value.has(index)) {
      row.classList.add('selected-row')
    } else {
      row.classList.remove('selected-row')
    }
  })
}

// 初始化表头排序弹窗的拖拽功能
function initColumnOrderDrag() {
  const table = columnOrderTableRef.value?.$el
  if (!table) return
  
  const tbody = table.querySelector('.el-table__body-wrapper tbody')
  if (!tbody) return
  
  // 销毁旧的 Sortable 实例
  if (columnOrderSortable) {
    columnOrderSortable.destroy()
    columnOrderSortable = null
  }
  
  // 获取表格的滚动容器
  const scrollWrapper = table.querySelector('.el-table__body-wrapper .el-scrollbar__wrap') as HTMLElement
  const bodyWrapper = table.querySelector('.el-table__body-wrapper') as HTMLElement
  const scrollContainer = scrollWrapper || bodyWrapper
  
  // 自动滚动相关变量
  let scrollInterval: ReturnType<typeof setInterval> | null = null
  let isDragging = false
  let draggedElement: HTMLElement | null = null
  const scrollSensitivity = 80 // 距离边缘多少像素时开始滚动
  const minScrollSpeed = 3 // 最小滚动速度
  const maxScrollSpeed = 30 // 最大滚动速度
  
  // 停止自动滚动
  const stopAutoScroll = () => {
    if (scrollInterval) {
      clearInterval(scrollInterval)
      scrollInterval = null
    }
  }
  
  // 开始自动滚动循环
  const startAutoScroll = () => {
    if (scrollInterval) return
    
    scrollInterval = setInterval(() => {
      if (!isDragging || !scrollContainer) {
        stopAutoScroll()
        return
      }
      
      // 获取拖拽元素
      const ghostElement = document.querySelector('.sortable-ghost') as HTMLElement
      const chosenElement = document.querySelector('.sortable-chosen') as HTMLElement
      const dragElement = ghostElement || chosenElement || draggedElement
      
      if (!dragElement) return
      
      // 获取表格滚动区域的位置
      const containerRect = scrollContainer.getBoundingClientRect()
      const draggedRect = dragElement.getBoundingClientRect()
      const draggedCenterY = draggedRect.top + draggedRect.height / 2
      
      const distanceFromTop = draggedCenterY - containerRect.top
      const distanceFromBottom = containerRect.bottom - draggedCenterY
      
      let scrollDirection: 'up' | 'down' | null = null
      let intensity = 0
      
      if (distanceFromTop < scrollSensitivity && distanceFromTop > 0) {
        scrollDirection = 'up'
        intensity = 1 - (distanceFromTop / scrollSensitivity)
      } else if (distanceFromBottom < scrollSensitivity && distanceFromBottom > 0) {
        scrollDirection = 'down'
        intensity = 1 - (distanceFromBottom / scrollSensitivity)
      }
      
      if (scrollDirection && intensity > 0) {
        const speed = minScrollSpeed + (maxScrollSpeed - minScrollSpeed) * intensity
        const scrollAmount = scrollDirection === 'up' ? -speed : speed
        scrollContainer.scrollTop += scrollAmount
      }
    }, 16) // 约60fps
  }
  
  // 创建新的 Sortable 实例
  columnOrderSortable = Sortable.create(tbody, {
    animation: 150,
    handle: '.drag-handle',
    onStart: (evt: any) => {
      isDragging = true
      draggedElement = evt.item
      
      // 如果拖拽的行不在选中列表中，清除选择并只选中当前行
      const dragIndex = evt.oldIndex
      if (!selectedColumnIndices.value.has(dragIndex)) {
        selectedColumnIndices.value.clear()
        selectedColumnIndices.value.add(dragIndex)
        selectedColumnIndices.value = new Set(selectedColumnIndices.value)
        updateColumnOrderRowSelection()
      }
      
      startAutoScroll()
    },
    onMove: (evt: any) => {
      if (evt.dragged) {
        draggedElement = evt.dragged
      }
    },
    onChange: (evt: any) => {
      draggedElement = evt.item
    },
    onEnd: async (evt: any) => {
      isDragging = false
      draggedElement = null
      stopAutoScroll()
      
      const { oldIndex, newIndex } = evt
      if (oldIndex === newIndex) return
      
      // 获取选中的索引列表（排序后）
      const selectedIndices = Array.from(selectedColumnIndices.value).sort((a, b) => a - b)
      
      if (selectedIndices.length <= 1) {
        // 单行拖拽
        const list = [...columnOrderList.value]
        const [removed] = list.splice(oldIndex, 1)
        list.splice(newIndex, 0, removed)
        columnOrderList.value = list
      } else {
        // 多行拖拽
        const list = [...columnOrderList.value]
        
        // 提取选中的项
        const selectedItems = selectedIndices.map(i => list[i])
        
        // 从列表中移除选中的项（从后往前删除，避免索引变化）
        for (let i = selectedIndices.length - 1; i >= 0; i--) {
          list.splice(selectedIndices[i], 1)
        }
        
        // 计算新的插入位置
        // 需要考虑删除选中项后的索引变化
        let insertIndex = newIndex
        // 计算在 newIndex 之前被删除了多少个选中项
        const removedBefore = selectedIndices.filter(i => i < oldIndex).length
        // 调整插入位置
        if (newIndex > oldIndex) {
          // 向下移动：newIndex 需要减去在 oldIndex 和 newIndex 之间被删除的项数
          const removedBetween = selectedIndices.filter(i => i > oldIndex && i <= newIndex).length
          insertIndex = newIndex - removedBetween - (selectedIndices.length - 1)
        } else {
          // 向上移动：newIndex 需要减去在 newIndex 之前被删除的项数
          insertIndex = newIndex - selectedIndices.filter(i => i < newIndex).length
        }
        
        // 确保插入位置有效
        insertIndex = Math.max(0, Math.min(insertIndex, list.length))
        
        // 在新位置插入所有选中的项
        list.splice(insertIndex, 0, ...selectedItems)
        
        columnOrderList.value = list
        
        // 更新选中索引
        selectedColumnIndices.value.clear()
        for (let i = 0; i < selectedItems.length; i++) {
          selectedColumnIndices.value.add(insertIndex + i)
        }
        selectedColumnIndices.value = new Set(selectedColumnIndices.value)
      }
      
      // 更新 columnOrder
      columnOrder.value = columnOrderList.value.map(item => item.fieldName)
      
      // 保存到数据库
      await saveColumnOrder()
      
      // 触发表格更新
      dataList.value = [...dataList.value]
      
      // 重新初始化主表格的拖拽
      nextTick(() => {
        initColumnDrag()
        updateColumnOrderRowSelection()
      })
      
      ElMessage.success('列顺序已更新')
    }
  })
}

// 监听 columns 和 columnOrderLoaded 变化，初始化拖拽
watch([() => columns.value, columnOrderLoaded], ([cols, loaded]) => {
  if (isRawTable.value && cols.length > 0 && loaded) {
    initColumnDrag()
  }
}, { immediate: true })

onMounted(async () => {
  // 先加载元数据（包括字段映射和列顺序）
  await loadMetadata()
  // 再加载数据
  await loadData()
  // 标记页面准备完成
  pageReady.value = true
  
  // 监听 WebSocket 消息，更新完成后刷新数据
  unsubscribe = onMessage((message) => {
    // 只处理当前接口的消息
    if (message.metadataId === metadataId.value) {
      if (message.type === 'complete' || message.type === 'error') {
        updating.value = false
        loadData()
        loadMetadata()
        
        if (message.type === 'complete') {
          ElMessage.success(message.message || '更新完成')
        } else {
          ElMessage.error(message.message || '更新失败')
        }
      }
    }
  })
})

onUnmounted(() => {
  // 取消订阅 WebSocket 消息
  if (unsubscribe) {
    unsubscribe()
  }
  // 销毁 Sortable 实例
  if (columnSortable) {
    columnSortable.destroy()
    columnSortable = null
  }
  // 销毁表头排序弹窗的 Sortable 实例
  if (columnOrderSortable) {
    columnOrderSortable.destroy()
    columnOrderSortable = null
  }
})
</script>

<style scoped>
.data-view {
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

.header-right {
  display: flex;
  align-items: center;
  gap: 15px;
}

.field-stats {
  display: flex;
  gap: 10px;
  margin-bottom: 15px;
  padding: 10px 15px;
  background: #f5f7fa;
  border-radius: 4px;
}

.clickable-tag {
  cursor: pointer;
  transition: opacity 0.2s;
}

.clickable-tag:hover {
  opacity: 0.8;
}

.fields-list {
  max-height: 400px;
  overflow-y: auto;
}

.field-item {
  padding: 8px 12px;
  border-bottom: 1px solid #ebeef5;
  display: flex;
  align-items: center;
}

.field-item:last-child {
  border-bottom: none;
}

.field-index {
  color: #909399;
  margin-right: 10px;
  min-width: 30px;
}

.field-name {
  font-family: monospace;
  color: #303133;
}

.search-box {
  margin-bottom: 15px;
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

:deep(.highlight) {
  background-color: #ffeb3b;
  color: #333;
  padding: 0 2px;
  border-radius: 2px;
}

.pagination {
  margin-top: 20px;
  display: flex;
  justify-content: flex-end;
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
  text-align: left;
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

/* 展开行样式 */
.expand-content {
  padding: 15px 20px;
  background: #fafafa;
}

.expand-loading {
  display: flex;
  align-items: center;
  gap: 8px;
  color: #909399;
  padding: 20px;
  justify-content: center;
}

.expand-empty {
  color: #909399;
  text-align: center;
  padding: 20px;
}

:deep(.el-table__expanded-cell) {
  padding: 0 !important;
}

/* 官方API字段弹窗样式 */
.raw-fields-unsupported {
  padding: 20px;
}

.raw-fields-header {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 15px;
  padding: 10px;
  background: #f5f7fa;
  border-radius: 4px;
}

.unknown-field {
  color: #909399;
  font-style: italic;
}

.empty-value {
  color: #c0c4cc;
  font-style: italic;
}

/* 可编辑表头样式 */
.editable-header {
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  transition: color 0.2s;
}

.editable-header:hover {
  color: var(--el-color-primary);
}

.editable-header .edit-icon {
  font-size: 12px;
  opacity: 0;
  transition: opacity 0.2s;
}

.editable-header:hover .edit-icon {
  opacity: 1;
}

/* 列拖拽样式 */
:deep(.el-table__header-wrapper th) {
  cursor: grab;
}

:deep(.el-table__header-wrapper th:active) {
  cursor: grabbing;
}

:deep(.el-table__header-wrapper th.sortable-ghost) {
  background: #f1f5f9 !important;
  opacity: 0.9;
}

:deep(.el-table__header-wrapper th.sortable-chosen) {
  background: #e2e8f0 !important;
  box-shadow: none;
}

/* 表格边框优化 - 扁平化 */
:deep(.el-table) {
  border-radius: 6px;
  overflow: hidden;
  box-shadow: none;
  border: 1px solid #e2e8f0;
  /* 添加过渡效果，使拖拽后的显示更平滑 */
  transition: opacity 0.1s ease-out;
}

:deep(.el-table__body-wrapper) {
  border-radius: 0 0 6px 6px;
}

/* 表格单元格居中 */
:deep(.el-table .el-table__cell) {
  text-align: center;
}

:deep(.el-table .el-table__header .el-table__cell) {
  text-align: center;
}

/* 斑马纹优化 - 极淡 */
:deep(.el-table__row--striped td) {
  background-color: #fafafa !important;
}

/* 自定义表头布局样式 */
.custom-header {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  min-height: 24px;
  padding: 0 28px;
  box-sizing: border-box;
}

.header-center-group {
  position: relative;
  width: max-content;
  margin: 0 auto;
}

.header-edit-icon {
  font-size: 13px;
  color: #94a3b8;
  cursor: pointer;
  transition: color 0.15s;
  flex-shrink: 0;
}

.header-edit-icon:hover {
  color: #3b82f6;
}

.header-text {
  flex: 0 0 auto;
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: center;
  gap: 6px;
  flex-wrap: nowrap;
  line-height: 1.3;
  min-width: 0;
  width: max-content;
  margin: 0 auto;
}

.header-cn-name {
  flex: 0 0 auto;
  font-size: 13px;
  font-weight: 500;
  color: #334155;
  white-space: nowrap;
  overflow: visible;
  text-overflow: clip;
  max-width: none;
}

.header-field-name {
  flex: 0 0 auto;
  font-size: 12px;
  color: #64748b;
  font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace;
  white-space: nowrap;
  overflow: visible;
  text-overflow: clip;
  max-width: none;
}

.header-field-name.has-cn-name {
  font-size: 11px;
  color: #94a3b8;
}

.header-label {
  flex: 1;
  text-align: center;
  white-space: nowrap;
  overflow: visible;
  text-overflow: clip;
}

.header-sort-icon {
  font-size: 14px;
  color: #c0c4cc;
  cursor: pointer;
  transition: color 0.2s;
  flex-shrink: 0;
}

.header-side-icon {
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
}

.header-side-icon-left {
  right: calc(100% + 10px);
}

.header-side-icon-right {
  left: calc(100% + 10px);
}

.header-sort-icon:hover {
  color: #606266;
}

.header-sort-icon.is-active {
  color: var(--el-color-primary);
}

.header-sort-icon.is-asc,
.header-sort-icon.is-desc {
  color: var(--el-color-primary);
}

/* 字段推断提示样式 */
.field-suggestion {
  display: flex;
  align-items: center;
  gap: 10px;
}

.no-suggestion {
  color: #909399;
  font-size: 13px;
  font-style: italic;
}

/* 数值格式化开关样式 */
.format-switch {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.format-hint {
  color: #909399;
  font-size: 12px;
}

.unit-hint {
  color: #909399;
  font-size: 12px;
  margin-left: 8px;
}

/* 字段值分布分析样式 */
.field-values-loading {
  display: flex;
  align-items: center;
  gap: 8px;
  color: #909399;
  font-size: 13px;
}

.field-values-analysis {
  width: 100%;
}

.values-summary {
  margin-bottom: 8px;
}

.values-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.values-hint {
  color: #909399;
  font-size: 12px;
  margin-bottom: 4px;
}

.value-item {
  display: flex;
  align-items: center;
  gap: 8px;
}

.value-item.clickable {
  cursor: pointer;
  padding: 2px 4px;
  border-radius: 4px;
  transition: background-color 0.2s;
}

.value-item.clickable:hover {
  background-color: #ecf5ff;
}

.clickable-value-tag {
  cursor: pointer;
}

.clickable-value-tag:hover {
  opacity: 0.8;
}

.values-hint-click {
  font-size: 12px;
  color: #909399;
  margin-left: 8px;
}

.value-count {
  color: #909399;
  font-size: 12px;
}

/* 弹窗 footer 样式 */
.dialog-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}

.footer-right {
  display: flex;
  gap: 10px;
}

/* 隐藏列弹窗样式 */
.hidden-columns-loading {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 40px;
  color: #909399;
}

.hidden-columns-empty {
  padding: 20px;
}

.hidden-column-values {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.top-values {
  color: #606266;
  font-size: 12px;
}

.top-value-item {
  display: inline;
}

.no-cn-name {
  color: #c0c4cc;
}

/* 值映射配置样式 */
.value-mapping-config {
  width: 100%;
}

.value-mapping-hint {
  color: #909399;
  font-size: 12px;
  margin-bottom: 10px;
}

.value-mapping-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.value-mapping-item {
  display: flex;
  align-items: center;
  gap: 8px;
}

.value-mapping-item .original-value {
  min-width: 60px;
  text-align: center;
}

.value-mapping-item .mapping-arrow {
  color: #909399;
  font-size: 14px;
}

.value-mapping-item .value-count {
  color: #909399;
  font-size: 12px;
  margin-left: auto;
}

/* 表头排序弹窗样式 */
.column-order-hint {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 12px;
  margin-bottom: 10px;
  background: #f0f9eb;
  border-radius: 4px;
  color: #67c23a;
  font-size: 11px;
}

.column-order-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 5px 12px;
  margin-bottom: 8px;
  background: #ecf5ff;
  border-radius: 4px;
}

.column-order-actions .el-tag {
  font-size: 11px;
}

.column-order-actions .el-button {
  font-size: 11px;
  padding: 4px 8px;
}

/* 表头排序表格紧凑样式 */
:deep(.column-order-table) .el-table__header th {
  padding: 4px 0;
  font-size: 11px;
}

:deep(.column-order-table) .el-table__body td {
  padding: 2px 0;
  font-size: 11px;
}

:deep(.column-order-table) .el-table__cell {
  padding: 2px 4px;
}

:deep(.column-order-table) .cell {
  padding: 0 4px;
  line-height: 1.3;
}

:deep(.column-order-table) .el-tag {
  font-size: 10px;
  padding: 0 4px;
  height: 18px;
  line-height: 18px;
}

.drag-handle {
  cursor: grab;
  color: #909399;
  font-size: 12px;
}

.drag-handle:hover {
  color: #409eff;
}

.drag-handle:active {
  cursor: grabbing;
}

/* 拖拽时的行样式 */
:deep(.sortable-ghost) {
  background: #ecf5ff !important;
  opacity: 0.8;
}

:deep(.sortable-chosen) {
  background: #f5f7fa !important;
}

/* 选中行样式 */
:deep(.el-table__body tr.selected-row) {
  background-color: #e6f7ff !important;
}

:deep(.el-table__body tr.selected-row:hover > td) {
  background-color: #bae7ff !important;
}

:deep(.el-table__body tr.selected-row > td) {
  background-color: #e6f7ff !important;
}

/* 表头排序弹窗 footer 样式 */
.column-order-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}

/* 隐藏列弹窗 footer 样式 */
.hidden-columns-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}

/* 采集规则弹窗样式 */
.crawl-rules-content {
  line-height: 1.8;
}

.crawl-rules-content h4 {
  margin: 0 0 10px 0;
  color: #303133;
  font-size: 14px;
  border-left: 3px solid #409eff;
  padding-left: 10px;
}

.crawl-rules-content p {
  margin: 0 0 15px 0;
  color: #606266;
  font-size: 13px;
}

.crawl-rules-content .el-table {
  margin-bottom: 10px;
}
</style>
