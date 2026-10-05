<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'

import { listRules, rulesUseMock, setRuleEnabled } from '@/api/modules/rule'
import type { RuleItem } from '@/types/api'

const rules = ref<RuleItem[]>([])
const loading = ref(false)
const saving = ref<string>()

async function load() {
  loading.value = true
  try {
    rules.value = (await listRules()).rules
  } finally {
    loading.value = false
  }
}

async function toggle(rule: RuleItem, enabled: boolean) {
  saving.value = rule.rule_id
  try {
    const response = await setRuleEnabled(rule.rule_id, enabled)
    if (response.rule) Object.assign(rule, response.rule)
    ElMessage.success(`${rule.rule_name} 已${enabled ? '启用' : '停用'}`)
  } catch (cause) {
    rule.enabled = !enabled
    ElMessage.error(cause instanceof Error ? cause.message : '切换规则状态失败')
  } finally {
    saving.value = undefined
  }
}

onMounted(() => void load())
</script>

<template>
  <div class="page">
    <div class="page-head"><div><h1 class="page-title">规则管理</h1><p class="page-description">管理规则启停和优先级。规则编辑器将在后续阶段开放。</p></div><el-button :loading="loading" @click="load">刷新</el-button></div>
    <el-alert v-if="rulesUseMock()" title="规则 CRUD 接口尚未补齐，当前使用浏览器本地 mock 数据。" type="warning" show-icon :closable="false" />
    <section class="panel">
      <el-table v-loading="loading" :data="rules" stripe>
        <el-table-column label="规则名称" prop="rule_name" min-width="210" />
        <el-table-column label="规则 ID" prop="rule_id" min-width="170" />
        <el-table-column label="级别" prop="level" width="90" />
        <el-table-column label="优先级" prop="priority" width="90" />
        <el-table-column label="版本" prop="version" width="80" />
        <el-table-column label="状态" width="100"><template #default="scope"><el-switch v-model="scope.row.enabled" :loading="saving === scope.row.rule_id" @change="(value) => toggle(scope.row, Boolean(value))" /></template></el-table-column>
      </el-table>
      <div v-if="!loading && rules.length === 0" class="empty-state">暂无规则</div>
    </section>
  </div>
</template>
