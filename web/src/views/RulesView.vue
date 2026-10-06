<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { listRules, rulesUseMock, saveRule, setRuleEnabled, type RuleDraft } from '@/api/modules/rule'
import type { RuleItem } from '@/types/api'

const rules = ref<RuleItem[]>([])
const loading = ref(false)
const saving = ref<string>()
const editorVisible = ref(false)
const editing = ref(false)
const form = reactive<RuleDraft>({ rule_id: '', rule_name: '', level: 'P1', enabled: true, priority: 100, device_type_id: 1, expr: 'temperature > 80', action: 'alarm.raise', action_params: { title: '设备异常' } })

async function load() { loading.value = true; try { rules.value = (await listRules()).rules ?? [] } catch (cause) { ElMessage.error(cause instanceof Error ? cause.message : '读取规则失败') } finally { loading.value = false } }
function openNew() { Object.assign(form, { rule_id: '', rule_name: '', level: 'P1', enabled: true, priority: 100, device_type_id: 1, expr: 'temperature > 80', action: 'alarm.raise', action_params: { title: '设备异常' } }); editing.value = false; editorVisible.value = true }
function openEdit(rule: RuleItem) { Object.assign(form, { ...rule, device_type_id: rule.device_type_id || 1, expr: rule.expr || 'temperature > 80', action: rule.action || 'alarm.raise', action_params: rule.action_params || {} }); editing.value = true; editorVisible.value = true }
async function submit() {
  if (!form.rule_id.trim() || !form.rule_name.trim() || !form.expr.trim()) { ElMessage.warning('请填写规则 ID、名称和条件表达式'); return }
  if (form.action === 'command.send' && !String(form.action_params.command || '').trim()) { ElMessage.warning('command.send 必须填写 command 参数'); return }
  try { const response = await saveRule({ ...form, rule_id: form.rule_id.trim(), rule_name: form.rule_name.trim(), expr: form.expr.trim() }); const index = rules.value.findIndex((item) => item.rule_id === response.rule.rule_id); if (index >= 0) rules.value[index] = response.rule; else rules.value.push(response.rule); editorVisible.value = false; ElMessage.success(editing.value ? '规则已更新' : '规则已创建') } catch (cause) { ElMessage.error(cause instanceof Error ? cause.message : '保存规则失败') }
}
async function toggle(rule: RuleItem, enabled: boolean) { saving.value = rule.rule_id; try { const response = await setRuleEnabled(rule.rule_id, enabled); if (response.rule) Object.assign(rule, response.rule); ElMessage.success(`${rule.rule_name} 已${enabled ? '启用' : '停用'}`) } catch (cause) { rule.enabled = !enabled; ElMessage.error(cause instanceof Error ? cause.message : '切换规则状态失败') } finally { saving.value = undefined } }
function setParam(key: string, value: string) { form.action_params = { ...form.action_params, [key]: value } }
function param(key: string) { return String(form.action_params[key] || '') }
onMounted(() => void load())
</script>

<template>
  <div class="page">
    <div class="page-head"><div><h1 class="page-title">规则管理</h1><p class="page-description">可视化配置条件、优先级与自动动作，保存时仅允许内置动作白名单。</p></div><div class="page-actions"><el-button :loading="loading" @click="load">刷新</el-button><el-button type="primary" @click="openNew">新建规则</el-button></div></div>
    <el-alert v-if="rulesUseMock()" title="当前通过 VITE_USE_MOCK=true 启用了本地规则 mock。" type="warning" show-icon :closable="false" />
    <section class="panel"><el-table v-loading="loading" :data="rules" stripe><el-table-column label="规则名称" prop="rule_name" min-width="190" /><el-table-column label="条件" min-width="220"><template #default="scope"><code>{{ scope.row.expr || '未配置' }}</code></template></el-table-column><el-table-column label="动作" prop="action" width="150" /><el-table-column label="级别" prop="level" width="80" /><el-table-column label="优先级" prop="priority" width="80" /><el-table-column label="版本" prop="version" width="70" /><el-table-column label="状态" width="90"><template #default="scope"><el-switch v-model="scope.row.enabled" :loading="saving === scope.row.rule_id" @change="(value) => toggle(scope.row, Boolean(value))" /></template></el-table-column><el-table-column label="操作" width="80"><template #default="scope"><el-button link type="primary" @click="openEdit(scope.row)">编辑</el-button></template></el-table-column></el-table><div v-if="!loading && rules.length === 0" class="empty-state">暂无规则，点击右上角新建规则</div></section>
    <el-drawer v-model="editorVisible" :title="editing ? '编辑规则' : '新建规则'" size="520px"><el-form label-position="top"><el-form-item label="规则 ID"><el-input v-model="form.rule_id" :disabled="editing" placeholder="例如 temperature-high" /></el-form-item><el-form-item label="规则名称"><el-input v-model="form.rule_name" placeholder="例如温度超限告警" /></el-form-item><div class="form-grid"><el-form-item label="级别"><el-select v-model="form.level"><el-option label="P1 严重" value="P1" /><el-option label="P2 警告" value="P2" /><el-option label="P3 提示" value="P3" /></el-select></el-form-item><el-form-item label="优先级"><el-input-number v-model="form.priority" :min="0" :max="10000" /></el-form-item></div><el-form-item label="条件表达式"><el-input v-model="form.expr" type="textarea" :rows="3" placeholder="例如 temperature > 80 && humidity > 70" /><div class="field-tip">指标名来自设备类型物模型；取不到物模型时按宽松模式处理。</div></el-form-item><el-form-item label="自动动作"><el-select v-model="form.action" style="width: 100%"><el-option label="产生告警 alarm.raise" value="alarm.raise" /><el-option label="下发命令 command.send" value="command.send" /></el-select></el-form-item><el-form-item v-if="form.action === 'alarm.raise'" label="告警标题"><el-input :model-value="param('title')" @update:model-value="setParam('title', $event)" /></el-form-item><template v-if="form.action === 'command.send'"><el-form-item label="命令名"><el-input :model-value="param('command')" @update:model-value="setParam('command', $event)" placeholder="例如 reboot" /></el-form-item><el-form-item label="命令参数 JSON"><el-input :model-value="param('payload')" @update:model-value="setParam('payload', $event)" placeholder='例如 {"mode":"safe"}' /></el-form-item></template><el-form-item><el-checkbox v-model="form.enabled">保存后启用</el-checkbox></el-form-item><el-button type="primary" :loading="loading" @click="submit">保存规则</el-button></el-form></el-drawer>
  </div>
</template>
