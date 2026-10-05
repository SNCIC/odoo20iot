<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

import { createEndpoint, deleteEndpoint, listEndpoints } from '@/api/modules/notify'
import type { NotificationEndpoint } from '@/types/api'
import { formatDate } from '@/utils/format'

const endpoints = ref<NotificationEndpoint[]>([])
const loading = ref(false)
const saving = ref(false)
const form = ref({ name: '', channel: 'webhook', target: '' })

async function load() {
  loading.value = true
  try {
    endpoints.value = (await listEndpoints()).endpoints
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '读取通知端点失败')
  } finally {
    loading.value = false
  }
}

async function save() {
  if (!form.value.name.trim() || !form.value.target.trim()) {
    ElMessage.warning('请填写名称和目标')
    return
  }
  saving.value = true
  try {
    const response = await createEndpoint({ name: form.value.name.trim(), channel: form.value.channel, target: form.value.target.trim() })
    endpoints.value.push(response.endpoint)
    form.value = { name: '', channel: 'webhook', target: '' }
    ElMessage.success('通知端点已创建')
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '创建通知端点失败')
  } finally {
    saving.value = false
  }
}

async function remove(endpoint: NotificationEndpoint) {
  try {
    await ElMessageBox.confirm(`确定停用“${endpoint.name}”吗？`, '停用通知端点', { type: 'warning' })
    await deleteEndpoint(endpoint.id)
    endpoints.value = endpoints.value.filter((item) => item.id !== endpoint.id)
    ElMessage.success('通知端点已停用')
  } catch (cause) {
    if (cause !== 'cancel' && cause !== 'close') ElMessage.error(cause instanceof Error ? cause.message : '停用失败')
  }
}

onMounted(() => void load())
</script>

<template>
  <div class="page">
    <div class="page-head"><div><h1 class="page-title">通知端点</h1><p class="page-description">配置邮件、短信、Webhook 和语音外呼的目标。</p></div><el-button :loading="loading" @click="load">刷新</el-button></div>
    <section class="panel">
      <h2 class="panel-title">新增端点</h2>
      <div class="endpoint-form">
        <el-input v-model="form.name" placeholder="端点名称" />
        <el-select v-model="form.channel" placeholder="通道"><el-option label="Webhook" value="webhook" /><el-option label="邮件" value="email" /><el-option label="短信" value="sms" /><el-option label="语音" value="voice" /></el-select>
        <el-input v-model="form.target" placeholder="Webhook URL、邮箱或手机号" />
        <el-button type="primary" :loading="saving" @click="save">保存</el-button>
      </div>
    </section>
    <section class="panel">
      <div class="panel-title-row"><h2 class="panel-title">已配置端点</h2><span class="stat-note">目标密文不会回显</span></div>
      <el-table v-loading="loading" :data="endpoints" stripe>
        <el-table-column label="名称" prop="name" min-width="180" />
        <el-table-column label="通道" prop="channel" width="110" />
        <el-table-column label="状态" width="90"><template #default="scope"><el-tag type="success" size="small">{{ scope.row.enabled ? '启用' : '停用' }}</el-tag></template></el-table-column>
        <el-table-column label="创建时间" min-width="170"><template #default="scope">{{ formatDate(scope.row.created_at) }}</template></el-table-column>
        <el-table-column label="操作" width="90"><template #default="scope"><el-button link type="danger" @click="remove(scope.row)">停用</el-button></template></el-table-column>
      </el-table>
      <div v-if="!loading && endpoints.length === 0" class="empty-state">暂无通知端点</div>
    </section>
  </div>
</template>
