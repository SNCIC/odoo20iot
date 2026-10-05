<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'

import { acknowledgeAlarm, alarmsUseMock, listAlarms } from '@/api/modules/alarm'
import type { AlarmItem } from '@/types/api'
import { formatDate } from '@/utils/format'

const alarms = ref<AlarmItem[]>([])
const loading = ref(false)
const acknowledging = ref<string>()

async function load() {
  loading.value = true
  try {
    alarms.value = (await listAlarms()).alarms
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '读取告警失败')
  } finally {
    loading.value = false
  }
}

async function ack(alarm: AlarmItem) {
  acknowledging.value = alarm.id
  try {
    await acknowledgeAlarm(alarm.id)
    alarm.status = 'acknowledged'
    alarm.ack_at = new Date().toISOString()
    ElMessage.success('告警已确认')
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '确认告警失败')
  } finally {
    acknowledging.value = undefined
  }
}

onMounted(() => void load())
</script>

<template>
  <div class="page">
    <div class="page-head"><div><h1 class="page-title">告警管理</h1><p class="page-description">查看活动告警并记录确认状态。</p></div><el-button :loading="loading" @click="load">刷新</el-button></div>
    <el-alert v-if="alarmsUseMock()" title="当前通过 VITE_USE_MOCK=true 启用了本地告警 mock。" type="warning" show-icon :closable="false" />
    <section class="panel">
      <el-table v-loading="loading" :data="alarms" stripe>
        <el-table-column label="级别" width="80"><template #default="scope"><el-tag :type="scope.row.level === 'P1' ? 'danger' : scope.row.level === 'P2' ? 'warning' : 'info'" size="small">{{ scope.row.level }}</el-tag></template></el-table-column>
        <el-table-column label="告警内容" prop="message" min-width="220" />
        <el-table-column label="设备" prop="device_key" min-width="170" />
        <el-table-column label="规则" prop="rule_id" min-width="150" />
        <el-table-column label="状态" width="110"><template #default="scope"><el-tag :type="scope.row.status === 'active' ? 'danger' : 'success'" size="small">{{ scope.row.status === 'active' ? '活动' : '已确认' }}</el-tag></template></el-table-column>
        <el-table-column label="发生时间" min-width="170"><template #default="scope">{{ formatDate(scope.row.created_at) }}</template></el-table-column>
        <el-table-column label="操作" width="100"><template #default="scope"><el-button v-if="scope.row.status === 'active'" link type="primary" :loading="acknowledging === scope.row.id" @click="ack(scope.row)">确认</el-button><span v-else class="stat-note">已处理</span></template></el-table-column>
      </el-table>
      <div v-if="!loading && alarms.length === 0" class="empty-state">暂无告警</div>
    </section>
  </div>
</template>
