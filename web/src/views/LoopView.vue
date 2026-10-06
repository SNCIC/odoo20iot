<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { listMaintenanceRequests, type MaintenanceRequest } from '@/api/modules/odoo'
import { formatDate } from '@/utils/format'

const items = ref<MaintenanceRequest[]>([])
const deviceKey = ref('')
const loading = ref(false)
const enabled = ref(true)

async function load() {
  loading.value = true
  try {
    const result = await listMaintenanceRequests({ device_key: deviceKey.value || undefined, limit: 100 })
    items.value = result.maintenance_requests || result.workorders || []
    enabled.value = true
  } catch (cause) {
    enabled.value = false
    items.value = []
    if (cause instanceof Error) ElMessage.warning(`Odoo 单据 API 暂不可用：${cause.message}`)
  } finally {
    loading.value = false
  }
}

function stateLabel(state: string) {
  return ({ new: '新建', normal: '处理中', draft: '草稿', done: '已完成', cancel: '已取消' } as Record<string, string>)[state] || state || '未知'
}
function stateType(state: string) {
  return state === 'done' ? 'success' : state === 'cancel' ? 'info' : state === 'new' ? 'warning' : 'primary'
}
function severityType(severity: string) {
  return severity === 'critical' ? 'danger' : severity === 'warn' ? 'warning' : 'info'
}

onMounted(() => void load())
</script>

<template>
  <div class="page">
    <div class="page-head">
      <div><h1 class="page-title">业务闭环</h1><p class="page-description">设备异常 → IoT 告警 → Odoo 维修单 → 完工。</p></div>
      <el-button :loading="loading" @click="load">刷新</el-button>
    </div>
    <el-alert v-if="!enabled" title="Odoo 单据 API 尚未启用或暂时不可用，请检查 svc-query 的 ODOO_URL、ODOO_DB 和 ODOO_API_KEY 配置。" type="warning" show-icon :closable="false" />
    <section class="panel loop-filter">
      <el-input v-model="deviceKey" clearable placeholder="按设备 Key 筛选" style="max-width: 320px" @keyup.enter="load" />
      <el-button type="primary" @click="load">查询</el-button>
    </section>
    <section class="panel">
      <el-table v-loading="loading" :data="items" stripe>
        <el-table-column label="告警级别" width="105"><template #default="scope"><el-tag :type="severityType(scope.row.iot_severity)" size="small">{{ scope.row.iot_severity || '—' }}</el-tag></template></el-table-column>
        <el-table-column label="告警 ID" prop="iot_alarm_id" min-width="170" />
        <el-table-column label="设备" prop="iot_device_key" min-width="160" />
        <el-table-column label="Odoo 维修单" min-width="230"><template #default="scope"><strong>#{{ scope.row.id }}</strong> {{ scope.row.name }}</template></el-table-column>
        <el-table-column label="设备资产" prop="equipment_name" min-width="150" />
        <el-table-column label="状态" width="110"><template #default="scope"><el-tag :type="stateType(scope.row.state)" size="small">{{ stateLabel(scope.row.state) }}</el-tag></template></el-table-column>
        <el-table-column label="告警时间" min-width="170"><template #default="scope">{{ formatDate(scope.row.iot_alarm_ts || scope.row.create_date) }}</template></el-table-column>
        <el-table-column label="完工时间" min-width="120"><template #default="scope">{{ scope.row.close_date || '—' }}</template></el-table-column>
      </el-table>
      <div v-if="!loading && items.length === 0" class="empty-state">暂无 Odoo 维修单记录</div>
    </section>
  </div>
</template>

<style scoped>
.loop-filter { display: flex; gap: 12px; align-items: center; margin-bottom: 16px; }
</style>
