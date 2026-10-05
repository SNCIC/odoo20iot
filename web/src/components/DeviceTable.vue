<script setup lang="ts">
import type { Device } from '@/types/api'
import { formatDate } from '@/utils/format'
import StatusBadge from '@/components/StatusBadge.vue'

defineProps<{
  devices: Device[]
  loading?: boolean
}>()

const emit = defineEmits<{
  select: [device: Device]
}>()

function selectDevice(device: Device) {
  emit('select', device)
}
</script>

<template>
  <div v-if="loading" class="loading-state">设备数据加载中…</div>
  <div v-else-if="devices.length === 0" class="empty-state">暂无匹配设备</div>
  <div v-else class="table-wrap">
    <el-table :data="devices" stripe table-layout="auto" @row-click="selectDevice">
      <el-table-column label="设备" min-width="210">
        <template #default="scope">
          <div><strong>{{ scope.row.name || '未命名设备' }}</strong></div>
          <div class="device-key">{{ scope.row.device_key }}</div>
        </template>
      </el-table-column>
      <el-table-column label="设备 ID" prop="id" width="100" />
      <el-table-column label="状态" width="100">
        <template #default="scope">
          <StatusBadge :online="scope.row.online" :status="scope.row.status" />
        </template>
      </el-table-column>
      <el-table-column label="运行状态" width="110">
        <template #default="scope">
          <el-tag :type="scope.row.status === 'active' ? 'success' : 'info'" size="small">
            {{ scope.row.status === 'active' ? '启用' : scope.row.status || '未知' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="认证方式" prop="auth_mode" width="120" />
      <el-table-column label="最近上报" min-width="170">
        <template #default="scope">{{ formatDate(scope.row.last_seen_at) }}</template>
      </el-table-column>
      <el-table-column label="操作" width="90" fixed="right">
        <template #default="scope">
          <el-button link type="primary" @click.stop="selectDevice(scope.row)">详情</el-button>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>
