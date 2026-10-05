<script setup lang="ts">
import { computed, onMounted, onUnmounted } from 'vue'
import { ElMessage } from 'element-plus'
import { useRouter } from 'vue-router'

import DeviceTable from '@/components/DeviceTable.vue'
import StatCard from '@/components/StatCard.vue'
import { useDeviceStore } from '@/stores/device'
import type { Device } from '@/types/api'

const router = useRouter()
const deviceStore = useDeviceStore()
let refreshTimer: ReturnType<typeof window.setInterval> | undefined

const total = computed(() => deviceStore.devices.length)
const offlineCount = computed(() => Math.max(0, total.value - deviceStore.onlineCount))

async function refresh() {
  try {
    await deviceStore.load({ limit: 100 })
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '读取设备失败')
  }
}

function openDevice(device: Device) {
  void router.push({ name: 'device-detail', params: { id: device.id } })
}

onMounted(() => {
  void refresh()
  refreshTimer = window.setInterval(() => void refresh(), 30_000)
})

onUnmounted(() => {
  if (refreshTimer !== undefined) window.clearInterval(refreshTimer)
})
</script>

<template>
  <div class="page">
    <div class="page-head">
      <div>
        <h1 class="page-title">运行总览</h1>
        <p class="page-description">查看当前项目设备在线状态和最近活动。</p>
      </div>
      <div class="page-actions">
        <el-button :loading="deviceStore.loading" @click="refresh">刷新</el-button>
        <el-button type="primary" @click="router.push({ name: 'devices' })">设备管理</el-button>
      </div>
    </div>

    <div class="stats-grid">
      <StatCard label="设备总数" :value="total" note="当前加载页" />
      <StatCard label="在线设备" :value="deviceStore.onlineCount" note="最近状态" />
      <StatCard label="离线设备" :value="offlineCount" note="需要关注" />
      <StatCard label="已启用" :value="deviceStore.activeCount" note="设备状态为 active" />
    </div>

    <section class="panel">
      <div class="panel-title-row">
        <h2 class="panel-title">设备状态</h2>
        <span class="stat-note">每 30 秒自动刷新</span>
      </div>
      <el-alert v-if="deviceStore.error" :title="deviceStore.error" type="error" show-icon :closable="false" />
      <DeviceTable :devices="deviceStore.devices" :loading="deviceStore.loading" @select="openDevice" />
    </section>
  </div>
</template>
