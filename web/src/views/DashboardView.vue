<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import draggable from 'vuedraggable'
import { ElMessage } from 'element-plus'

import { getLatest, getSeries } from '@/api/modules/device'
import MetricChart from '@/components/MetricChart.vue'
import { useDeviceStore } from '@/stores/device'
import type { Device, LatestItem, SeriesResponse } from '@/types/api'
import { formatDate, formatNumber } from '@/utils/format'

type WidgetType = 'chart' | 'number' | 'gauge' | 'table'
interface DashboardWidget {
  id: string
  type: WidgetType
  title: string
  deviceId?: number
  metric: string
}

const storageKey = 'iot_dashboard_layout'
const deviceStore = useDeviceStore()
const widgets = ref<DashboardWidget[]>(loadLayout())
const seriesByWidget = ref<Record<string, SeriesResponse>>({})
const latestByDevice = ref<Record<number, LatestItem>>({})
const loading = ref(false)
const metricOptions = ['temperature', 'humidity', 'pressure', 'voltage', 'running']

const devices = computed(() => deviceStore.devices)

function loadLayout(): DashboardWidget[] {
  const raw = localStorage.getItem(storageKey)
  if (raw) {
    try {
      return JSON.parse(raw) as DashboardWidget[]
    } catch {
      localStorage.removeItem(storageKey)
    }
  }
  return [
    { id: 'overview-chart', type: 'chart', title: '温度趋势', metric: 'temperature' },
    { id: 'overview-number', type: 'number', title: '在线设备', metric: 'online' },
    { id: 'overview-gauge', type: 'gauge', title: '平均湿度', metric: 'humidity' },
    { id: 'overview-table', type: 'table', title: '设备状态', metric: 'online' },
  ]
}

function persist() {
  localStorage.setItem(storageKey, JSON.stringify(widgets.value))
}

function defaultDeviceId() {
  return devices.value[0]?.id
}

function addWidget(type: WidgetType) {
  const titles: Record<WidgetType, string> = { chart: '新趋势图', number: '新数字卡', gauge: '新仪表盘', table: '新数据表' }
  widgets.value.push({
    id: `widget-${Date.now()}`,
    type,
    title: titles[type],
    metric: type === 'number' || type === 'table' ? 'online' : 'temperature',
    deviceId: defaultDeviceId(),
  })
  persist()
  void refreshWidget(widgets.value[widgets.value.length - 1])
}

function removeWidget(widget: DashboardWidget) {
  widgets.value = widgets.value.filter((item) => item.id !== widget.id)
  const next = { ...seriesByWidget.value }
  delete next[widget.id]
  seriesByWidget.value = next
  persist()
}

function latestValue(widget: DashboardWidget) {
  if (widget.metric === 'online') return devices.value.filter((device) => device.online).length
  const deviceId = widget.deviceId || defaultDeviceId()
  const value = latestByDevice.value[deviceId || 0]?.values?.[widget.metric]
  return typeof value === 'number' ? formatNumber(value) : value === undefined ? '—' : String(value)
}

function numericValue(widget: DashboardWidget) {
  const value = latestValue(widget)
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : 0
}

async function refreshWidget(widget: DashboardWidget) {
  if (widget.type === 'chart') {
    const deviceId = widget.deviceId || defaultDeviceId()
    if (!deviceId) return
    const response = await getSeries({
      device_ids: [deviceId], metric: widget.metric,
      since: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
      until: new Date().toISOString(), limit: 1000,
    })
    seriesByWidget.value = { ...seriesByWidget.value, [widget.id]: response }
  }
  persist()
}

async function refresh() {
  loading.value = true
  try {
    await deviceStore.load({ limit: 100 })
    const ids = devices.value.map((device) => device.id).slice(0, 50)
    if (ids.length) {
      const latest = await getLatest(ids)
      latestByDevice.value = Object.fromEntries(latest.latest.map((item) => [item.device_id, item]))
    }
    await Promise.all(widgets.value.filter((widget) => widget.type === 'chart').map(refreshWidget))
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '看板数据加载失败')
  } finally {
    loading.value = false
  }
}

function deviceName(deviceId?: number) {
  return devices.value.find((device) => device.id === deviceId)?.name || devices.value.find((device) => device.id === deviceId)?.device_key || '未选择设备'
}

function deviceStatus(device: Device) {
  return device.online ? '在线' : '离线'
}

onMounted(() => void refresh())
</script>

<template>
  <div class="page">
    <div class="page-head">
      <div>
        <h1 class="page-title">数据看板</h1>
        <p class="page-description">拖拽卡片调整布局，配置保存在当前浏览器。</p>
      </div>
      <div class="page-actions">
        <el-dropdown split-button type="primary" @click="addWidget('chart')">
          添加组件
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item @click="addWidget('chart')">折线图</el-dropdown-item>
              <el-dropdown-item @click="addWidget('number')">数字卡</el-dropdown-item>
              <el-dropdown-item @click="addWidget('gauge')">仪表盘</el-dropdown-item>
              <el-dropdown-item @click="addWidget('table')">设备表格</el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
        <el-button :loading="loading" @click="refresh">刷新</el-button>
      </div>
    </div>

    <el-alert title="M1 配置保存在浏览器 localStorage；后端持久化将在二期接入。" type="info" show-icon :closable="false" />
    <draggable v-model="widgets" item-key="id" handle=".widget-handle" class="dashboard-grid" @end="persist">
      <template #item="{ element: widget }">
        <section class="panel dashboard-widget">
          <div class="widget-head">
            <div class="widget-title"><span class="widget-handle">☷</span><el-input v-model="widget.title" size="small" @change="persist" /></div>
            <div class="toolbar">
              <el-button text type="danger" @click="removeWidget(widget)">删除</el-button>
            </div>
          </div>
          <div v-if="widget.type === 'chart' || widget.type === 'gauge'" class="widget-config">
            <el-select v-model="widget.deviceId" clearable placeholder="选择设备" size="small" @change="refreshWidget(widget)">
              <el-option v-for="device in devices" :key="device.id" :label="device.name || device.device_key" :value="device.id" />
            </el-select>
            <el-select v-model="widget.metric" size="small" @change="refreshWidget(widget)">
              <el-option v-for="metric in metricOptions" :key="metric" :label="metric" :value="metric" />
            </el-select>
          </div>
          <MetricChart v-if="widget.type === 'chart'" :response="seriesByWidget[widget.id]" :name="widget.metric" height="240px" />
          <div v-else-if="widget.type === 'number'" class="number-widget">
            <div class="dashboard-number">{{ deviceStore.onlineCount }}</div>
            <div class="stat-note">在线设备 / {{ devices.length }} 台</div>
          </div>
          <div v-else-if="widget.type === 'gauge'" class="gauge-widget">
            <div class="gauge-ring" :style="{ '--gauge-value': `${Math.min(100, Math.max(0, numericValue(widget)))}%` }"><span>{{ latestValue(widget) }}%</span></div>
            <div class="stat-note">{{ deviceName(widget.deviceId) }} · {{ widget.metric }}</div>
          </div>
          <el-table v-else :data="devices.slice(0, 6)" size="small" stripe>
            <el-table-column label="设备" min-width="150"><template #default="scope">{{ scope.row.name || scope.row.device_key }}</template></el-table-column>
            <el-table-column label="状态" width="80"><template #default="scope">{{ deviceStatus(scope.row) }}</template></el-table-column>
            <el-table-column label="最近上报" prop="last_seen_at" min-width="145" />
          </el-table>
    </section>
      </template>
    </draggable>
  </div>
</template>
