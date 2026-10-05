<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { use } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { CanvasRenderer } from 'echarts/renderers'
import { GridComponent, TooltipComponent } from 'echarts/components'
import type { EChartsOption } from 'echarts'
import VChart from 'vue-echarts'
import { ElMessage } from 'element-plus'
import { useRoute, useRouter } from 'vue-router'

import { getCommand, issueCommand } from '@/api/modules/command'
import { exportSeries, getLatest, getSeries } from '@/api/modules/device'
import StatusBadge from '@/components/StatusBadge.vue'
import { useDeviceStore } from '@/stores/device'
import type { Device, SeriesResponse } from '@/types/api'
import { formatDate, formatNumber } from '@/utils/format'

use([LineChart, CanvasRenderer, GridComponent, TooltipComponent])

const route = useRoute()
const router = useRouter()
const deviceStore = useDeviceStore()
const loading = ref(false)
const latestLoading = ref(false)
const seriesLoading = ref(false)
const error = ref('')
const latestValues = ref<Record<string, unknown>>({})
const latestAt = ref<string>()
const selectedMetric = ref('temperature')
const timeRange = ref('24h')
const series = ref<SeriesResponse>()
const activeTab = ref('monitor')
const commandForm = ref({ command: '', payload: '{}' })
const commandRecord = ref<Awaited<ReturnType<typeof getCommand>>['command']>()
const commandLoading = ref(false)
const ingestForm = ref({ base: localStorage.getItem('iot_gateway_ingest_url') || '/ingest/v1', secret: '', payload: '{\n  "temperature": 25\n}' })
const ingestLoading = ref(false)
const debugMessages = ref<string[]>([])
let commandPollTimer: ReturnType<typeof window.setTimeout> | undefined
let refreshTimer: ReturnType<typeof window.setInterval> | undefined

const deviceId = computed(() => Number(route.params.id))
const device = computed<Device | undefined>(() => deviceStore.findById(deviceId.value))
const metricOptions = computed(() => {
  const values = new Set(['temperature', 'humidity', 'pressure', 'voltage', 'running'])
  Object.keys(latestValues.value).forEach((key) => values.add(key))
  return [...values]
})

const chartOption = computed<EChartsOption>(() => {
  const response = series.value
  const points = response?.points?.length
    ? response.points.map((point) => [point.ts, point.value])
    : (response?.buckets || []).map((bucket) => [bucket.bucket, bucket.avg ?? bucket.max])
  return {
    animation: false,
    grid: { left: 46, right: 22, top: 20, bottom: 42 },
    tooltip: { trigger: 'axis' },
    xAxis: {
      type: 'time',
      axisLabel: { color: '#718096' },
      axisLine: { lineStyle: { color: '#d9e1ec' } },
    },
    yAxis: {
      type: 'value',
      scale: true,
      axisLabel: { color: '#718096' },
      splitLine: { lineStyle: { color: '#edf1f6' } },
    },
    series: [{
      name: selectedMetric.value,
      type: 'line',
      showSymbol: points.length < 80,
      connectNulls: false,
      smooth: true,
      data: points,
      lineStyle: { color: '#2563eb', width: 2 },
      itemStyle: { color: '#2563eb' },
    }],
  }
})

function rangeStart() {
  const hours = timeRange.value === '7d' ? 24 * 7 : 24
  return new Date(Date.now() - hours * 60 * 60 * 1000).toISOString()
}

async function loadDevice() {
  loading.value = true
  error.value = ''
  try {
    if (!device.value) await deviceStore.load({ limit: 100 })
    if (!device.value) {
      error.value = '设备不存在，或当前令牌无权访问该设备'
      return
    }
    await Promise.all([loadLatest(), loadSeries()])
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : '读取设备详情失败'
  } finally {
    loading.value = false
  }
}

async function loadLatest() {
  if (!device.value) return
  latestLoading.value = true
  try {
    const response = await getLatest([device.value.id])
    const item = response.latest.find((entry) => entry.device_id === device.value?.id)
    latestValues.value = item?.values || {}
    latestAt.value = item?.ts
  } finally {
    latestLoading.value = false
  }
}

async function loadSeries() {
  if (!device.value) return
  seriesLoading.value = true
  try {
    series.value = await getSeries({
      device_ids: [device.value.id],
      metric: selectedMetric.value,
      since: rangeStart(),
      until: new Date().toISOString(),
      limit: 5000,
    })
  } finally {
    seriesLoading.value = false
  }
}

async function changeMetric() {
  try {
    await loadSeries()
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '读取时序数据失败')
  }
}

async function refresh() {
  try {
    await Promise.all([loadLatest(), loadSeries()])
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '刷新设备数据失败')
  }
}

async function downloadCsv() {
  if (!device.value) return
  try {
    const blob = await exportSeries({
      device_ids: [device.value.id],
      metric: selectedMetric.value,
      since: rangeStart(),
      until: new Date().toISOString(),
      limit: 5000,
    })
    const url = URL.createObjectURL(blob)
    const anchor = document.createElement('a')
    anchor.href = url
    anchor.download = `${device.value.device_key}-${selectedMetric.value}.csv`
    anchor.click()
    URL.revokeObjectURL(url)
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '导出失败')
  }
}

function addDebugMessage(message: string) {
  debugMessages.value = [`${formatDate(new Date())} ${message}`, ...debugMessages.value].slice(0, 20)
}

function isCommandDone(status?: string) {
  return status === 'acked' || status === 'succeeded' || status === 'failed' || status === 'expired' || status === 'cancelled'
}

async function pollCommand(commandId: string, attempt = 0) {
  try {
    const response = await getCommand(commandId)
    commandRecord.value = response.command
    if (!isCommandDone(response.command.status) && attempt < 15) {
      commandPollTimer = window.setTimeout(() => void pollCommand(commandId, attempt + 1), 2000)
    }
  } catch (cause) {
    addDebugMessage(cause instanceof Error ? `命令状态查询失败：${cause.message}` : '命令状态查询失败')
  }
}

async function sendCommand() {
  if (!device.value || !commandForm.value.command.trim()) {
    ElMessage.warning('请输入命令名')
    return
  }
  let payload: Record<string, unknown>
  try {
    payload = JSON.parse(commandForm.value.payload || '{}') as Record<string, unknown>
  } catch {
    ElMessage.error('命令 payload 必须是合法 JSON 对象')
    return
  }
  commandLoading.value = true
  if (commandPollTimer !== undefined) window.clearTimeout(commandPollTimer)
  try {
    const response = await issueCommand({ device_key: device.value.device_key, command: commandForm.value.command.trim(), payload })
    commandRecord.value = response.command
    addDebugMessage(`已下发 ${response.command.command_key}，命令 ID：${response.command.command_id}`)
    void pollCommand(response.command.command_id)
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '命令下发失败')
  } finally {
    commandLoading.value = false
  }
}

async function simulateIngest() {
  if (!device.value) return
  if (!ingestForm.value.secret.trim()) {
    ElMessage.warning('请输入设备密钥')
    return
  }
  try {
    JSON.parse(ingestForm.value.payload)
  } catch {
    ElMessage.error('模拟上报内容必须是合法 JSON')
    return
  }
  ingestLoading.value = true
  localStorage.setItem('iot_gateway_ingest_url', ingestForm.value.base.trim() || '/ingest/v1')
  const base = (ingestForm.value.base.trim() || '/ingest/v1').replace(/\/$/, '')
  try {
    const response = await fetch(`${base}/devices/${encodeURIComponent(device.value.device_key)}/telemetry`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Device-Secret': ingestForm.value.secret.trim() },
      body: ingestForm.value.payload,
    })
    const body = await response.json().catch(() => ({})) as { ok?: boolean; trace_id?: string; error?: string }
    if (!response.ok || body.ok !== true) throw new Error(body.error || `HTTP ${response.status}`)
    addDebugMessage(`模拟上报成功，trace_id：${body.trace_id || '—'}`)
    ElMessage.success('模拟上报已接收')
  } catch (cause) {
    addDebugMessage(cause instanceof Error ? `模拟上报失败：${cause.message}` : '模拟上报失败')
    ElMessage.error(cause instanceof Error ? cause.message : '模拟上报失败')
  } finally {
    ingestLoading.value = false
  }
}

function displayValue(value: unknown) {
  if (typeof value === 'number') return formatNumber(value)
  if (typeof value === 'boolean') return value ? '是' : '否'
  if (value === null || value === undefined) return '—'
  return String(value)
}

onMounted(() => {
  void loadDevice()
  refreshTimer = window.setInterval(() => void refresh(), 30_000)
})

onUnmounted(() => {
  if (refreshTimer !== undefined) window.clearInterval(refreshTimer)
  if (commandPollTimer !== undefined) window.clearTimeout(commandPollTimer)
})
</script>

<template>
  <div class="page">
    <div class="page-head">
      <div>
        <el-button text @click="router.back">← 返回设备列表</el-button>
        <h1 class="page-title" style="margin-top: 10px">设备详情</h1>
        <p class="page-description">查看设备最新值和时序数据。</p>
      </div>
      <div class="page-actions">
        <el-button :loading="loading" @click="refresh">刷新</el-button>
        <el-button :disabled="!device" @click="router.push({ name: 'shadow', params: { deviceKey: device?.device_key } })">设备影子</el-button>
        <el-button type="primary" :disabled="!device" @click="downloadCsv">导出 CSV</el-button>
      </div>
    </div>

    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />

    <el-tabs v-if="device" v-model="activeTab" class="device-tabs">
      <el-tab-pane label="监控" name="monitor">
        <div class="detail-layout">
          <section class="panel">
            <div class="device-title-row"><div><h2>{{ device.name || '未命名设备' }}</h2><p class="device-key">{{ device.device_key }}</p></div><StatusBadge :online="device.online" :status="device.status" /></div>
            <dl class="key-value-list"><dt>设备 ID</dt><dd>{{ device.id }}</dd><dt>设备类型</dt><dd>{{ device.device_type_id }}</dd><dt>认证方式</dt><dd>{{ device.auth_mode || '—' }}</dd><dt>运行状态</dt><dd>{{ device.status || '—' }}</dd><dt>最近上报</dt><dd>{{ formatDate(device.last_seen_at) }}</dd><dt>创建时间</dt><dd>{{ formatDate(device.created_at) }}</dd></dl>
          </section>
          <section class="panel"><div class="panel-title-row"><h2 class="panel-title">最新值</h2><span class="stat-note">{{ latestLoading ? '刷新中…' : formatDate(latestAt) }}</span></div><div v-if="Object.keys(latestValues).length" class="value-grid"><div v-for="(value, key) in latestValues" :key="key" class="value-card"><div class="value-key">{{ key }}</div><div class="value-number">{{ displayValue(value) }}</div></div></div><div v-else class="empty-state">暂无最新值</div></section>
        </div>
        <section class="panel"><div class="metric-toolbar"><div><h2 class="panel-title" style="margin-bottom: 4px">时序曲线</h2><span class="metric-toolbar-label">{{ series?.source || '等待数据' }} · {{ series?.granularity || '—' }}</span></div><div class="toolbar"><el-select v-model="selectedMetric" style="width: 150px" @change="changeMetric"><el-option v-for="metric in metricOptions" :key="metric" :label="metric" :value="metric" /></el-select><el-radio-group v-model="timeRange" size="small" @change="changeMetric"><el-radio-button label="24h">24 小时</el-radio-button><el-radio-button label="7d">7 天</el-radio-button></el-radio-group></div></div><div v-loading="seriesLoading" class="chart-container"><v-chart :option="chartOption" autoresize /></div><el-alert v-if="series?.cap_hit" title="数据量达到查询上限，曲线已按后端策略降采样。" type="warning" show-icon :closable="false" /></section>
      </el-tab-pane>
      <el-tab-pane label="调试台" name="debug">
        <section class="panel"><div class="panel-title-row"><h2 class="panel-title">命令试发</h2><el-tag type="warning">会产生设备下行</el-tag></div><div class="debug-form"><el-input v-model="commandForm.command" placeholder="命令名，例如 reboot" /><el-input v-model="commandForm.payload" type="textarea" :rows="5" placeholder='{"duration": 10}' /><el-button type="primary" :loading="commandLoading" @click="sendCommand">下发命令</el-button></div><div v-if="commandRecord" class="command-result"><div><strong>命令 ID：</strong>{{ commandRecord.command_id }}</div><div><strong>状态：</strong><el-tag size="small">{{ commandRecord.status }}</el-tag></div><div><strong>尝试次数：</strong>{{ commandRecord.attempts }}</div><div v-if="commandRecord.last_error"><strong>错误：</strong>{{ commandRecord.last_error }}</div></div></section>
        <section class="panel"><div class="panel-title-row"><h2 class="panel-title">模拟 HTTP 上报</h2><el-tag type="info">网关接入地址可配置</el-tag></div><div class="debug-form"><el-input v-model="ingestForm.base" placeholder="例如 /ingest/v1 或 http://网关:18080/ingest/v1" /><el-input v-model="ingestForm.secret" type="password" show-password placeholder="设备密钥" /><el-input v-model="ingestForm.payload" type="textarea" :rows="7" placeholder='{"temperature": 25}' /><el-button type="primary" :loading="ingestLoading" @click="simulateIngest">发送模拟上报</el-button></div></section>
        <section class="panel"><div class="panel-title-row"><h2 class="panel-title">调试操作记录</h2><span class="stat-note">仅保留当前页面最近 20 条</span></div><div v-if="debugMessages.length" class="debug-log"><div v-for="message in debugMessages" :key="message">{{ message }}</div></div><div v-else class="empty-state">暂无操作记录</div></section>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>
