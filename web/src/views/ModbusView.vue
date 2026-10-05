<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

import { disableModbusConfig, listModbusConfigs, saveModbusConfig } from '@/api/modules/modbus'
import type { ModbusConfig, ModbusPoint } from '@/types/api'

const supportedPointTypes = ['uint16', 'int16', 'uint32', 'int32', 'float32'] as const
const configs = ref<ModbusConfig[]>([])
const loading = ref(false)
const saving = ref(false)
const editingKey = ref<string>()

interface ModbusForm {
  enabled: boolean
  transport: 'tcp' | 'rtu'
  device_key: string
  device_id: number
  device_type_id: number
  endpoint: string
  serial_path: string
  baud_rate: number
  data_bits: number
  stop_bits: number
  parity: 'none' | 'even' | 'odd'
  unit_id: number
  interval_ms: number
  timeout_ms: number
  points_text: string
}

function defaultPoints() {
  return JSON.stringify([
    { name: 'temperature', address: 0, type: 'int16', scale: 0.1, unit: '°C' },
  ], null, 2)
}

function emptyForm(): ModbusForm {
  return {
    enabled: true,
    transport: 'tcp',
    device_key: '',
    device_id: 0,
    device_type_id: 0,
    endpoint: '',
    serial_path: '',
    baud_rate: 9600,
    data_bits: 8,
    stop_bits: 1,
    parity: 'none',
    unit_id: 1,
    interval_ms: 10_000,
    timeout_ms: 5_000,
    points_text: defaultPoints(),
  }
}

const form = ref<ModbusForm>(emptyForm())
const isRTU = computed(() => form.value.transport === 'rtu')
const formTitle = computed(() => editingKey.value ? `编辑配置：${editingKey.value}` : '新增或修改轮询配置')

function resetForm() {
  editingKey.value = undefined
  form.value = emptyForm()
}

function editConfig(config: ModbusConfig) {
  editingKey.value = config.device_key
  form.value = {
    enabled: config.enabled,
    transport: config.transport === 'rtu' ? 'rtu' : 'tcp',
    device_key: config.device_key,
    device_id: config.device_id || 0,
    device_type_id: config.device_type_id || 0,
    endpoint: config.endpoint || '',
    serial_path: config.serial_path || '',
    baud_rate: config.baud_rate || 9600,
    data_bits: config.data_bits || 8,
    stop_bits: config.stop_bits || 1,
    parity: config.parity === 'even' || config.parity === 'odd' ? config.parity : 'none',
    unit_id: config.unit_id || 1,
    interval_ms: config.interval_ms || 10_000,
    timeout_ms: config.timeout_ms || 5_000,
    points_text: JSON.stringify(config.points || [], null, 2),
  }
  window.scrollTo({ top: 0, behavior: 'smooth' })
}

function parsePoints(): ModbusPoint[] | undefined {
  let parsed: unknown
  try {
    parsed = JSON.parse(form.value.points_text)
  } catch {
    ElMessage.warning('点位 JSON 格式错误')
    return undefined
  }
  if (!Array.isArray(parsed) || parsed.length === 0) {
    ElMessage.warning('至少填写一个 Modbus 点位')
    return undefined
  }
  const points: ModbusPoint[] = []
  for (const item of parsed) {
    if (!item || typeof item !== 'object') {
      ElMessage.warning('点位必须是 JSON 对象')
      return undefined
    }
    const candidate = item as Record<string, unknown>
    const name = typeof candidate.name === 'string' ? candidate.name.trim() : ''
    const address = candidate.address
    const type = typeof candidate.type === 'string' ? candidate.type : ''
    if (!name || !Number.isInteger(address) || Number(address) < 0 || Number(address) > 65_535 || !supportedPointTypes.includes(type as typeof supportedPointTypes[number])) {
      ElMessage.warning('点位必须包含合法的 name、address 和 type')
      return undefined
    }
    const scale = candidate.scale === undefined ? undefined : Number(candidate.scale)
    const offset = candidate.offset === undefined ? undefined : Number(candidate.offset)
    if ((scale !== undefined && !Number.isFinite(scale)) || (offset !== undefined && !Number.isFinite(offset))) {
      ElMessage.warning(`点位 ${name} 的 scale/offset 必须是有限数字`)
      return undefined
    }
    points.push({
      name,
      address: Number(address),
      type,
      ...(scale === undefined ? {} : { scale }),
      ...(offset === undefined ? {} : { offset }),
      ...(typeof candidate.unit === 'string' && candidate.unit.trim() ? { unit: candidate.unit.trim() } : {}),
    })
  }
  return points
}

function validateForm(points: ModbusPoint[]) {
  const key = form.value.device_key.trim()
  if (!key || /[\s/#+]/.test(key) || key.length > 128) {
    return '设备 Key 必须填写，且不能包含空格、斜杠、# 或 +' 
  }
  if (isRTU.value) {
    if (!form.value.serial_path.trim().startsWith('/')) return 'RTU 串口路径必须是绝对路径'
  } else {
    const endpoint = form.value.endpoint.trim()
    const portMatch = endpoint.match(/:(\d+)$/)
    if (!portMatch || Number(portMatch[1]) < 1 || Number(portMatch[1]) > 65_535) return 'TCP 地址必须是 host:port 格式'
  }
  if (!Number.isInteger(form.value.unit_id) || form.value.unit_id < 1 || form.value.unit_id > 247) return 'Unit ID 必须在 1 到 247 之间'
  if (!Number.isInteger(form.value.interval_ms) || form.value.interval_ms < 100 || form.value.interval_ms > 86_400_000) return '轮询周期必须在 100ms 到 24h 之间'
  if (!Number.isInteger(form.value.timeout_ms) || form.value.timeout_ms < 100 || form.value.timeout_ms > 300_000) return '超时必须在 100ms 到 5m 之间'
  if (points.length > 128) return '点位最多 128 个'
  return ''
}

async function load() {
  loading.value = true
  try {
    configs.value = (await listModbusConfigs()).configs
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '读取 Modbus 配置失败')
  } finally {
    loading.value = false
  }
}

async function save() {
  const points = parsePoints()
  if (!points) return
  const validationError = validateForm(points)
  if (validationError) {
    ElMessage.warning(validationError)
    return
  }
  saving.value = true
  try {
    const response = await saveModbusConfig({
      enabled: form.value.enabled,
      transport: form.value.transport,
      device_key: form.value.device_key.trim(),
      device_id: form.value.device_id || 0,
      device_type_id: form.value.device_type_id || 0,
      endpoint: form.value.endpoint.trim(),
      serial_path: form.value.serial_path.trim(),
      baud_rate: form.value.baud_rate,
      data_bits: form.value.data_bits,
      stop_bits: form.value.stop_bits,
      parity: form.value.parity,
      unit_id: form.value.unit_id,
      interval_ms: form.value.interval_ms,
      timeout_ms: form.value.timeout_ms,
      points,
    })
    const index = configs.value.findIndex((item) => item.device_key === response.config.device_key)
    if (index >= 0) configs.value.splice(index, 1, response.config)
    else configs.value.push(response.config)
    ElMessage.success('Modbus 配置已保存，网关将在下次刷新时应用')
    resetForm()
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '保存 Modbus 配置失败')
  } finally {
    saving.value = false
  }
}

async function disable(config: ModbusConfig) {
  try {
    await ElMessageBox.confirm(`确定停用“${config.device_key}”的 Modbus 轮询吗？`, '停用 Modbus 配置', { type: 'warning' })
    await disableModbusConfig(config.device_key)
    config.enabled = false
    if (editingKey.value === config.device_key) form.value.enabled = false
    ElMessage.success('Modbus 配置已停用')
  } catch (cause) {
    if (cause !== 'cancel' && cause !== 'close') ElMessage.error(cause instanceof Error ? cause.message : '停用 Modbus 配置失败')
  }
}

function addressFor(config: ModbusConfig) {
  return config.transport === 'rtu' ? config.serial_path || '—' : config.endpoint || '—'
}

function pointNames(config: ModbusConfig) {
  const names = config.points.map((point) => point.name).filter(Boolean)
  return names.length > 3 ? `${names.slice(0, 3).join('、')} 等 ${names.length} 个` : names.join('、') || '—'
}

onMounted(() => void load())
</script>

<template>
  <div class="page">
    <div class="page-head">
      <div>
        <h1 class="page-title">Modbus 采集</h1>
        <p class="page-description">配置 Modbus TCP 或 Linux RTU 串口轮询；保存后由 `svc-modbus-gw` 读取。</p>
      </div>
      <el-button :loading="loading" @click="load">刷新</el-button>
    </div>

    <section class="panel">
      <div class="panel-title-row">
        <h2 class="panel-title">{{ formTitle }}</h2>
        <el-tag :type="isRTU ? 'warning' : 'primary'">{{ isRTU ? 'Modbus RTU' : 'Modbus TCP' }}</el-tag>
      </div>
      <div class="modbus-form-grid">
        <div class="modbus-field">
          <label>设备 Key</label>
          <el-input v-model="form.device_key" placeholder="例如 modbus-plc-1" :disabled="Boolean(editingKey)" />
          <span class="field-hint">应与平台设备或遥测目标的 device_key 一致。</span>
        </div>
        <div class="modbus-field">
          <label>传输方式</label>
          <el-select v-model="form.transport" style="width: 100%">
            <el-option label="Modbus TCP" value="tcp" />
            <el-option label="Modbus RTU 串口" value="rtu" />
          </el-select>
        </div>
        <div class="modbus-field">
          <label>设备 ID（可选）</label>
          <el-input-number v-model="form.device_id" :min="0" :step="1" controls-position="right" style="width: 100%" />
        </div>
        <div class="modbus-field">
          <label>设备类型 ID（可选）</label>
          <el-input-number v-model="form.device_type_id" :min="0" :step="1" controls-position="right" style="width: 100%" />
        </div>
        <div v-if="!isRTU" class="modbus-field">
          <label>TCP 地址</label>
          <el-input v-model="form.endpoint" placeholder="例如 192.168.1.20:502" />
        </div>
        <div v-else class="modbus-field">
          <label>串口路径</label>
          <el-input v-model="form.serial_path" placeholder="例如 /dev/ttyUSB0" />
        </div>
        <div class="modbus-field">
          <label>Unit ID</label>
          <el-input-number v-model="form.unit_id" :min="1" :max="247" :step="1" controls-position="right" style="width: 100%" />
        </div>
        <div class="modbus-field">
          <label>轮询周期（毫秒）</label>
          <el-input-number v-model="form.interval_ms" :min="100" :max="86400000" :step="100" controls-position="right" style="width: 100%" />
        </div>
        <div class="modbus-field">
          <label>超时（毫秒）</label>
          <el-input-number v-model="form.timeout_ms" :min="100" :max="300000" :step="100" controls-position="right" style="width: 100%" />
        </div>
        <template v-if="isRTU">
          <div class="modbus-field">
            <label>波特率</label>
            <el-select v-model="form.baud_rate" style="width: 100%"><el-option v-for="value in [1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200]" :key="value" :label="String(value)" :value="value" /></el-select>
          </div>
          <div class="modbus-field">
            <label>数据位</label>
            <el-select v-model="form.data_bits" style="width: 100%"><el-option v-for="value in [5, 6, 7, 8]" :key="value" :label="String(value)" :value="value" /></el-select>
          </div>
          <div class="modbus-field">
            <label>停止位</label>
            <el-select v-model="form.stop_bits" style="width: 100%"><el-option v-for="value in [1, 2]" :key="value" :label="String(value)" :value="value" /></el-select>
          </div>
          <div class="modbus-field">
            <label>校验位</label>
            <el-select v-model="form.parity" style="width: 100%"><el-option label="无" value="none" /><el-option label="偶校验" value="even" /><el-option label="奇校验" value="odd" /></el-select>
          </div>
        </template>
        <div class="modbus-field modbus-field-wide">
          <label>点位 JSON</label>
          <el-input v-model="form.points_text" type="textarea" :rows="9" spellcheck="false" placeholder='[{"name":"temperature","address":0,"type":"int16","scale":0.1,"unit":"°C"}]' />
          <span class="field-hint">支持 uint16、int16、uint32、int32、float32；地址不能重叠，单次读取跨度最多 125 个寄存器。</span>
        </div>
        <div class="modbus-field modbus-enabled">
          <label>运行状态</label>
          <el-switch v-model="form.enabled" active-text="启用轮询" inactive-text="停用" />
        </div>
      </div>
      <div class="page-actions modbus-actions">
        <el-button type="primary" :loading="saving" @click="save">保存配置</el-button>
        <el-button @click="resetForm">清空表单</el-button>
      </div>
    </section>

    <section class="panel">
      <div class="panel-title-row"><h2 class="panel-title">当前配置</h2><span class="stat-note">禁用配置仍保留，重新保存即可启用</span></div>
      <el-table v-loading="loading" :data="configs" stripe>
        <el-table-column label="设备 Key" prop="device_key" min-width="170" />
        <el-table-column label="方式" width="110"><template #default="scope">{{ scope.row.transport === 'rtu' ? 'RTU' : 'TCP' }}</template></el-table-column>
        <el-table-column label="地址" min-width="190"><template #default="scope"><span class="mono-text">{{ addressFor(scope.row) }}</span></template></el-table-column>
        <el-table-column label="Unit" width="75" prop="unit_id" />
        <el-table-column label="周期" width="100"><template #default="scope">{{ scope.row.interval_ms }} ms</template></el-table-column>
        <el-table-column label="点位" min-width="210"><template #default="scope">{{ pointNames(scope.row) }}</template></el-table-column>
        <el-table-column label="状态" width="90"><template #default="scope"><el-tag :type="scope.row.enabled ? 'success' : 'info'" size="small">{{ scope.row.enabled ? '启用' : '停用' }}</el-tag></template></el-table-column>
        <el-table-column label="操作" width="150" fixed="right"><template #default="scope"><el-button link type="primary" @click="editConfig(scope.row)">编辑</el-button><el-button link type="danger" :disabled="!scope.row.enabled" @click="disable(scope.row)">停用</el-button></template></el-table-column>
      </el-table>
      <div v-if="!loading && configs.length === 0" class="empty-state">暂无 Modbus 配置</div>
    </section>
  </div>
</template>
