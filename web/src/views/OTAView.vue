<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

import { createTask, getTaskDevices, listFirmwares, listTasks, registerFirmware, rollbackTask, startTask, uploadArtifact } from '@/api/modules/ota'
import { useDeviceStore } from '@/stores/device'
import type { Firmware, OTATask, OTATaskDevice } from '@/types/api'
import { formatDate, formatNumber } from '@/utils/format'

const deviceStore = useDeviceStore()
const firmwares = ref<Firmware[]>([])
const tasks = ref<OTATask[]>([])
const taskDevices = ref<OTATaskDevice[]>([])
const selectedTask = ref<OTATask>()
const loading = ref(false)
const uploading = ref(false)
const saving = ref(false)
const taskLoading = ref(false)
const fileInput = ref<HTMLInputElement>()
const selectedDevices = ref<string[]>([])
const artifact = ref<{ key: string; filename: string; size_bytes: number; sha256: string }>()
const firmwareForm = ref({ version: '', signature: '', signing_key_id: '', metadata: '{}' })
const taskForm = ref({ firmware_id: undefined as number | undefined, rollout: '1,10,50,100', success_threshold: 0.98, offline_ttl_hours: 24 })

async function load() {
  loading.value = true
  try {
    const [firmwareResponse, taskResponse] = await Promise.all([listFirmwares(), listTasks()])
    firmwares.value = firmwareResponse.firmwares ?? []
    tasks.value = taskResponse.tasks || []
    if (!taskForm.value.firmware_id) taskForm.value.firmware_id = firmwares.value[0]?.id
    await deviceStore.load({ limit: 100 })
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '读取 OTA 数据失败')
  } finally {
    loading.value = false
  }
}

function chooseFile() {
  fileInput.value?.click()
}

async function onFileChange(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  uploading.value = true
  try {
    const response = await uploadArtifact(file)
    artifact.value = response.artifact
    ElMessage.success('固件制品上传成功')
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '固件上传失败')
  } finally {
    uploading.value = false
    input.value = ''
  }
}

async function register() {
  if (!artifact.value || !firmwareForm.value.version.trim()) {
    ElMessage.warning('请先上传固件并填写版本号')
    return
  }
  let metadata: Record<string, unknown>
  try {
    metadata = JSON.parse(firmwareForm.value.metadata || '{}') as Record<string, unknown>
  } catch {
    ElMessage.error('metadata 必须是合法 JSON')
    return
  }
  saving.value = true
  try {
    const response = await registerFirmware({
      version: firmwareForm.value.version.trim(), filename: artifact.value.filename, object_key: artifact.value.key,
      size_bytes: artifact.value.size_bytes, sha256: artifact.value.sha256, signature: firmwareForm.value.signature.trim(),
      signing_key_id: firmwareForm.value.signing_key_id.trim(), metadata,
    })
    firmwares.value.unshift(response.firmware)
    taskForm.value.firmware_id = response.firmware.id
    artifact.value = undefined
    firmwareForm.value = { version: '', signature: '', signing_key_id: '', metadata: '{}' }
    ElMessage.success('固件已登记')
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '登记固件失败')
  } finally {
    saving.value = false
  }
}

async function create() {
  if (!taskForm.value.firmware_id || selectedDevices.value.length === 0) {
    ElMessage.warning('请选择固件和至少一台设备')
    return
  }
  const batches = taskForm.value.rollout.split(',').map((value) => Number(value.trim())).filter((value) => Number.isFinite(value))
  if (batches.length === 0 || batches[batches.length - 1] !== 100) {
    ElMessage.warning('灰度批次必须以 100 结束，例如 1,10,50,100')
    return
  }
  saving.value = true
  try {
    const response = await createTask({
      firmware_id: taskForm.value.firmware_id, device_keys: selectedDevices.value,
      rollout: { batches, success_threshold: taskForm.value.success_threshold },
      offline_ttl_seconds: taskForm.value.offline_ttl_hours * 3600,
    })
    tasks.value.unshift(response.task)
    selectedDevices.value = []
    ElMessage.success('OTA 任务已创建')
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '创建 OTA 任务失败')
  } finally {
    saving.value = false
  }
}

async function inspect(task: OTATask) {
  taskLoading.value = true
  selectedTask.value = task
  try {
    taskDevices.value = (await getTaskDevices(task.id)).devices ?? []
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '读取任务进度失败')
  } finally {
    taskLoading.value = false
  }
}

async function start(task: OTATask) {
  try {
    await ElMessageBox.confirm(`确定启动 OTA 任务 ${task.id} 吗？`, '启动 OTA', { type: 'warning' })
    const response = await startTask(task.id)
    Object.assign(task, response.task)
    ElMessage.success('OTA 任务已进入调度')
  } catch (cause) {
    if (cause !== 'cancel' && cause !== 'close') ElMessage.error(cause instanceof Error ? cause.message : '启动任务失败')
  }
}

async function rollback(task: OTATask) {
  const firmwareId = await ElMessageBox.prompt('请输入目标回滚固件 ID', '创建回滚任务', { inputPattern: /^\d+$/, inputErrorMessage: '请输入数字固件 ID' }).catch(() => undefined)
  if (!firmwareId) return
  try {
    await rollbackTask(task.id, Number(firmwareId.value), '由控制台发起回滚')
    ElMessage.success('回滚任务已创建')
    await load()
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '创建回滚任务失败')
  }
}

onMounted(() => void load())
</script>

<template>
  <div class="page">
    <div class="page-head"><div><h1 class="page-title">OTA 升级</h1><p class="page-description">上传签名固件、创建灰度任务并查看设备升级进度。</p></div><el-button :loading="loading" @click="load">刷新</el-button></div>
    <div class="ota-grid">
      <section class="panel">
        <h2 class="panel-title">固件制品</h2>
        <input ref="fileInput" type="file" hidden @change="onFileChange" />
        <el-button :loading="uploading" @click="chooseFile">上传固件</el-button>
        <div v-if="artifact" class="artifact-box"><strong>{{ artifact.filename }}</strong><span>{{ formatNumber(artifact.size_bytes) }} bytes · SHA256 {{ artifact.sha256 }}</span></div>
        <el-divider />
        <h3 class="sub-title">登记固件清单</h3>
        <el-form label-position="top">
          <el-form-item label="版本"><el-input v-model="firmwareForm.version" placeholder="例如 v2.0.1" /></el-form-item>
          <el-form-item label="签名"><el-input v-model="firmwareForm.signature" placeholder="设备端验签所需签名" /></el-form-item>
          <el-form-item label="签名 Key ID"><el-input v-model="firmwareForm.signing_key_id" /></el-form-item>
          <el-form-item label="Metadata JSON"><el-input v-model="firmwareForm.metadata" type="textarea" :rows="3" /></el-form-item>
          <el-button type="primary" :loading="saving" @click="register">登记固件</el-button>
        </el-form>
      </section>
      <section class="panel">
        <h2 class="panel-title">创建升级任务</h2>
        <el-form label-position="top">
          <el-form-item label="目标固件"><el-select v-model="taskForm.firmware_id" placeholder="选择固件" style="width: 100%"><el-option v-for="firmware in firmwares" :key="firmware.id" :label="`#${firmware.id} ${firmware.version}`" :value="firmware.id" /></el-select></el-form-item>
          <el-form-item label="目标设备"><el-select v-model="selectedDevices" multiple filterable collapse-tags placeholder="选择设备" style="width: 100%"><el-option v-for="device in deviceStore.devices" :key="device.id" :label="device.name || device.device_key" :value="device.device_key" /></el-select></el-form-item>
          <el-form-item label="灰度批次"><el-input v-model="taskForm.rollout" /></el-form-item>
          <el-form-item label="成功率阈值"><el-input-number v-model="taskForm.success_threshold" :min="0.01" :max="1" :step="0.01" /></el-form-item>
          <el-form-item label="离线任务 TTL（小时）"><el-input-number v-model="taskForm.offline_ttl_hours" :min="1" :max="720" /></el-form-item>
          <el-button type="primary" :loading="saving" @click="create">创建任务</el-button>
        </el-form>
      </section>
    </div>
    <section class="panel">
      <div class="panel-title-row"><h2 class="panel-title">固件列表</h2><span class="stat-note">{{ firmwares.length }} 个版本</span></div>
      <el-table :data="firmwares" stripe><el-table-column label="ID" prop="id" width="75" /><el-table-column label="版本" prop="version" width="130" /><el-table-column label="文件名" prop="filename" min-width="180" /><el-table-column label="大小" min-width="120"><template #default="scope">{{ formatNumber(scope.row.size_bytes) }} bytes</template></el-table-column><el-table-column label="登记时间" min-width="170"><template #default="scope">{{ formatDate(scope.row.created_at) }}</template></el-table-column></el-table>
    </section>
    <section class="panel">
      <div class="panel-title-row"><h2 class="panel-title">升级任务</h2><span class="stat-note">{{ tasks.length }} 个任务</span></div>
      <el-table :data="tasks" stripe><el-table-column label="任务 ID" prop="id" min-width="250" /><el-table-column label="固件 ID" prop="firmware_id" width="90" /><el-table-column label="状态" prop="status" width="110" /><el-table-column label="批次" prop="batch_index" width="75" /><el-table-column label="创建时间" min-width="170"><template #default="scope">{{ formatDate(scope.row.created_at) }}</template></el-table-column><el-table-column label="操作" min-width="230" fixed="right"><template #default="scope"><el-button link type="primary" @click="inspect(scope.row)">进度</el-button><el-button v-if="scope.row.status === 'draft'" link type="success" @click="start(scope.row)">启动</el-button><el-button v-if="scope.row.status !== 'completed'" link type="warning" @click="rollback(scope.row)">回滚</el-button></template></el-table-column></el-table>
      <div v-if="selectedTask" class="task-progress"><div class="panel-title-row"><h3 class="sub-title">任务 {{ selectedTask.id }} 设备进度</h3><span v-if="taskLoading">加载中…</span></div><el-table :data="taskDevices" size="small" stripe><el-table-column label="设备" prop="device_key" min-width="180" /><el-table-column label="状态" prop="status" width="130" /><el-table-column label="进度" width="180"><template #default="scope"><el-progress :percentage="scope.row.progress" /></template></el-table-column><el-table-column label="版本" prop="firmware_version" /><el-table-column label="更新时间" min-width="170"><template #default="scope">{{ formatDate(scope.row.updated_at) }}</template></el-table-column></el-table></div>
    </section>
  </div>
</template>
