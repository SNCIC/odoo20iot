<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useRoute } from 'vue-router'

import { getShadow, updateDesired } from '@/api/modules/shadow'
import type { ShadowSnapshot } from '@/types/api'
import { formatDate } from '@/utils/format'

const route = useRoute()
const snapshot = ref<ShadowSnapshot>()
const desiredText = ref('{}')
const patchText = ref('{}')
const loading = ref(false)
const saving = ref(false)
const error = ref('')
const deviceKey = String(route.params.deviceKey)

async function load() {
  loading.value = true
  error.value = ''
  try {
    const response = await getShadow(deviceKey)
    snapshot.value = response.shadow
    desiredText.value = JSON.stringify(response.shadow.desired, null, 2)
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : '读取设备影子失败'
  } finally {
    loading.value = false
  }
}

async function save() {
  let patch: Record<string, unknown>
  try {
    patch = JSON.parse(patchText.value) as Record<string, unknown>
  } catch {
    ElMessage.error('desired patch 必须是合法 JSON 对象')
    return
  }
  if (!patch || Array.isArray(patch) || typeof patch !== 'object') {
    ElMessage.error('desired patch 必须是 JSON 对象')
    return
  }
  saving.value = true
  try {
    const response = await updateDesired(deviceKey, patch, snapshot.value?.version)
    snapshot.value = response.shadow
    desiredText.value = JSON.stringify(response.shadow.desired, null, 2)
    patchText.value = '{}'
    ElMessage.success('desired 已下发')
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '更新 desired 失败')
  } finally {
    saving.value = false
  }
}

onMounted(() => void load())
</script>

<template>
  <div class="page">
    <div class="page-head">
      <div>
        <h1 class="page-title">设备影子</h1>
        <p class="page-description"><code>{{ deviceKey }}</code> · desired / reported 对照与下发。</p>
      </div>
      <el-button :loading="loading" @click="load">刷新</el-button>
    </div>
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <div v-if="snapshot" class="shadow-grid">
      <section class="panel">
        <div class="panel-title-row"><h2 class="panel-title">desired</h2><span class="stat-note">版本 {{ snapshot.version }}</span></div>
        <el-input v-model="desiredText" type="textarea" :rows="14" readonly />
        <div class="shadow-meta">最近更新：{{ formatDate(snapshot.desired_updated_at) }}</div>
      </section>
      <section class="panel">
        <div class="panel-title-row"><h2 class="panel-title">reported</h2><span class="stat-note">设备实际状态</span></div>
        <el-input :model-value="JSON.stringify(snapshot.reported, null, 2)" type="textarea" :rows="14" readonly />
        <div class="shadow-meta">最近更新：{{ formatDate(snapshot.reported_updated_at) }}</div>
      </section>
      <section class="panel shadow-full-width">
        <div class="panel-title-row"><h2 class="panel-title">下发 desired patch</h2><el-tag type="warning">会产生设备下行</el-tag></div>
        <el-input v-model="patchText" type="textarea" :rows="8" placeholder='例如：{"sampling_interval": 30}' />
        <div class="page-actions" style="margin-top: 14px"><el-button type="primary" :loading="saving" @click="save">保存并下发</el-button></div>
      </section>
    </div>
    <div v-else-if="loading" class="panel loading-state">影子加载中…</div>
  </div>
</template>
