<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useRouter } from 'vue-router'

import DeviceTable from '@/components/DeviceTable.vue'
import { useDeviceStore } from '@/stores/device'
import type { Device } from '@/types/api'

const router = useRouter()
const deviceStore = useDeviceStore()
const search = ref('')
const status = ref('')

const hasMore = computed(() => Boolean(deviceStore.nextCursor))

async function loadDevices(append = false) {
  try {
    await deviceStore.load(
      {
        q: search.value.trim() || undefined,
        status: status.value || undefined,
        limit: 100,
        cursor: append ? deviceStore.nextCursor : undefined,
      },
      append,
    )
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '读取设备失败')
  }
}

function resetFilters() {
  search.value = ''
  status.value = ''
  void loadDevices()
}

function openDevice(device: Device) {
  void router.push({ name: 'device-detail', params: { id: device.id } })
}

onMounted(() => void loadDevices())
</script>

<template>
  <div class="page">
    <div class="page-head">
      <div>
        <h1 class="page-title">设备管理</h1>
        <p class="page-description">搜索设备、查看认证方式和最近在线状态。</p>
      </div>
      <div class="page-actions">
        <el-button :loading="deviceStore.loading" @click="loadDevices()">刷新</el-button>
      </div>
    </div>

    <section class="panel">
      <div class="filter-row">
        <el-input
          v-model="search"
          clearable
          placeholder="搜索设备名称或 device key"
          style="width: min(360px, 100%)"
          @keyup.enter="loadDevices()"
        />
        <el-select v-model="status" clearable placeholder="全部状态" style="width: 150px">
          <el-option label="启用" value="active" />
          <el-option label="停用" value="disabled" />
        </el-select>
        <el-button type="primary" @click="loadDevices()">查询</el-button>
        <el-button @click="resetFilters">重置</el-button>
      </div>
    </section>

    <section class="panel">
      <div class="panel-title-row">
        <h2 class="panel-title">设备列表</h2>
        <span class="stat-note">{{ deviceStore.devices.length }} 台</span>
      </div>
      <DeviceTable :devices="deviceStore.devices" :loading="deviceStore.loading" @select="openDevice" />
      <div v-if="hasMore" class="toolbar" style="justify-content: center; margin-top: 18px">
        <el-button :loading="deviceStore.loading" @click="loadDevices(true)">加载下一页</el-button>
      </div>
    </section>
  </div>
</template>
