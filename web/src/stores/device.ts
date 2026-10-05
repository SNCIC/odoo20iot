import { computed, ref } from 'vue'
import { defineStore } from 'pinia'

import { listDevices } from '@/api/modules/device'
import type { Device, DeviceQuery } from '@/types/api'

export const useDeviceStore = defineStore('device', () => {
  const devices = ref<Device[]>([])
  const nextCursor = ref('')
  const loading = ref(false)
  const error = ref('')
  const onlineCount = computed(() => devices.value.filter((device) => device.online).length)
  const activeCount = computed(() => devices.value.filter((device) => device.status === 'active').length)

  async function load(query: DeviceQuery = {}, append = false) {
    loading.value = true
    error.value = ''
    try {
      const response = await listDevices(query)
      devices.value = append ? [...devices.value, ...response.devices] : response.devices
      nextCursor.value = response.next_cursor || ''
    } catch (cause) {
      error.value = cause instanceof Error ? cause.message : '读取设备失败'
      throw cause
    } finally {
      loading.value = false
    }
  }

  function findById(id: number) {
    return devices.value.find((device) => device.id === id)
  }

  return { devices, nextCursor, loading, error, onlineCount, activeCount, load, findById }
})
