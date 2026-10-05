import { request } from '@/api/client'
import type { Firmware, OTAArtifact, OTARollout, OTATask, OTATaskDevice } from '@/types/api'

export function listFirmwares() {
  return request<{ ok: true; firmwares: Firmware[] }>({ method: 'GET', url: '/ota/firmwares' })
}

export function uploadArtifact(file: File) {
  const data = new FormData()
  data.append('firmware', file)
  return request<{ ok: true; artifact: OTAArtifact; signed_manifest_required: boolean }>({
    method: 'POST',
    url: '/ota/artifacts',
    data,
  })
}

export interface RegisterFirmwareRequest {
  version: string
  filename: string
  object_key: string
  size_bytes: number
  sha256: string
  signature: string
  signing_key_id: string
  metadata?: Record<string, unknown>
}

export function registerFirmware(data: RegisterFirmwareRequest) {
  return request<{ ok: true; firmware: Firmware }>({ method: 'POST', url: '/ota/firmwares', data })
}

export function listTasks() {
  return request<{ ok: true; tasks: OTATask[] }>({ method: 'GET', url: '/ota/tasks' })
}

export interface CreateTaskRequest {
  firmware_id: number
  device_keys: string[]
  rollout: OTARollout
  offline_ttl_seconds?: number
}

export function createTask(data: CreateTaskRequest) {
  return request<{ ok: true; task: OTATask }>({ method: 'POST', url: '/ota/tasks', data })
}

export function getTask(taskId: string) {
  return request<{ ok: true; task: OTATask; devices?: OTATaskDevice[] }>({
    method: 'GET',
    url: `/ota/tasks/${encodeURIComponent(taskId)}`,
  })
}

export function getTaskDevices(taskId: string) {
  return request<{ ok: true; task: OTATask; devices: OTATaskDevice[] }>({
    method: 'GET',
    url: `/ota/tasks/${encodeURIComponent(taskId)}/devices`,
  })
}

export function startTask(taskId: string) {
  return request<{ ok: true; task: OTATask; scheduled: boolean }>({
    method: 'POST',
    url: `/ota/tasks/${encodeURIComponent(taskId)}/start`,
  })
}

export function rollbackTask(taskId: string, firmwareId: number, reason: string) {
  return request<{ ok: true; task: OTATask; scheduled: boolean }>({
    method: 'POST',
    url: `/ota/tasks/${encodeURIComponent(taskId)}/rollback`,
    data: { firmware_id: firmwareId, reason },
  })
}
