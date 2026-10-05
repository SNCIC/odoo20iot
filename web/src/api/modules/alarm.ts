import { request } from '@/api/client'
import type { AlarmItem } from '@/types/api'

const mockKey = 'iot_mock_alarms'

function seedAlarms(): AlarmItem[] {
  return [
    {
      id: 'demo-alarm-1', device_key: 'demo-device-001', rule_id: 'temperature-high',
      level: 'P1', status: 'active', created_at: new Date(Date.now() - 12 * 60 * 1000).toISOString(),
      message: '温度超过安全阈值',
    },
    {
      id: 'demo-alarm-2', device_key: 'demo-device-002', rule_id: 'offline',
      level: 'P2', status: 'acknowledged', created_at: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
      ack_at: new Date(Date.now() - 90 * 60 * 1000).toISOString(), message: '设备长时间未上报',
    },
  ]
}

function readMockAlarms() {
  const raw = localStorage.getItem(mockKey)
  if (!raw) {
    const seeded = seedAlarms()
    localStorage.setItem(mockKey, JSON.stringify(seeded))
    return seeded
  }
  try {
    return JSON.parse(raw) as AlarmItem[]
  } catch {
    return seedAlarms()
  }
}

export function alarmsUseMock() {
  return import.meta.env.VITE_USE_MOCK === 'true'
}

export async function listAlarms() {
  if (alarmsUseMock()) return { ok: true as const, alarms: readMockAlarms(), mock: true }
  return request<{ ok: true; alarms: AlarmItem[] }>({ method: 'GET', url: '/alarms' })
}

export async function acknowledgeAlarm(id: string) {
  if (alarmsUseMock()) {
    const alarms = readMockAlarms().map((alarm) => alarm.id === id
      ? { ...alarm, status: 'acknowledged', ack_at: new Date().toISOString() }
      : alarm)
    localStorage.setItem(mockKey, JSON.stringify(alarms))
    return { ok: true as const, alarm_id: id, acknowledged: true }
  }
  return request<{ ok: true; alarm_id: string; acknowledged: boolean }>({
    method: 'POST',
    url: `/alarms/${encodeURIComponent(id)}/ack`,
  })
}
