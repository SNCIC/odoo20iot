import { request } from '@/api/client'
import type { ModbusConfig, ModbusPoint } from '@/types/api'

function readField<T>(value: Record<string, unknown>, lower: string, upper: string): T | undefined {
  return (value[lower] ?? value[upper]) as T | undefined
}

function normalizePoint(value: Record<string, unknown>): ModbusPoint {
  return {
    name: readField<string>(value, 'name', 'Name') || '',
    address: readField<number>(value, 'address', 'Address') || 0,
    type: readField<string>(value, 'type', 'Type') || 'int16',
    scale: readField<number>(value, 'scale', 'Scale'),
    offset: readField<number>(value, 'offset', 'Offset'),
    unit: readField<string>(value, 'unit', 'Unit'),
  }
}

function normalizeConfig(value: Record<string, unknown>): ModbusConfig {
  const rawPoints = readField<unknown[]>(value, 'points', 'Points') || []
  return {
    id: readField<number>(value, 'id', 'ID'),
    enabled: readField<boolean>(value, 'enabled', 'Enabled') !== false,
    transport: readField<string>(value, 'transport', 'Transport') || 'tcp',
    device_key: readField<string>(value, 'device_key', 'DeviceKey') || '',
    device_id: readField<number>(value, 'device_id', 'DeviceID') || 0,
    device_type_id: readField<number>(value, 'device_type_id', 'DeviceTypeID') || 0,
    endpoint: readField<string>(value, 'endpoint', 'Endpoint') || '',
    serial_path: readField<string>(value, 'serial_path', 'SerialPath') || '',
    baud_rate: readField<number>(value, 'baud_rate', 'BaudRate') || 9600,
    data_bits: readField<number>(value, 'data_bits', 'DataBits') || 8,
    stop_bits: readField<number>(value, 'stop_bits', 'StopBits') || 1,
    parity: readField<string>(value, 'parity', 'Parity') || 'none',
    unit_id: readField<number>(value, 'unit_id', 'UnitID') || 1,
    interval_ms: readField<number>(value, 'interval_ms', 'IntervalMS') || 10_000,
    timeout_ms: readField<number>(value, 'timeout_ms', 'TimeoutMS') || 5_000,
    points: rawPoints.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === 'object')).map(normalizePoint),
  }
}

export function listModbusConfigs() {
  return request<{ ok: true; configs: Record<string, unknown>[] }>({
    method: 'GET',
    url: '/modbus/configs',
  }).then((response) => ({ ...response, configs: (response.configs || []).map(normalizeConfig) }))
}

export function saveModbusConfig(config: ModbusConfig) {
  return request<{ ok: true; config: Record<string, unknown> }>({
    method: 'POST',
    url: '/modbus/configs',
    data: config,
  }).then((response) => ({ ...response, config: normalizeConfig(response.config) }))
}

export function disableModbusConfig(deviceKey: string) {
  return request<{ ok: true; device_key: string; enabled: boolean }>({
    method: 'DELETE',
    url: `/modbus/configs/${encodeURIComponent(deviceKey)}`,
  })
}
