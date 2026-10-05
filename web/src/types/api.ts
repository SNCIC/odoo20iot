export interface Device {
  id: number
  device_key: string
  name: string
  device_type_id: number
  status: string
  auth_mode: string
  online: boolean
  last_seen_at?: string
  tags?: Record<string, unknown>
  version: number
  created_at: string
  updated_at: string
}

export interface DevicesResponse {
  ok: boolean
  devices: Device[]
  next_cursor?: string
}

export interface LatestItem {
  device_id: number
  available: boolean
  ts?: string
  values?: Record<string, unknown>
}

export interface LatestResponse {
  ok: boolean
  latest: LatestItem[]
}

export interface SeriesPoint {
  ts: string
  device_id: number
  value: number | null
}

export interface SeriesBucket {
  bucket: string
  device_id: number
  avg: number | null
  max: number | null
  count: number
}

export interface SeriesResponse {
  ok: boolean
  granularity: string
  source: string
  bucket?: string
  cap_hit: boolean
  points?: SeriesPoint[]
  buckets?: SeriesBucket[]
}

export interface ApiErrorBody {
  ok?: false
  code?: string
  message?: string
  error?: string
}

export interface DeviceQuery {
  q?: string
  status?: string
  limit?: number
  cursor?: string
}

export interface SeriesQuery {
  device_ids: number[]
  metric: string
  since?: string
  until?: string
  bucket?: string
  limit?: number
}

export interface CommandRecord {
  project_id?: number
  command_id: string
  device_key: string
  command_key: string
  correlation_id: string
  payload: Record<string, unknown>
  status: string
  attempts: number
  last_error?: string
  issued_at: string
  acked_at?: string
  updated_at: string
}

export interface ShadowSnapshot {
  project_id?: number
  device_key: string
  desired: Record<string, unknown>
  reported: Record<string, unknown>
  delta: Record<string, unknown>
  version: number
  desired_updated_at: string
  reported_updated_at?: string
  updated_at: string
}

export interface Firmware {
  id: number
  project_id?: number
  version: string
  filename: string
  object_key: string
  size_bytes: number
  sha256: string
  signature: string
  signing_key_id: string
  metadata?: Record<string, unknown>
  created_at: string
}

export interface OTAArtifact {
  key: string
  filename: string
  size_bytes: number
  sha256: string
}

export interface OTARollout {
  batches: number[]
  success_threshold: number
}

export interface OTATask {
  id: string
  project_id?: number
  firmware_id: number
  status: string
  batch_index: number
  rollout: OTARollout
  offline_ttl: string | number
  created_by?: string
  created_at: string
  started_at?: string
  finished_at?: string
  rollback_of?: string
  rollback_reason?: string
  is_rollback?: boolean
}

export interface OTATaskDevice {
  task_id: string
  device_key: string
  status: string
  progress: number
  bytes_downloaded: number
  error_code?: string
  error_message?: string
  firmware_version?: string
  updated_at: string
}

export interface NotificationEndpoint {
  id: number
  project_id?: number
  name: string
  channel: 'webhook' | 'email' | 'sms' | 'voice' | string
  target?: string
  enabled: boolean
  created_at?: string
  updated_at?: string
}

export interface QuotaPolicy {
  project_id?: number
  metric: string
  soft_limit: number
  warning_limit: number
  hard_limit: number
  window: 'day' | 'month' | string
  enforcement: 'throttle' | 'reject' | string
}

export interface AlarmItem {
  id: string
  device_key: string
  rule_id: string
  level: string
  status: 'active' | 'acknowledged' | 'resolved' | string
  created_at: string
  ack_at?: string
  message: string
}

export interface RuleItem {
  rule_id: string
  rule_name: string
  level: string
  enabled: boolean
  priority: number
  version: number
}

export interface ModbusPoint {
  name: string
  address: number
  type: 'uint16' | 'int16' | 'uint32' | 'int32' | 'float32' | string
  scale?: number
  offset?: number
  unit?: string
}

export interface ModbusConfig {
  id?: number
  enabled: boolean
  transport: 'tcp' | 'rtu' | string
  device_key: string
  device_id?: number
  device_type_id?: number
  endpoint?: string
  serial_path?: string
  baud_rate: number
  data_bits: number
  stop_bits: number
  parity: 'none' | 'even' | 'odd' | string
  unit_id: number
  interval_ms: number
  timeout_ms: number
  points: ModbusPoint[]
}
