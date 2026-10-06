import { request } from '@/api/client'

export interface MaintenanceRequest {
  id: number
  name: string
  state: string
  priority: string
  maintenance_type: string
  equipment_id: number
  equipment_name: string
  iot_alarm_id: string
  iot_severity: string
  iot_device_key: string
  iot_metric_snapshot?: Record<string, unknown>
  iot_alarm_ts: string
  create_date: string
  write_date: string
  close_date: string
  company_id: number
}

export interface MaintenanceQuery {
  device_key?: string
  alarm_id?: string
  state?: string
  limit?: number
}

export function listMaintenanceRequests(query: MaintenanceQuery = {}) {
  return request<{ ok: true; maintenance_requests: MaintenanceRequest[]; workorders: MaintenanceRequest[] }>({
    method: 'GET', url: '/odoo/maintenance-requests', params: query,
  })
}

export function listWorkorders(query: MaintenanceQuery = {}) {
  return request<{ ok: true; maintenance_requests: MaintenanceRequest[]; workorders: MaintenanceRequest[] }>({
    method: 'GET', url: '/odoo/workorders', params: query,
  })
}

export function getMaintenanceRequest(id: number) {
  return request<{ ok: true; maintenance_request: MaintenanceRequest; workorder: MaintenanceRequest }>({
    method: 'GET', url: `/odoo/maintenance-requests/${id}`,
  })
}
