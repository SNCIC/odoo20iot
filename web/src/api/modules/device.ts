import { request } from '@/api/client'
import type {
  DeviceQuery,
  DevicesResponse,
  LatestResponse,
  SeriesQuery,
  SeriesResponse,
} from '@/types/api'

export function listDevices(params: DeviceQuery = {}) {
  return request<DevicesResponse>({ method: 'GET', url: '/devices', params })
}

export function getLatest(deviceIds: number[]) {
  return request<LatestResponse>({
    method: 'GET',
    url: '/latest',
    params: { device_ids: deviceIds.join(',') },
  })
}

export function getSeries(query: SeriesQuery) {
  return request<SeriesResponse>({
    method: 'GET',
    url: '/series',
    params: {
      device_ids: query.device_ids.join(','),
      metric: query.metric,
      since: query.since,
      until: query.until,
      bucket: query.bucket,
      limit: query.limit,
    },
  })
}

export async function exportSeries(query: SeriesQuery) {
  const response = await request<Blob>({
    method: 'GET',
    url: '/export',
    params: {
      device_ids: query.device_ids.join(','),
      metric: query.metric,
      since: query.since,
      until: query.until,
      bucket: query.bucket,
      limit: query.limit,
    },
    responseType: 'blob',
  })
  return response
}
