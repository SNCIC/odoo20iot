import { request } from '@/api/client'
import type { NotificationEndpoint } from '@/types/api'

function readField<T>(value: Record<string, unknown>, lower: string, upper: string): T | undefined {
  return (value[lower] ?? value[upper]) as T | undefined
}

function normalizeEndpoint(value: Record<string, unknown>): NotificationEndpoint {
  return {
    id: readField<number>(value, 'id', 'ID') || 0,
    project_id: readField<number>(value, 'project_id', 'ProjectID'),
    name: readField<string>(value, 'name', 'Name') || '',
    channel: readField<string>(value, 'channel', 'Channel') || 'webhook',
    enabled: readField<boolean>(value, 'enabled', 'Enabled') !== false,
    created_at: readField<string>(value, 'created_at', 'CreatedAt'),
    updated_at: readField<string>(value, 'updated_at', 'UpdatedAt'),
  }
}

export async function listEndpoints() {
  const response = await request<{ ok: true; endpoints: Record<string, unknown>[] }>({
    method: 'GET',
    url: '/notification-endpoints',
  })
  return { ...response, endpoints: (response.endpoints || []).map(normalizeEndpoint) }
}

export function createEndpoint(data: Pick<NotificationEndpoint, 'name' | 'channel'> & { target: string }) {
  return request<{ ok: true; endpoint: Record<string, unknown> }>({
    method: 'POST',
    url: '/notification-endpoints',
    data,
  }).then((response) => ({ ...response, endpoint: normalizeEndpoint(response.endpoint) }))
}

export function deleteEndpoint(id: number) {
  return request<{ ok: true }>({
    method: 'DELETE',
    url: `/notification-endpoints?id=${encodeURIComponent(id)}`,
  })
}
