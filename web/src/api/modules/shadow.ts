import { request } from '@/api/client'
import type { ShadowSnapshot } from '@/types/api'

export function getShadow(deviceKey: string) {
  return request<{ ok: true; shadow: ShadowSnapshot }>({
    method: 'GET',
    url: `/shadows/${encodeURIComponent(deviceKey)}`,
  })
}

export function updateDesired(deviceKey: string, patch: Record<string, unknown>, expectedVersion?: number) {
  return request<{ ok: true; shadow: ShadowSnapshot }>({
    method: 'PATCH',
    url: `/shadows/${encodeURIComponent(deviceKey)}/desired`,
    data: { patch, expected_version: expectedVersion },
  })
}
