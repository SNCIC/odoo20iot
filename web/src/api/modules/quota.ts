import { request } from '@/api/client'
import type { QuotaPolicy } from '@/types/api'

export function listPolicies() {
  return request<{ ok: true; policies: QuotaPolicy[] | null }>({ method: 'GET', url: '/quota/policies' })
    .then((response) => ({ ...response, policies: response.policies ?? [] }))
}

export function savePolicy(policy: QuotaPolicy) {
  return request<{ ok: true; policy: QuotaPolicy }>({
    method: 'PUT',
    url: `/quota/policies/${encodeURIComponent(policy.metric)}`,
    data: policy,
  })
}
