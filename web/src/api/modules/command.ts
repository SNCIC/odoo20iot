import { request } from '@/api/client'
import type { CommandRecord } from '@/types/api'

export interface IssueCommandRequest {
  device_key: string
  command: string
  payload: Record<string, unknown>
  correlation_id?: string
}

export function issueCommand(data: IssueCommandRequest) {
  return request<{ ok: true; command: CommandRecord }>({
    method: 'POST',
    url: '/commands',
    data,
  })
}

export function getCommand(commandId: string) {
  return request<{ ok: true; command: CommandRecord }>({
    method: 'GET',
    url: `/commands/${encodeURIComponent(commandId)}`,
  })
}
