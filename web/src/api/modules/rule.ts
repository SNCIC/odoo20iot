import type { RuleItem } from '@/types/api'
import { request } from '@/api/client'

const mockKey = 'iot_mock_rules'

const defaults: RuleItem[] = [
  { rule_id: 'temperature-high', rule_name: '温度超限告警', level: 'P1', enabled: true, priority: 10, version: 1 },
  { rule_id: 'device-offline', rule_name: '设备离线告警', level: 'P2', enabled: true, priority: 20, version: 2 },
  { rule_id: 'voltage-low', rule_name: '电压过低提醒', level: 'P3', enabled: false, priority: 30, version: 1 },
]

function readRules() {
  const raw = localStorage.getItem(mockKey)
  if (!raw) {
    localStorage.setItem(mockKey, JSON.stringify(defaults))
    return [...defaults]
  }
  try {
    return JSON.parse(raw) as RuleItem[]
  } catch {
    return [...defaults]
  }
}

export function rulesUseMock() {
  return import.meta.env.VITE_USE_MOCK === 'true'
}

export async function listRules() {
  if (!rulesUseMock()) return request<{ ok: true; rules: RuleItem[] }>({ method: 'GET', url: '/rules' })
  return { ok: true as const, rules: readRules(), mock: true }
}

export async function setRuleEnabled(ruleId: string, enabled: boolean) {
  if (!rulesUseMock()) {
    return request<{ ok: true; rule: RuleItem }>({ method: 'PUT', url: `/rules/${encodeURIComponent(ruleId)}`, data: { enabled } })
  }
  const rules = readRules().map((rule) => rule.rule_id === ruleId
    ? { ...rule, enabled, version: rule.version + 1 }
    : rule)
  localStorage.setItem(mockKey, JSON.stringify(rules))
  return { ok: true as const, rule: rules.find((rule) => rule.rule_id === ruleId) }
}
