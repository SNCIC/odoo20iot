import type { RuleItem } from '@/types/api'
import { request } from '@/api/client'

const mockKey = 'iot_mock_rules'

const defaults: RuleItem[] = [
  { rule_id: 'temperature-high', rule_name: '温度超限告警', level: 'P1', enabled: true, priority: 10, version: 1, expr: 'temperature > 80', action: 'alarm.raise', action_params: { title: '温度超限' } },
  { rule_id: 'device-offline', rule_name: '设备离线告警', level: 'P2', enabled: true, priority: 20, version: 2, expr: 'online == false', action: 'alarm.raise', action_params: { title: '设备离线' } },
  { rule_id: 'voltage-low', rule_name: '电压过低提醒', level: 'P3', enabled: false, priority: 30, version: 1, expr: 'voltage < 180', action: 'alarm.raise', action_params: { title: '电压过低' } },
]

function readRules() {
  const raw = localStorage.getItem(mockKey)
  if (!raw) { localStorage.setItem(mockKey, JSON.stringify(defaults)); return [...defaults] }
  try { return JSON.parse(raw) as RuleItem[] } catch { return [...defaults] }
}
export function rulesUseMock() { return import.meta.env.VITE_USE_MOCK === 'true' }
export async function listRules() {
  if (!rulesUseMock()) return request<{ ok: true; rules: RuleItem[] }>({ method: 'GET', url: '/rules' })
  return { ok: true as const, rules: readRules(), mock: true }
}
export async function setRuleEnabled(ruleId: string, enabled: boolean) {
  if (!rulesUseMock()) return request<{ ok: true; rule: RuleItem }>({ method: 'PUT', url: `/rules/${encodeURIComponent(ruleId)}`, data: { enabled } })
  const rules = readRules().map((rule) => rule.rule_id === ruleId ? { ...rule, enabled, version: rule.version + 1 } : rule)
  localStorage.setItem(mockKey, JSON.stringify(rules))
  return { ok: true as const, rule: rules.find((rule) => rule.rule_id === ruleId) }
}
export async function deleteRule(ruleId: string) {
  if (!rulesUseMock()) return request<{ ok: true }>({ method: 'DELETE', url: `/rules/${encodeURIComponent(ruleId)}` })
  const rules = readRules().filter((rule) => rule.rule_id !== ruleId)
  localStorage.setItem(mockKey, JSON.stringify(rules))
  return { ok: true as const }
}
export interface RuleDraft {
  rule_id: string
  rule_name: string
  level: string
  enabled: boolean
  priority: number
  device_type_id: number
  expr: string
  action: string
  action_params: Record<string, unknown>
}
export async function saveRule(draft: RuleDraft) {
  if (!rulesUseMock()) return request<{ ok: true; rule: RuleItem }>({ method: 'POST', url: '/rules', data: draft })
  const rules = readRules()
  const existing = rules.findIndex((rule) => rule.rule_id === draft.rule_id)
  const item = { ...draft, version: existing >= 0 ? rules[existing].version + 1 : 1 }
  if (existing >= 0) rules[existing] = item; else rules.push(item)
  localStorage.setItem(mockKey, JSON.stringify(rules))
  return { ok: true as const, rule: item }
}
