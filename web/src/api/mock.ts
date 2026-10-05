export const useMockData = import.meta.env.VITE_USE_MOCK !== 'false'

export const mockConfig = {
  enabled: useMockData,
  note: '显式设置 VITE_USE_MOCK=true 时，规则与告警列表使用浏览器本地 mock。',
}
