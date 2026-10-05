export const useMockData = import.meta.env.VITE_USE_MOCK !== 'false'

export const mockConfig = {
  enabled: useMockData,
  note: '规则与告警列表在后端查询接口补齐前使用浏览器本地 mock。',
}
