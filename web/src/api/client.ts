import axios, { type AxiosError, type AxiosRequestConfig } from 'axios'

import type { ApiErrorBody } from '@/types/api'

export class ApiError extends Error {
  readonly code: string
  readonly status?: number

  constructor(message: string, code = 'UNKNOWN', status?: number) {
    super(message)
    this.name = 'ApiError'
    this.code = code
    this.status = status
  }
}

export const api = axios.create({
  baseURL: '/api/v1',
  timeout: 15_000,
  headers: { Accept: 'application/json' },
})

api.interceptors.request.use((config) => {
  const token = localStorage.getItem('iot_token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

api.interceptors.response.use(
  (response) => {
    const body = response.data as ApiErrorBody | undefined
    if (body?.ok === false) {
      throw new ApiError(body.message || body.error || '请求失败', body.code, response.status)
    }
    return response
  },
  (error: AxiosError<ApiErrorBody>) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('iot_token')
      window.dispatchEvent(new CustomEvent('iot:unauthorized'))
    }
    const body = error.response?.data
    const message = body?.message || body?.error || error.message || '网络请求失败'
    return Promise.reject(new ApiError(message, body?.code || 'NETWORK_ERROR', error.response?.status))
  },
)

export async function request<T>(config: AxiosRequestConfig): Promise<T> {
  const response = await api.request<T>(config)
  return response.data
}
