import { computed, ref } from 'vue'
import { defineStore } from 'pinia'

export const useAuthStore = defineStore('auth', () => {
  const token = ref(localStorage.getItem('iot_token') || '')
  const authenticated = computed(() => token.value.length > 0)

  function setToken(value: string) {
    token.value = value.trim()
    if (token.value) {
      localStorage.setItem('iot_token', token.value)
    } else {
      localStorage.removeItem('iot_token')
    }
  }

  function logout() {
    setToken('')
  }

  return { token, authenticated, setToken, logout }
})
