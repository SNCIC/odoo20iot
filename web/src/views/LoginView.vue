<script setup lang="ts">
import { ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'

import { useAuthStore } from '@/stores/auth'

const router = useRouter()
const route = useRoute()
const auth = useAuthStore()
const token = ref(auth.token)
const loading = ref(false)

function submit() {
  const value = token.value.trim()
  if (!value) {
    ElMessage.warning('请输入 Bearer Token')
    return
  }
  loading.value = true
  auth.setToken(value)
  const redirect = typeof route.query.redirect === 'string' ? route.query.redirect : '/overview'
  void router.replace(redirect).finally(() => {
    loading.value = false
  })
}
</script>

<template>
  <div class="login-page">
    <section class="login-card">
      <div class="brand">
        <span class="brand-mark">IoT</span>
        <span>Odoo20IoT</span>
      </div>
      <h1>登录控制台</h1>
      <p>使用项目访问令牌进入设备管理与运维控制台。</p>

      <form class="login-form" @submit.prevent="submit">
        <el-input
          v-model="token"
          type="password"
          show-password
          size="large"
          autocomplete="off"
          placeholder="请输入 Bearer Token"
        />
        <el-button native-type="submit" type="primary" size="large" :loading="loading" block>
          进入控制台
        </el-button>
      </form>

      <p class="login-hint">
        Token 仅保存在当前浏览器的 localStorage 中，不会写入代码或服务端日志。若令牌过期，系统会自动返回登录页。
      </p>
    </section>
  </div>
</template>
