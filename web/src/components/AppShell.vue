<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { useAuthStore } from '@/stores/auth'

const router = useRouter()
const route = useRoute()
const auth = useAuthStore()
const sidebarOpen = ref(false)

const menuItems = [
  { name: 'overview', label: '运行总览', icon: '▦' },
  { name: 'devices', label: '设备管理', icon: '◈' },
  { name: 'dashboards', label: '数据看板', icon: '▥' },
  { name: 'alarms', label: '告警管理', icon: '⚠' },
  { name: 'rules', label: '规则管理', icon: '◇' },
  { name: 'ota', label: 'OTA 升级', icon: '⇧' },
  { name: 'modbus', label: 'Modbus 采集', icon: '▣' },
  { name: 'notify', label: '通知端点', icon: '✉' },
  { name: 'quota', label: '配额策略', icon: '◫' },
  { name: 'loop', label: '业务闭环', icon: '↻' },
] as const

const activeMenu = computed(() => {
  if (route.name === 'device-detail') return 'devices'
  if (route.name === 'shadow') return 'devices'
  return String(route.name || 'overview')
})

function navigate(name: string) {
  sidebarOpen.value = false
  void router.push({ name })
}

function logout() {
  auth.logout()
  void router.replace({ name: 'login' })
}

function handleUnauthorized() {
  if (route.name !== 'login') {
    void router.replace({ name: 'login', query: { redirect: route.fullPath } })
  }
}

onMounted(() => window.addEventListener('iot:unauthorized', handleUnauthorized))
onUnmounted(() => window.removeEventListener('iot:unauthorized', handleUnauthorized))
</script>

<template>
  <div class="app-shell">
    <header class="topbar">
      <div class="brand">
        <el-button class="menu-toggle" text @click="sidebarOpen = !sidebarOpen">☰</el-button>
        <span class="brand-mark">IoT</span>
        <span>Odoo20IoT</span>
      </div>
      <div class="topbar-actions">
        <span class="connection-state"><i class="connection-dot" />已连接控制台</span>
        <el-button text type="danger" @click="logout">退出登录</el-button>
      </div>
    </header>

    <div class="app-body">
      <aside :class="['sidebar', { open: sidebarOpen }]">
        <div class="sidebar-section-title">控制台</div>
        <nav class="nav-list" aria-label="主导航">
          <button
            v-for="item in menuItems"
            :key="item.name"
            :class="['nav-item', { active: activeMenu === item.name }]"
            type="button"
            @click="navigate(item.name)"
          >
            <span class="nav-icon">{{ item.icon }}</span>
            <span>{{ item.label }}</span>
          </button>
        </nav>
      </aside>

      <main class="content">
        <RouterView />
      </main>
    </div>
  </div>
</template>
