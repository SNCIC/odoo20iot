import { createRouter, createWebHashHistory } from 'vue-router'

import AppShell from '@/components/AppShell.vue'
import AlarmView from '@/views/AlarmView.vue'
import DashboardView from '@/views/DashboardView.vue'
import DeviceDetailView from '@/views/DeviceDetailView.vue'
import DevicesView from '@/views/DevicesView.vue'
import LoginView from '@/views/LoginView.vue'
import ModbusView from '@/views/ModbusView.vue'
import NotifyView from '@/views/NotifyView.vue'
import OverviewView from '@/views/OverviewView.vue'
import OTAView from '@/views/OTAView.vue'
import PlaceholderView from '@/views/PlaceholderView.vue'
import QuotaView from '@/views/QuotaView.vue'
import RulesView from '@/views/RulesView.vue'
import ShadowView from '@/views/ShadowView.vue'

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/login', name: 'login', component: LoginView },
    {
      path: '/',
      component: AppShell,
      meta: { requiresAuth: true },
      children: [
        { path: '', redirect: '/overview' },
        { path: 'overview', name: 'overview', component: OverviewView },
        { path: 'devices', name: 'devices', component: DevicesView },
        { path: 'devices/:id', name: 'device-detail', component: DeviceDetailView },
        { path: 'dashboards', name: 'dashboards', component: DashboardView, meta: { title: '数据看板' } },
        { path: 'alarms', name: 'alarms', component: AlarmView, meta: { title: '告警管理' } },
        { path: 'rules', name: 'rules', component: RulesView, meta: { title: '规则管理' } },
        { path: 'shadow/:deviceKey', name: 'shadow', component: ShadowView, meta: { title: '设备影子' } },
        { path: 'ota', name: 'ota', component: OTAView, meta: { title: 'OTA 升级' } },
        { path: 'modbus', name: 'modbus', component: ModbusView, meta: { title: 'Modbus 采集' } },
        { path: 'notify', name: 'notify', component: NotifyView, meta: { title: '通知端点' } },
        { path: 'quota', name: 'quota', component: QuotaView, meta: { title: '配额策略' } },
      ],
    },
    { path: '/:pathMatch(.*)*', redirect: '/overview' },
  ],
})

router.beforeEach((to) => {
  const authenticated = Boolean(localStorage.getItem('iot_token'))
  if (to.meta.requiresAuth && !authenticated) {
    return { name: 'login', query: { redirect: to.fullPath } }
  }
  if (to.name === 'login' && authenticated) return { name: 'overview' }
  return true
})

export default router
