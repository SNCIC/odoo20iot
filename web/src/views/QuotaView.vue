<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'

import { listPolicies, savePolicy } from '@/api/modules/quota'
import type { QuotaPolicy } from '@/types/api'

const metrics = [
  { key: 'msg_count', label: '消息数', unit: '条' },
  { key: 'conn_peak', label: '连接峰值', unit: '个' },
  { key: 'device_count', label: '设备数', unit: '台' },
  { key: 'storage_bytes', label: '存储量', unit: '字节' },
  { key: 'api_calls', label: 'API 调用', unit: '次' },
]
const policies = ref<QuotaPolicy[]>(metrics.map((item) => defaultPolicy(item.key)))
const loading = ref(false)
const saving = ref<string>()

function defaultPolicy(metric: string): QuotaPolicy {
  return { metric, soft_limit: 80, warning_limit: 90, hard_limit: 100, window: 'day', enforcement: 'reject' }
}

function policyFor(metric: string) {
  return policies.value.find((item) => item.metric === metric) as QuotaPolicy
}

async function load() {
  loading.value = true
  try {
    const response = await listPolicies()
    policies.value = metrics.map((item) => response.policies.find((policy) => policy.metric === item.key) || defaultPolicy(item.key))
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '读取配额策略失败')
    policies.value = metrics.map((item) => defaultPolicy(item.key))
  } finally {
    loading.value = false
  }
}

async function save(metric: string) {
  const policy = policyFor(metric)
  if (!(policy.soft_limit <= policy.warning_limit && policy.warning_limit <= policy.hard_limit)) {
    ElMessage.warning('阈值必须满足 80% ≤ 90% ≤ 100% 的递增关系')
    return
  }
  saving.value = metric
  try {
    const response = await savePolicy(policy)
    policies.value = [...policies.value.filter((item) => item.metric !== metric), response.policy]
    ElMessage.success(`${metric} 配额策略已保存`)
  } catch (cause) {
    ElMessage.error(cause instanceof Error ? cause.message : '保存配额策略失败')
  } finally {
    saving.value = undefined
  }
}

onMounted(() => void load())
</script>

<template>
  <div class="page">
    <div class="page-head"><div><h1 class="page-title">配额策略</h1><p class="page-description">配置 80% / 90% / 100% 三档预警和超配额执行策略。</p></div><el-button :loading="loading" @click="load">刷新</el-button></div>
    <el-alert title="Phase 1 以阈值告警为主；throttle / reject 执行策略由配额二期业务路径接管。" type="info" show-icon :closable="false" />
    <section class="panel quota-list">
      <div v-for="item in metrics" :key="item.key" class="quota-row">
        <div class="quota-name"><strong>{{ item.label }}</strong><span>{{ item.key }} · {{ item.unit }}</span></div>
        <div class="quota-fields">
          <label>预警 <el-input-number v-model="policyFor(item.key).soft_limit" :min="0" controls-position="right" /></label>
          <label>告警 <el-input-number v-model="policyFor(item.key).warning_limit" :min="0" controls-position="right" /></label>
          <label>硬限 <el-input-number v-model="policyFor(item.key).hard_limit" :min="0" controls-position="right" /></label>
          <el-select v-model="policyFor(item.key).window" style="width: 110px"><el-option label="按日" value="day" /><el-option label="按月" value="month" /></el-select>
          <el-select v-model="policyFor(item.key).enforcement" style="width: 120px"><el-option label="拒绝 reject" value="reject" /><el-option label="限流 throttle" value="throttle" /></el-select>
          <el-button type="primary" :loading="saving === item.key" @click="save(item.key)">保存</el-button>
        </div>
      </div>
    </section>
  </div>
</template>
