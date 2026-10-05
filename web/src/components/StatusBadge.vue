<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  online: boolean
  status?: string
}>()

const label = computed(() => (props.online ? '在线' : '离线'))
const type = computed(() => (props.online ? 'success' : 'info'))
</script>

<template>
  <el-tag :type="type" size="small" effect="light">{{ label }}</el-tag>
</template>
