<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'

const route = useRoute()
const title = computed(() => String(route.meta.title || '功能页面'))
</script>

<template>
  <div class="page">
    <div class="page-head">
      <div>
        <h1 class="page-title">{{ title }}</h1>
        <p class="page-description">页面骨架已接入，正在等待对应业务模块完成。</p>
      </div>
    </div>
    <section class="panel placeholder-card">
      <div>
        <strong>{{ title }}</strong>
        <span>该模块将在 M1.2 / M1.3 阶段逐步开放。</span>
      </div>
    </section>
  </div>
</template>
