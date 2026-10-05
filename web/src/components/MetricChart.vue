<script setup lang="ts">
import { computed } from 'vue'
import { use } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { CanvasRenderer } from 'echarts/renderers'
import { GridComponent, TooltipComponent } from 'echarts/components'
import type { EChartsOption } from 'echarts'
import VChart from 'vue-echarts'

import type { SeriesResponse } from '@/types/api'

use([LineChart, CanvasRenderer, GridComponent, TooltipComponent])

const props = defineProps<{
  response?: SeriesResponse
  name?: string
  height?: string
}>()

const option = computed<EChartsOption>(() => {
  const response = props.response
  const points = response?.points?.length
    ? response.points.map((point) => [point.ts, point.value])
    : (response?.buckets || []).map((bucket) => [bucket.bucket, bucket.avg ?? bucket.max])
  return {
    animation: false,
    grid: { left: 42, right: 18, top: 18, bottom: 34 },
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'time', axisLabel: { color: '#718096' } },
    yAxis: { type: 'value', scale: true, axisLabel: { color: '#718096' }, splitLine: { lineStyle: { color: '#edf1f6' } } },
    series: [{
      name: props.name || '指标', type: 'line', data: points, smooth: true,
      showSymbol: points.length < 80, connectNulls: false,
      lineStyle: { color: '#2563eb', width: 2 }, itemStyle: { color: '#2563eb' },
    }],
  }
})
</script>

<template>
  <div class="metric-chart" :style="{ height: height || '260px' }">
    <v-chart :option="option" autoresize />
  </div>
</template>
