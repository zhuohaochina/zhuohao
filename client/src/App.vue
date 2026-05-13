<template>
  <div class="app-container">
    <!-- 全局进度条 -->
    <ProgressBar v-if="!isFieldsSyncRoute" />

    <!-- 主内容区域 -->
    <el-main class="app-main">
      <router-view />
    </el-main>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import ProgressBar from './components/ProgressBar.vue'

const route = useRoute()
const isFieldsSyncRoute = computed(() => route.path === '/fields-sync')
</script>

<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
  background-color: #ffffff;
}

.app-container {
  min-height: 100vh;
  background: #ffffff;
}

.app-main {
  padding: 0;
}
</style>
