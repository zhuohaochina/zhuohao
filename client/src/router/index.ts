/**
 * 路由配置
 */
import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      name: 'Dashboard',
      component: () => import('../views/Dashboard.vue'),
      meta: { title: '首页' }
    },
    {
      path: '/data/:metadataId',
      name: 'DataView',
      component: () => import('../views/DataView.vue'),
      meta: { title: '数据详情' }
    },
    {
      path: '/update-history/:metadataId',
      name: 'UpdateHistory',
      component: () => import('../views/UpdateHistory.vue'),
      meta: { title: '更新历史' }
    },
    {
      path: '/fields-sync',
      name: 'FieldsSync',
      component: () => import('../views/FieldsSync.vue'),
      meta: { title: '更新接口&字段' }
    },
    {
      path: '/data-combos',
      name: 'DataComboList',
      component: () => import('../views/DataComboList.vue'),
      meta: { title: '关联视图' }
    },
    {
      path: '/data-combos/:id',
      name: 'DataComboView',
      component: () => import('../views/DataComboView.vue'),
      meta: { title: '关联视图详情' }
    },
    {
      path: '/data-combos/:id/row/:code',
      name: 'DataComboRowView',
      component: () => import('../views/DataComboRowView.vue'),
      meta: { title: '关联视图子表明细' }
    }
  ]
})

router.beforeEach((to, _from, next) => {
  document.title = `${to.meta.title || '首页'} - 股票数据采集系统`
  next()
})

export default router
