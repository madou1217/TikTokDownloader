<template>
  <div class="app">
    <header class="hero">
      <div class="hero-text">
        <p class="kicker">抖音数据中心</p>
        <h1>抖音管理台</h1>
        <p class="subtitle">账号管理与抓包凭证统一配置中心</p>
      </div>
    </header>

    <nav class="tabs">
      <RouterLink
        class="tab"
        :class="{ active: isActive('/users') }"
        to="/users"
      >
        用户管理
      </RouterLink>
      <RouterLink
        class="tab"
        :class="{ active: isActive('/settings') }"
        to="/settings"
      >
        设置
      </RouterLink>
    </nav>

    <main class="content">
      <RouterView />
    </main>

    <ToastStack :toasts="toasts" />
  </div>
</template>

<script setup>
import { RouterLink, RouterView, useRoute } from "vue-router";
import { provide, ref } from "vue";
import ToastStack from "./components/ToastStack.vue";

const route = useRoute();
const toasts = ref([]);
let toastId = 0;

const setAlert = (type, message) => {
  if (!message) {
    return;
  }
  toastId += 1;
  const item = {
    id: toastId,
    type,
    message,
  };
  toasts.value = [item, ...toasts.value].slice(0, 4);
  window.setTimeout(() => {
    toasts.value = toasts.value.filter((toast) => toast.id !== item.id);
  }, 3500);
};

const formatErrorMessage = (detail) => {
  if (!detail) {
    return "请求失败";
  }
  if (Array.isArray(detail) && detail.length > 0) {
    return detail[0]?.msg || "请求参数错误";
  }
  const text = String(detail);
  if (text.includes("Traceback") || text.includes("File \"")) {
    return "服务异常，请查看后端日志";
  }
  const firstLine = text.split("\n").find((line) => line.trim()) || text;
  if (firstLine.length > 120) {
    return `${firstLine.slice(0, 120)}...`;
  }
  return firstLine.trim();
};

const apiRequest = async (path, options = {}) => {
  const headers = options.body ? { "Content-Type": "application/json" } : {};
  const response = await fetch(path, {
    method: options.method || "GET",
    headers,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  const text = await response.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch (error) {
      data = { message: text };
    }
  }
  if (!response.ok) {
    const detail = data?.detail || data?.message || response.statusText;
    const error = new Error(formatErrorMessage(detail));
    error.status = response.status;
    throw error;
  }
  return data;
};

const isActive = (path) => route.path.startsWith(path);

provide("setAlert", setAlert);
provide("apiRequest", apiRequest);
</script>
