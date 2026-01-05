<template>
  <section class="card">
    <div class="card-header">
      <div>
        <h2>计划任务</h2>
        <p class="muted">支持时间点或区间配置（24 小时制）。</p>
      </div>
      <button class="ghost" :disabled="state.loading.fetch" @click="loadSetting">
        刷新
      </button>
    </div>

    <div class="toggle-row">
      <label class="toggle">
        <input v-model="state.form.enabled" type="checkbox" />
        <span>启用自动拉取</span>
      </label>
    </div>

    <div class="form-grid">
      <label class="field">
        <span>执行时间</span>
        <input
          v-model="state.form.times"
          placeholder="例如 09:00,12:30 或 08:00-23:00/2"
        />
        <small class="muted">
          多个时间可用逗号或空格分隔，逗号支持中英文，区间支持“08:00-23:00/2”。
        </small>
      </label>
      <label class="field">
        <span>当前表达式</span>
        <textarea
          v-model="state.form.expression"
          rows="2"
          readonly
          placeholder="-"
        ></textarea>
      </label>
    </div>

    <div class="form-actions">
      <button class="primary" :disabled="state.loading.save" @click="saveSetting">
        保存设置
      </button>
    </div>
  </section>
</template>

<script setup>
import { inject, onMounted, reactive } from "vue";

const apiRequest = inject("apiRequest");
const setAlert = inject("setAlert");

const state = reactive({
  form: {
    enabled: true,
    times: "",
    expression: "",
  },
  loading: {
    fetch: false,
    save: false,
  },
});

const loadSetting = async () => {
  state.loading.fetch = true;
  try {
    const data = await apiRequest("/admin/douyin/schedule");
    state.form.enabled = Boolean(data.enabled);
    state.form.times = data.times || "";
    state.form.expression = data.expression || "";
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.fetch = false;
  }
};

const saveSetting = async () => {
  state.loading.save = true;
  try {
    const payload = {
      enabled: Boolean(state.form.enabled),
      times: state.form.times || "",
      expression: state.form.expression || "",
    };
    const data = await apiRequest("/admin/douyin/schedule", {
      method: "POST",
      body: payload,
    });
    state.form.enabled = Boolean(data.enabled);
    state.form.times = data.times || "";
    state.form.expression = data.expression || "";
    setAlert("success", "计划任务已更新");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.save = false;
  }
};

onMounted(async () => {
  await loadSetting();
});
</script>
