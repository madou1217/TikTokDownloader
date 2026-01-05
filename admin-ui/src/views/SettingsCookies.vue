<template>
  <section class="card">
    <div class="card-header">
      <div>
        <h2>Cookie设置</h2>
        <p class="muted">管理用于抓包的抖音登录凭证。</p>
      </div>
      <button class="ghost" :disabled="state.loading.list" @click="loadCookies">
        刷新
      </button>
    </div>

    <div class="form-grid">
      <label class="field">
        <span>账号标识</span>
        <input v-model="state.form.account" placeholder="用于区分凭证的名称" />
      </label>
      <label class="field">
        <span>登录凭证内容</span>
        <textarea
          v-model="state.form.cookie"
          rows="3"
          placeholder="粘贴登录凭证字符串"
        ></textarea>
      </label>
      <div class="form-actions">
        <button
          class="primary"
          :disabled="state.loading.create"
          @click="createCookie"
        >
          保存凭证
        </button>
        <button
          class="ghost"
          :disabled="state.loading.clipboard"
          @click="createCookieFromClipboard"
        >
          从剪贴板读取凭证
        </button>
      </div>
    </div>

    <div class="form-row">
      <label class="field grow">
        <span>浏览器来源</span>
        <select v-model="state.form.browser">
          <option
            v-for="option in browserOptions"
            :key="option.value"
            :value="option.value"
          >
            {{ option.label }}
          </option>
        </select>
      </label>
      <button
        class="primary"
        :disabled="state.loading.browser"
        @click="createCookieFromBrowser"
      >
        从浏览器读取凭证
      </button>
    </div>

    <div class="toggle-row">
      <label class="toggle">
        <input v-model="state.showExpired" type="checkbox" />
        <span>显示已过期凭证</span>
      </label>
    </div>

    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>账号</th>
            <th>凭证</th>
            <th>状态</th>
            <th>失败次数</th>
            <th>最近使用</th>
            <th>最近失败</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in filteredCookies" :key="item.id">
            <td>{{ item.account || "-" }}</td>
            <td class="mono">{{ item.cookie_masked }}</td>
            <td>
              <span :class="['pill', item.status]">
                {{ formatStatus(item.status) }}
              </span>
            </td>
            <td>{{ item.fail_count }}</td>
            <td class="mono">{{ item.last_used_at || "-" }}</td>
            <td class="mono">{{ item.last_failed_at || "-" }}</td>
            <td class="actions">
              <button class="danger ghost" @click="deleteCookie(item.id)">
                删除
              </button>
            </td>
          </tr>
          <tr v-if="!filteredCookies.length && !state.loading.list">
            <td colspan="7" class="empty">暂无凭证数据</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>

<script setup>
import { computed, inject, onMounted, reactive } from "vue";

const apiRequest = inject("apiRequest");
const setAlert = inject("setAlert");

const state = reactive({
  form: {
    account: "",
    cookie: "",
    browser: "Chrome",
  },
  list: [],
  showExpired: true,
  loading: {
    list: false,
    create: false,
    clipboard: false,
    browser: false,
  },
});

const browserOptions = [
  { label: "谷歌浏览器", value: "Chrome" },
  { label: "微软浏览器", value: "Edge" },
  { label: "火狐浏览器", value: "Firefox" },
  { label: "苹果浏览器", value: "Safari" },
  { label: "铬系浏览器", value: "Chromium" },
  { label: "弧形浏览器", value: "Arc" },
  { label: "欧朋浏览器", value: "Opera" },
  { label: "欧朋游戏版", value: "OperaGX" },
  { label: "勇敢浏览器", value: "Brave" },
  { label: "维瓦尔第浏览器", value: "Vivaldi" },
  { label: "自由狼浏览器", value: "LibreWolf" },
];

const formatStatus = (value) => {
  if (value === "expired") {
    return "已过期";
  }
  return "可用";
};

const loadCookies = async () => {
  state.loading.list = true;
  try {
    state.list = await apiRequest("/admin/douyin/cookies");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.list = false;
  }
};

const createCookie = async () => {
  const cookie = state.form.cookie.trim();
  if (!cookie) {
    setAlert("error", "请填写凭证内容");
    return;
  }
  state.loading.create = true;
  try {
    await apiRequest("/admin/douyin/cookies", {
      method: "POST",
      body: {
        account: state.form.account.trim(),
        cookie,
      },
    });
    state.form.cookie = "";
    await loadCookies();
    setAlert("success", "凭证已保存");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.create = false;
  }
};

const createCookieFromClipboard = async () => {
  state.loading.clipboard = true;
  try {
    await apiRequest("/admin/douyin/cookies/clipboard", {
      method: "POST",
      body: { account: state.form.account.trim() },
    });
    await loadCookies();
    setAlert("success", "已从剪贴板读取凭证");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.clipboard = false;
  }
};

const createCookieFromBrowser = async () => {
  const browser = state.form.browser.trim();
  if (!browser) {
    setAlert("error", "请填写浏览器名称");
    return;
  }
  state.loading.browser = true;
  try {
    await apiRequest("/admin/douyin/cookies/browser", {
      method: "POST",
      body: {
        account: state.form.account.trim(),
        browser,
      },
    });
    await loadCookies();
    setAlert("success", "已从浏览器读取凭证");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.browser = false;
  }
};

const deleteCookie = async (cookieId) => {
  if (!window.confirm("确认删除该凭证？")) {
    return;
  }
  try {
    await apiRequest(`/admin/douyin/cookies/${cookieId}`, { method: "DELETE" });
    await loadCookies();
    setAlert("success", "凭证已删除");
  } catch (error) {
    setAlert("error", error.message);
  }
};

const filteredCookies = computed(() => {
  if (state.showExpired) {
    return state.list;
  }
  return state.list.filter((item) => item.status === "active");
});

onMounted(async () => {
  await loadCookies();
});
</script>
