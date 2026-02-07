<template>
  <section class="card">
    <div class="card-header">
      <div>
        <h2>用户管理</h2>
        <p class="muted">维护抖音账号信息，支持分页查看。</p>
      </div>
      <div class="toolbar">
        <button class="ghost" :disabled="state.loading.list" @click="loadUsers">
          刷新
        </button>
        <button class="primary" @click="openModal">新增用户</button>
      </div>
    </div>

    <div class="table-wrap">
      <table class="user-table">
        <thead>
          <tr>
            <th>用户标识</th>
            <th>用户ID</th>
            <th>昵称</th>
            <th>状态</th>
            <th>直播</th>
            <th>自动下载</th>
            <th>最近拉取</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in state.items" :key="item.sec_user_id">
            <td class="mono">
              <div class="id-cell">
                <span class="id-text" :title="item.sec_user_id">
                  {{ item.sec_user_id }}
                </span>
                <button
                  class="ghost icon-button"
                  type="button"
                  @click="copyUserId(item.sec_user_id)"
                  aria-label="Copy"
                >
                  <svg viewBox="0 0 24 24" aria-hidden="true">
                    <path
                      d="M8 8a3 3 0 0 1 3-3h6a3 3 0 0 1 3 3v6a3 3 0 0 1-3 3h-6a3 3 0 0 1-3-3V8Zm3-1a1 1 0 0 0-1 1v6a1 1 0 0 0 1 1h6a1 1 0 0 0 1-1V8a1 1 0 0 0-1-1h-6Z"
                    />
                    <path
                      d="M5 10a3 3 0 0 1 3-3h1a1 1 0 1 1 0 2H8a1 1 0 0 0-1 1v6a1 1 0 0 0 1 1h6a1 1 0 0 0 1-1v-1a1 1 0 1 1 2 0v1a3 3 0 0 1-3 3H8a3 3 0 0 1-3-3v-6Z"
                    />
                  </svg>
                </button>
              </div>
            </td>
            <td class="mono">{{ item.uid || "-" }}</td>
            <td>
              <div class="name-cell">
                <span class="name-avatar">
                  <img
                    v-if="item.avatar"
                    :src="mediaUrl(item.avatar)"
                    referrerpolicy="no-referrer"
                    alt="头像"
                  />
                  <span v-else class="name-avatar-fallback">无</span>
                </span>
                <span class="name-text">{{ item.nickname || "-" }}</span>
              </div>
            </td>
            <td>
              <span :class="['pill', item.status]">
                {{ formatStatus(item.status) }}
              </span>
            </td>
            <td>{{ item.is_live ? "是" : "否" }}</td>
            <td>
              <button
                class="toggle-pill"
                :class="{ on: item.auto_update }"
                type="button"
                @click="toggleAutoDownload(item)"
              >
                {{ item.auto_update ? "开启" : "关闭" }}
              </button>
            </td>
            <td class="mono">{{ item.last_fetch_at || "-" }}</td>
            <td class="actions">
              <div class="action-group">
                <RouterLink
                  class="ghost btn-mini"
                  :to="`/users/${encodeURIComponent(item.sec_user_id)}`"
                >
                  详情
                </RouterLink>
                <button class="danger ghost btn-mini" @click="deleteUser(item.sec_user_id)">
                  删除
                </button>
              </div>
            </td>
          </tr>
          <tr v-if="!state.items.length && !state.loading.list">
            <td colspan="8" class="empty">暂无用户数据</td>
          </tr>
        </tbody>
      </table>
    </div>

    <PaginationBar
      :page="state.page"
      :page-size="state.pageSize"
      :total="state.total"
      @change="handlePageChange"
    />
  </section>

  <AddUserModal
    v-model:open="state.modalOpen"
    :loading="state.loading.create"
    @submit="createUser"
  />
</template>

<script setup>
import { inject, onMounted, reactive } from "vue";
import { RouterLink } from "vue-router";
import AddUserModal from "../components/AddUserModal.vue";
import PaginationBar from "../components/PaginationBar.vue";

const apiRequest = inject("apiRequest");
const setAlert = inject("setAlert");

const state = reactive({
  items: [],
  total: 0,
  page: 1,
  pageSize: 20,
  modalOpen: false,
  loading: {
    list: false,
    create: false,
  },
});

const formatStatus = (value) => {
  const map = {
    active: "正常",
    no_works: "无作品",
    unknown: "未知",
    private: "私密",
  };
  return map[value] || "未知";
};

const loadUsers = async () => {
  state.loading.list = true;
  try {
    const query = new URLSearchParams({
      page: String(state.page),
      page_size: String(state.pageSize),
    });
    let data = await apiRequest(`/admin/douyin/users/paged?${query.toString()}`);
    const totalPages = Math.max(1, Math.ceil(data.total / state.pageSize));
    if (state.page > totalPages) {
      state.page = totalPages;
      const retryQuery = new URLSearchParams({
        page: String(state.page),
        page_size: String(state.pageSize),
      });
      data = await apiRequest(`/admin/douyin/users/paged?${retryQuery.toString()}`);
    }
    state.items = data.items || [];
    state.total = data.total || 0;
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.list = false;
  }
};

const openModal = () => {
  state.modalOpen = true;
};

const isValidUserInput = (value) => {
  const trimmed = value.trim();
  if (!trimmed) {
    return false;
  }
  if (/^MS4wL[0-9A-Za-z_-]+$/.test(trimmed)) {
    return true;
  }
  if (/^https?:\/\/live\.douyin\.com\/\d+/.test(trimmed)) {
    return true;
  }
  if (/^live\.douyin\.com\/\d+/.test(trimmed)) {
    return true;
  }
  if (/^https?:\/\/(www\.)?douyin\.com\/user\/[A-Za-z0-9_-]+/.test(trimmed)) {
    return true;
  }
  if (/^(www\.)?douyin\.com\/user\/[A-Za-z0-9_-]+/.test(trimmed)) {
    return true;
  }
  if (/^https?:\/\/(www\.)?douyin\.com\/(video|note|slides)\/\d{19}/.test(trimmed)) {
    return true;
  }
  if (/^(www\.)?douyin\.com\/(video|note|slides)\/\d{19}/.test(trimmed)) {
    return true;
  }
  if (/^https?:\/\/(www\.)?douyin\.com\/\S*modal_id=\d{19}/.test(trimmed)) {
    return true;
  }
  if (/^(www\.)?douyin\.com\/\S*modal_id=\d{19}/.test(trimmed)) {
    return true;
  }
  if (/^https?:\/\/(www\.)?iesdouyin\.com\/share\/(video|note|slides)\/\d{19}/.test(trimmed)) {
    return true;
  }
  if (/^(www\.)?iesdouyin\.com\/share\/(video|note|slides)\/\d{19}/.test(trimmed)) {
    return true;
  }
  if (/^https?:\/\/(www\.)?iesdouyin\.com\/share\/user\//.test(trimmed)) {
    return true;
  }
  if (/^(www\.)?iesdouyin\.com\/share\/user\//.test(trimmed)) {
    return true;
  }
  if (/^https?:\/\/webcast\.amemv\.com\/douyin\/webcast\/reflow\//.test(trimmed)) {
    return true;
  }
  if (/^webcast\.amemv\.com\/douyin\/webcast\/reflow\//.test(trimmed)) {
    return true;
  }
  if (/^https?:\/\/www\.douyin\.com\/follow\?webRid=\d+/.test(trimmed)) {
    return true;
  }
  if (/^www\.douyin\.com\/follow\?webRid=\d+/.test(trimmed)) {
    return true;
  }
  return false;
};

const copyUserId = async (value) => {
  const text = value || "";
  if (!text) {
    return;
  }
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
    } else {
      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.style.position = "fixed";
      textarea.style.opacity = "0";
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand("copy");
      document.body.removeChild(textarea);
    }
    setAlert("success", "用户标识已复制");
  } catch (error) {
    setAlert("error", "复制失败，请手动复制");
  }
};

const mediaUrl = (url) => {
  if (!url) {
    return "";
  }
  if (url.startsWith("/")) {
    return url;
  }
  return `/admin/douyin/media?url=${encodeURIComponent(url)}`;
};

const createUser = async (secUserId) => {
  const value = secUserId.trim();
  if (!value) {
    setAlert("error", "请输入用户标识");
    return;
  }
  if (!isValidUserInput(value)) {
    setAlert("error", "请输入用户标识或抖音链接");
    return;
  }
  state.loading.create = true;
  try {
    await apiRequest("/admin/douyin/users", {
      method: "POST",
      body: { sec_user_id: value },
    });
    state.modalOpen = false;
    state.page = 1;
    await loadUsers();
    setAlert("success", "用户新增成功");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.create = false;
  }
};

const deleteUser = async (secUserId) => {
  if (!window.confirm("确认删除该用户？")) {
    return;
  }
  try {
    await apiRequest(`/admin/douyin/users/${encodeURIComponent(secUserId)}`, {
      method: "DELETE",
    });
    await loadUsers();
    setAlert("success", "用户已删除");
  } catch (error) {
    setAlert("error", error.message);
  }
};

const toggleAutoDownload = async (item) => {
  if (!item?.sec_user_id) {
    return;
  }
  try {
    const payload = {
      auto_update: !Boolean(item.auto_update),
      update_window_start: item.update_window_start || "",
      update_window_end: item.update_window_end || "",
    };
    const data = await apiRequest(
      `/admin/douyin/users/${encodeURIComponent(item.sec_user_id)}/settings`,
      {
        method: "PUT",
        body: payload,
      }
    );
    item.auto_update = Boolean(data?.auto_update);
    setAlert(
      "success",
      item.auto_update
        ? "已开启自动下载，已触发立即扫描"
        : "已关闭自动下载"
    );
  } catch (error) {
    setAlert("error", error.message);
  }
};

const handlePageChange = async (page) => {
  state.page = page;
  await loadUsers();
};

onMounted(async () => {
  await loadUsers();
});
</script>
