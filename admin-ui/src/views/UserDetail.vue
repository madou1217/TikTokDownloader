<template>
  <section class="card detail-hero" :style="coverStyle">
    <div class="detail-hero-header">
      <RouterLink class="ghost btn-mini back-link" to="/users">
        返回列表
      </RouterLink>
      <div class="detail-actions">
        <button
          class="primary"
          :disabled="state.loading.fetch"
          @click="fetchTodayData"
        >
          <span v-if="state.loading.fetch" class="spinner"></span>
          {{ state.loading.fetch ? "拉取中..." : "拉取今日数据" }}
        </button>
        <button
          class="ghost"
          :disabled="state.loading.live"
          @click="refreshLive"
        >
          <span v-if="state.loading.live" class="spinner dark"></span>
          {{ state.loading.live ? "刷新中..." : "刷新直播" }}
        </button>
        <button
          class="ghost"
          :disabled="state.loading.fullSync || isFullSyncRunning"
          @click="triggerFullSync"
        >
          <span v-if="state.loading.fullSync" class="spinner dark"></span>
          {{ state.loading.fullSync ? "同步中..." : "同步历史作品" }}
        </button>
        <button
          class="ghost"
          type="button"
          @click="toggleAutoDownload"
        >
          {{ state.user.auto_update ? "自动下载: 开启" : "自动下载: 关闭" }}
        </button>
      </div>
    </div>

    <div class="detail-profile">
      <div class="avatar-block">
        <div :class="['avatar-shell', { live: isLive }]">
          <img
            v-if="state.user.avatar"
            :src="mediaUrl(state.user.avatar)"
            class="avatar-img"
            referrerpolicy="no-referrer"
            alt="用户头像"
          />
          <div v-else class="avatar-fallback">头像</div>
        </div>
        <a
          v-if="isLive && liveUrl"
          :href="liveUrl"
          class="live-badge live-badge-link"
          target="_blank"
          rel="noreferrer"
        >
          直播中
        </a>
      </div>

      <div class="profile-main">
        <div class="profile-title">
          <h2>{{ state.user.nickname || "未命名用户" }}</h2>
          <span :class="['pill', state.user.status]">
            {{ formatStatus(state.user.status) }}
          </span>
          <a class="ghost btn-mini" :href="clientUserUrl" target="_blank" rel="noreferrer">
            观看用户作品
          </a>
          <button class="icon-button info-button" @click="toggleInfo" aria-label="信息">
            i
          </button>
        </div>

        <div class="profile-id">
          <span class="id-text mono" :title="userId">{{ userId }}</span>
          <button class="ghost icon-button" @click="copyText(userId)">
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

        <div class="profile-meta">
          <span>用户ID <span class="mono">{{ state.user.uid || "-" }}</span></span>
          <span
            class="sync-progress"
            :class="`status-${fullSyncStatus}`"
            :title="fullSyncHint"
          >
            历史同步: {{ fullSyncText }}
          </span>
        </div>

        <div v-if="state.showInfo" class="info-popover">
          <div class="info-row">
            <span>直播间 ID</span>
            <span class="mono">{{ state.liveInfo.room_id || "-" }}</span>
          </div>
          <div class="info-row">
            <span>Web RID</span>
            <span class="mono">{{ state.liveInfo.web_rid || "-" }}</span>
          </div>
          <div class="info-row">
            <span>最近拉取</span>
            <span class="mono">{{ state.user.last_fetch_at || "-" }}</span>
          </div>
          <div class="info-row">
            <span>下次自动更新</span>
            <span class="mono">{{ nextAutoUpdateText }}</span>
          </div>
          <div class="info-row">
            <span>最近直播</span>
            <span class="mono">{{ state.user.last_live_at || "-" }}</span>
          </div>
        </div>
      </div>

    </div>
  </section>

  <section class="card works-card">
    <div class="card-header">
      <div>
        <h2>作品集</h2>
        <p class="muted">今日新增作品已标记“新”。</p>
      </div>
      <button class="ghost" :disabled="state.works.loading" @click="reloadWorks">
        刷新
      </button>
    </div>
    <div class="works-grid">
      <article v-for="item in state.works.items" :key="item.aweme_id" class="work-card">
        <div class="work-cover">
          <a class="work-link" :href="workUrl(item)" target="_blank" rel="noreferrer">
            <img
              v-if="item.cover"
              :src="mediaUrl(item.cover)"
              referrerpolicy="no-referrer"
              alt="作品封面"
            />
            <div v-else class="work-placeholder">暂无封面</div>
          </a>
          <div class="work-badges" v-if="isToday(item)">
            <span class="badge-new">新</span>
          </div>
          <div class="work-views">
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path
                d="M12 5c5.5 0 9.5 5.1 9.5 7s-4 7-9.5 7S2.5 14 2.5 12 6.5 5 12 5Zm0 2c-4 0-7.1 3.7-7.5 5 .4 1.3 3.5 5 7.5 5s7.1-3.7 7.5-5c-.4-1.3-3.5-5-7.5-5Zm0 2.5a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5Z"
              />
            </svg>
            <span>{{ formatCount(item.play_count) }}</span>
          </div>
          <div
            class="work-upload-state"
            :class="`status-${getUploadState(item)}`"
            :title="getUploadTitle(item)"
          >
            <svg
              v-if="getUploadState(item) === 'uploaded'"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                d="M6 16a5 5 0 0 1 .8-9.9A6 6 0 0 1 18.5 8a4 4 0 0 1-.5 8H9"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
              />
              <path
                d="m9 16 2 2 4-4"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
                stroke-linejoin="round"
              />
            </svg>
            <svg
              v-else-if="getUploadState(item) === 'uploading'"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                d="M6 16a5 5 0 0 1 .8-9.9A6 6 0 0 1 18.5 8a4 4 0 0 1-.5 8H8"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
              />
              <path
                d="M12 9v7m0 0-3-3m3 3 3-3"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
                stroke-linejoin="round"
              />
            </svg>
            <svg
              v-else-if="getUploadState(item) === 'downloading'"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                d="M6 16a5 5 0 0 1 .8-9.9A6 6 0 0 1 18.5 8a4 4 0 0 1-.5 8H8"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
              />
              <path
                d="M12 8v7m0 0-3-3m3 3 3-3"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
                stroke-linejoin="round"
              />
            </svg>
            <svg
              v-else-if="getUploadState(item) === 'failed'"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                d="M6 16a5 5 0 0 1 .8-9.9A6 6 0 0 1 18.5 8a4 4 0 0 1-.5 8H8"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
              />
              <path
                d="M9 9l6 6m0-6-6 6"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
              />
            </svg>
            <svg v-else viewBox="0 0 24 24" aria-hidden="true">
              <path
                d="M6 16a5 5 0 0 1 .8-9.9A6 6 0 0 1 18.5 8a4 4 0 0 1-.5 8H8"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
              />
              <path
                d="M5 5 19 19"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
              />
            </svg>
          </div>
        </div>
        <div class="work-body">
          <a
            class="work-title"
            :href="workUrl(item)"
            target="_blank"
            rel="noreferrer"
            :title="item.desc || item.aweme_id"
          >
            {{ item.desc || item.aweme_id }}
          </a>
          <div class="work-meta-row">
            <p class="work-meta">{{ formatDateTime(item) }}</p>
            <span class="work-stage-tag" :class="`status-${getUploadState(item)}`">
              {{ getUploadText(item) }}
            </span>
          </div>
          <a
            v-if="item.upload_destination"
            class="work-upload-path"
            :href="item.upload_destination"
            target="_blank"
            rel="noreferrer"
            :title="item.upload_destination"
          >
            上传路径
          </a>
        </div>
      </article>
      <div v-if="!state.works.items.length && !state.works.loading" class="empty">
        暂无作品记录
      </div>
    </div>
    <div class="load-more">
      <button
        class="ghost"
        :disabled="!hasMore || state.works.loadingMore"
        @click="loadMoreWorks"
      >
        <span v-if="state.works.loadingMore" class="spinner dark"></span>
        {{ state.works.loadingMore ? "加载中..." : hasMore ? "加载更多" : "没有更多" }}
      </button>
    </div>
  </section>
</template>

<script setup>
import { computed, inject, onMounted, reactive, watch } from "vue";
import { RouterLink, useRoute } from "vue-router";

const apiRequest = inject("apiRequest");
const setAlert = inject("setAlert");
const route = useRoute();

const state = reactive({
  user: {},
  liveInfo: {},
  fullSync: {},
  settings: {
    uploadEnabled: true,
  },
  works: {
    items: [],
    total: 0,
    page: 1,
    pageSize: 12,
    loading: false,
    loadingMore: false,
  },
  loading: {
    fetch: false,
    live: false,
    fullSync: false,
  },
  showInfo: false,
});

const userId = computed(() => {
  const raw = route.params.secUserId || "";
  try {
    return decodeURIComponent(String(raw));
  } catch (error) {
    return String(raw);
  }
});

const isLive = computed(() => Boolean(state.liveInfo?.live_status));
const liveWebRid = computed(() => state.liveInfo?.web_rid || "");
const liveRoomId = computed(() => state.liveInfo?.room_id || "");
const liveUrl = computed(() => {
  if (!liveWebRid.value) {
    return "";
  }
  const params = new URLSearchParams({
    action_type: "click",
    enter_from_merge: "web_others_homepage",
    enter_method: "web_homepage_head",
    enter_method_temai: "web_video_head",
    group_id: "undefined",
    is_livehead_preview_mini_window_show: "",
    is_replaced_live: "0",
    live_position: "undefined",
    mini_window_show_type: "",
    request_id: "undefined",
    room_id: liveRoomId.value || "undefined",
    search_tab: "undefined",
    web_card_rank: "",
    web_live_page: "",
  });
  return `https://live.douyin.com/${liveWebRid.value}?${params.toString()}`;
});

const hasMore = computed(
  () => state.works.items.length < (state.works.total || 0)
);

const coverStyle = computed(() => {
  if (!state.user.cover) {
    return {};
  }
  const url = mediaUrl(state.user.cover);
  if (!url) {
    return {};
  }
  return {
    backgroundImage: `linear-gradient(120deg, rgba(10, 10, 14, 0.86), rgba(10, 10, 14, 0.45)), url('${url}')`,
  };
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

const nextAutoUpdateText = computed(
  () => state.user.next_auto_update_at || "-"
);
const fullSyncStatus = computed(() => state.fullSync?.status || "idle");
const isFullSyncRunning = computed(() =>
  ["queued", "running"].includes(fullSyncStatus.value)
);
const fullSyncText = computed(() => {
  const status = fullSyncStatus.value;
  if (status === "running") {
    return `进行中（页 ${state.fullSync.pages || 0}，作品 ${state.fullSync.works || 0}，入库 ${state.fullSync.stored || 0}）`;
  }
  if (status === "queued") {
    return "排队中";
  }
  if (status === "done") {
    return `已完成（页 ${state.fullSync.pages || 0}，作品 ${state.fullSync.works || 0}，入库 ${state.fullSync.stored || 0}）`;
  }
  if (status === "failed") {
    return "失败";
  }
  return "未同步";
});
const fullSyncHint = computed(() => {
  if (!state.fullSync) {
    return "";
  }
  const pieces = [];
  if (state.fullSync.started_at) {
    pieces.push(`开始: ${state.fullSync.started_at}`);
  }
  if (state.fullSync.updated_at) {
    pieces.push(`更新: ${state.fullSync.updated_at}`);
  }
  if (state.fullSync.finished_at) {
    pieces.push(`结束: ${state.fullSync.finished_at}`);
  }
  if (state.fullSync.error) {
    pieces.push(`错误: ${state.fullSync.error}`);
  }
  return pieces.join(" | ");
});
const isUploadEnabled = computed(() => state.settings.uploadEnabled);
const clientUserUrl = computed(() => {
  if (!userId.value) {
    return "/client-ui/";
  }
  return `/client-ui/user/${encodeURIComponent(userId.value)}`;
});

const toggleInfo = () => {
  state.showInfo = !state.showInfo;
};

const copyText = async (text) => {
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
    setAlert("success", "已复制");
  } catch (error) {
    setAlert("error", "复制失败");
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

const workUrl = (item) => {
  if (item?.type === "live") {
    return item?.upload_destination || item?.upload_origin_destination || "#";
  }
  const awemeId = item?.aweme_id || "";
  if (!awemeId) {
    return "";
  }
  if (item?.type === "note") {
    return `https://www.douyin.com/note/${awemeId}`;
  }
  return `https://www.douyin.com/video/${awemeId}`;
};

const formatCount = (value) => {
  const count = Number(value) || 0;
  if (count >= 100000000) {
    return `${(count / 100000000).toFixed(1)}亿`;
  }
  if (count >= 10000) {
    return `${(count / 10000).toFixed(1)}万`;
  }
  return String(count || 0);
};

const formatDateTime = (item) => {
  if (!item) {
    return "-";
  }
  const value = item.create_time || "";
  if (value) {
    return value.slice(0, 16);
  }
  const ts = Number(item.create_ts) || 0;
  if (!ts) {
    return "-";
  }
  const date = new Date(ts * 1000);
  const pad = (num) => String(num).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(
    date.getHours()
  )}:${pad(date.getMinutes())}`;
};

const getUploadState = (item) => {
  const status = (item?.upload_status || "pending").toLowerCase();
  if (!isUploadEnabled.value) {
    if (["uploaded", "uploading", "downloaded"].includes(status)) {
      return "downloaded";
    }
    return "pending";
  }
  if (["uploaded", "uploading", "downloading", "downloaded", "failed"].includes(status)) {
    return status;
  }
  return "pending";
};

const getUploadTitle = (item) => {
  const status = getUploadState(item);
  if (!isUploadEnabled.value) {
    return status === "downloaded" ? "已下载" : "未下载";
  }
  if (status === "uploaded") {
    return "上传完成";
  }
  if (status === "uploading") {
    return "下载完成，上传中";
  }
  if (status === "downloaded") {
    return "已下载，未上传";
  }
  if (status === "downloading") {
    return "下载中";
  }
  if (status === "failed") {
    return item?.upload_message || "处理失败";
  }
  return "未下载";
};

const getUploadText = (item) => {
  const status = getUploadState(item);
  if (!isUploadEnabled.value) {
    return status === "downloaded" ? "已下载" : "未下载";
  }
  if (status === "uploaded") {
    return "已上传";
  }
  if (status === "uploading") {
    return "上传中";
  }
  if (status === "downloaded") {
    return "未上传";
  }
  if (status === "downloading") {
    return "下载中";
  }
  if (status === "failed") {
    return "失败";
  }
  return "未下载";
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const hasRunningOrPendingWorks = () =>
  state.works.items.some((item) => {
    const status = getUploadState(item);
    return status === "pending" || status === "downloading" || status === "uploading";
  });

const pollWorkProgress = async (rounds = 6, intervalMs = 5000) => {
  for (let i = 0; i < rounds; i += 1) {
    if (!hasRunningOrPendingWorks()) {
      return;
    }
    await sleep(intervalMs);
    await reloadWorks();
  }
};

const todayString = () => {
  const date = new Date();
  const pad = (num) => String(num).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
};

const isToday = (item) => {
  if (!item) {
    return false;
  }
  return item.create_date === todayString();
};

const loadUser = async () => {
  try {
    const data = await apiRequest(
      `/admin/douyin/users/${encodeURIComponent(userId.value)}`
    );
    state.user = data || {};
  } catch (error) {
    setAlert("error", error.message);
  }
};

const loadSettings = async () => {
  try {
    const data = await apiRequest("/settings");
    state.settings.uploadEnabled = Boolean(data?.upload?.enabled ?? true);
  } catch (error) {
    state.settings.uploadEnabled = true;
  }
};

const loadFullSyncProgress = async () => {
  try {
    const data = await apiRequest(
      `/admin/douyin/users/${encodeURIComponent(userId.value)}/full-sync`
    );
    state.fullSync = data.data || {};
  } catch (error) {
    setAlert("error", error.message);
  }
};

const loadWorks = async (page, append = false) => {
  if (!append) {
    state.works.loading = true;
  } else {
    state.works.loadingMore = true;
  }
  try {
    const query = new URLSearchParams({
      page: String(page),
      page_size: String(state.works.pageSize),
    });
    const data = await apiRequest(
      `/admin/douyin/users/${encodeURIComponent(
        userId.value
      )}/works/stored?${query.toString()}`
    );
    const items = data.items || [];
    state.works.total = data.total || 0;
    state.works.items = append ? [...state.works.items, ...items] : items;
    state.works.page = page;
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.works.loading = false;
    state.works.loadingMore = false;
  }
};

const reloadWorks = async () => {
  await loadWorks(1, false);
};

const loadMoreWorks = async () => {
  if (!hasMore.value || state.works.loadingMore) {
    return;
  }
  const nextPage = state.works.page + 1;
  await loadWorks(nextPage, true);
};

const fetchLatestWorks = async () => {
  await apiRequest(`/admin/douyin/users/${encodeURIComponent(userId.value)}/latest`, {
    method: "POST",
  });
};

const loadLiveCache = async () => {
  try {
    const data = await apiRequest(
      `/admin/douyin/users/${encodeURIComponent(userId.value)}/live`
    );
    state.liveInfo = data.data || {};
  } catch (error) {
    if (error.status === 405) {
      await refreshLive();
      return;
    }
    setAlert("error", error.message);
  }
};

const refreshLive = async () => {
  state.loading.live = true;
  try {
    const data = await apiRequest(
      `/admin/douyin/users/${encodeURIComponent(userId.value)}/live`,
      {
        method: "POST",
      }
    );
    state.liveInfo = data.data || {};
    await loadUser();
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.live = false;
  }
};

const fetchTodayData = async () => {
  state.loading.fetch = true;
  try {
    await fetchLatestWorks();
    await refreshLive();
    await reloadWorks();
    if (state.user.auto_update) {
      await pollWorkProgress();
    }
    await loadUser();
    setAlert("success", "拉取完成");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.fetch = false;
  }
};

const pollFullSyncProgress = async (rounds = 30, intervalMs = 4000) => {
  for (let i = 0; i < rounds; i += 1) {
    await loadFullSyncProgress();
    if (!isFullSyncRunning.value) {
      return;
    }
    await sleep(intervalMs);
  }
};

const triggerFullSync = async () => {
  if (!userId.value) {
    return;
  }
  state.loading.fullSync = true;
  try {
    await apiRequest(
      `/admin/douyin/users/${encodeURIComponent(userId.value)}/full-sync`,
      { method: "POST" }
    );
    await loadFullSyncProgress();
    await pollFullSyncProgress();
    await reloadWorks();
    setAlert("success", "已触发同步");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.fullSync = false;
  }
};

const toggleAutoDownload = async () => {
  if (!userId.value) {
    return;
  }
  try {
    const payload = {
      auto_update: !Boolean(state.user.auto_update),
      update_window_start: state.user.update_window_start || "",
      update_window_end: state.user.update_window_end || "",
    };
    const data = await apiRequest(
      `/admin/douyin/users/${encodeURIComponent(userId.value)}/settings`,
      {
        method: "PUT",
        body: payload,
      }
    );
    state.user = data || state.user;
    await loadUser();
    await reloadWorks();
    if (state.user.auto_update) {
      await pollWorkProgress();
    }
    setAlert(
      "success",
      state.user.auto_update
        ? "已开启自动下载，已触发立即扫描"
        : "已关闭自动下载"
    );
  } catch (error) {
    setAlert("error", error.message);
  }
};

onMounted(async () => {
  await loadSettings();
  await loadUser();
  await reloadWorks();
  await loadLiveCache();
  await loadFullSyncProgress();
  if (isFullSyncRunning.value) {
    await pollFullSyncProgress();
  }
});

watch(
  () => userId.value,
  async () => {
    state.works.items = [];
    state.works.page = 1;
    state.liveInfo = {};
    state.fullSync = {};
    state.showInfo = false;
    await loadSettings();
    await loadUser();
    await reloadWorks();
    await loadLiveCache();
    await loadFullSyncProgress();
  }
);
</script>
