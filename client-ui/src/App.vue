<template>
  <div class="app">
    <header class="top-bar">
      <div class="brand">
        <span class="brand-kicker">Douyin Client</span>
        <h1>今日播放台</h1>
      </div>
    </header>

    <section class="layout" :class="{ collapsed: state.sidebarCollapsed }">
      <button
        class="sidebar-toggle"
        type="button"
        :class="{ collapsed: state.sidebarCollapsed }"
        :aria-label="state.sidebarCollapsed ? '展开菜单' : '折叠菜单'"
        @click="toggleSidebar"
      >
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M15 6l-6 6 6 6" fill="none" stroke="currentColor" stroke-width="2" />
        </svg>
      </button>
      <aside class="feed-panel" :class="{ collapsed: state.sidebarCollapsed }">
        <div class="panel-header">
          <div>
            <h2>今日列表</h2>
            <p class="muted">视频与直播混合排序。</p>
          </div>
          <div class="panel-tools">
            <span class="count">{{ state.total }} 条</span>
          </div>
        </div>
        <div class="feed-list" @scroll.passive="onFeedScroll">
          <button
            v-for="(item, index) in state.items"
            :key="itemKey(item, index)"
            type="button"
            class="feed-item"
            :class="{ active: index === state.activeIndex }"
            @click="selectItem(index, true)"
          >
            <div class="thumb">
              <img
                v-if="item.cover"
                :src="mediaUrl(item.cover)"
                referrerpolicy="no-referrer"
                alt="封面"
              />
              <div v-else class="thumb-fallback">暂无封面</div>
              <span :class="['type-tag', item.type]">
                {{ item.type === "live" ? "直播" : "视频" }}
              </span>
            </div>
            <div class="feed-info">
              <div class="feed-title-row">
                <div class="feed-title" :title="item.title || item.aweme_id">
                  {{ item.title || item.aweme_id || "未命名内容" }}
                </div>
                <div
                  v-if="index === state.activeIndex"
                  class="playing-indicator"
                  aria-label="正在播放"
                >
                  <span class="equalizer" aria-hidden="true">
                    <span></span>
                    <span></span>
                    <span></span>
                  </span>
                </div>
              </div>
              <div class="feed-meta">
                <img
                  v-if="item.avatar"
                  class="avatar"
                  :src="mediaUrl(item.avatar)"
                  referrerpolicy="no-referrer"
                  alt="头像"
                />
                <span class="name">{{ item.nickname || item.uid || "未知账号" }}</span>
                <span class="time">{{ formatFeedTime(item.sort_time || item.last_live_at) }}</span>
              </div>
            </div>
          </button>
          <div v-if="!state.items.length && !state.loading" class="empty">
            暂无播放内容
          </div>
          <div v-if="state.loadingMore" class="feed-loading">
            <span class="spinner"></span>
            加载中...
          </div>
        </div>
      </aside>

      <main class="player-panel">
        <div class="panel-header player-header">
          <div class="player-heading">
            <div class="player-title">
              <span class="type-pill">{{ activeTypeLabel }}</span>
              <h2>{{ state.player.title || "请选择内容" }}</h2>
            </div>
            <div class="player-author">
              <img
                v-if="state.player.avatar"
                class="author-avatar"
                :src="mediaUrl(state.player.avatar)"
                referrerpolicy="no-referrer"
                alt="头像"
              />
              <span class="author-name">
                {{ state.player.nickname ? `@${state.player.nickname}` : "@未知作者" }}
              </span>
            </div>
          </div>
          <div class="player-actions">
            <button class="ghost" :disabled="!hasPrev" @click="playPrev">
              上一条
            </button>
            <button class="ghost" :disabled="!hasNext" @click="playNext">
              下一条
            </button>
          </div>
        </div>

        <div
          ref="stageRef"
          class="player-stage"
          :class="[state.player.type, state.player.orientation]"
          :style="stageStyle"
          @touchstart="onTouchStart"
          @touchend="onTouchEnd"
          @wheel.passive="onStageWheel"
        >
          <video
            ref="videoRef"
            class="player-video"
            controls
            :autoplay="!state.player.suppressAutoplay"
            :muted="!state.audioUnlocked"
            playsinline
            preload="metadata"
            :poster="state.player.cover ? mediaUrl(state.player.cover) : ''"
            :style="playerVideoStyle"
          ></video>
          <div class="stage-author">
            <img
              v-if="state.player.avatar"
              class="stage-avatar"
              :src="mediaUrl(state.player.avatar)"
              referrerpolicy="no-referrer"
              alt="头像"
            />
            <span class="stage-name">
              {{ state.player.nickname ? `@${state.player.nickname}` : "@未知作者" }}
            </span>
          </div>
          <a
            v-if="originUrl"
            class="stage-link"
            :href="originUrl"
            target="_blank"
            rel="noreferrer"
            aria-label="打开原链接"
          >
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path
                d="M14 5h5v5m0-5-7 7m-6 2a4 4 0 0 1 4-4h4"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
              />
              <path
                d="M10 19H6a4 4 0 0 1-4-4v-4"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
              />
            </svg>
          </a>
          <a
            v-if="state.player.type === 'live' && originUrl"
            class="live-entry-btn"
            :href="originUrl"
            target="_blank"
            rel="noreferrer"
          >
            <span class="equalizer" aria-hidden="true">
              <span></span>
              <span></span>
              <span></span>
            </span>
            点击进入直播间
          </a>
          <button
            v-if="state.player.replayReady"
            class="stage-replay"
            type="button"
            @click="replayCurrent"
          >
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path
                d="M4 12a8 8 0 1 1 2.3 5.7"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
              />
              <path
                d="M4 8v4h4"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
                stroke-linejoin="round"
              />
            </svg>
            重新播放
          </button>
          <button
            class="fullscreen-btn"
            type="button"
            aria-label="全屏"
            @click="toggleFullscreen"
          >
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path
                d="M9 3H5a2 2 0 0 0-2 2v4m0 6v4a2 2 0 0 0 2 2h4m6-18h4a2 2 0 0 1 2 2v4m0 6v4a2 2 0 0 1-2 2h-4"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
              />
            </svg>
          </button>
          <div class="mobile-overlay">
            <div class="mobile-info">
              <div class="mobile-author">
                {{ state.player.nickname ? `@${state.player.nickname}` : "@未知账号" }}
              </div>
              <div
                class="mobile-title"
                :class="{ expanded: state.mobileTitleExpanded }"
                @click.stop="toggleMobileTitle"
              >
                {{ state.player.title || "" }}
              </div>
            </div>
            <div class="mobile-actions">
              <img
                v-if="state.player.avatar"
                class="mobile-avatar"
                :src="mediaUrl(state.player.avatar)"
                referrerpolicy="no-referrer"
                alt="头像"
              />
              <a
                v-if="originUrl"
                class="mobile-link"
                :href="originUrl"
                target="_blank"
                rel="noreferrer"
                aria-label="打开原链接"
              >
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path
                    d="M14 5h5v5m0-5-7 7m-6 2a4 4 0 0 1 4-4h4"
                    fill="none"
                    stroke="currentColor"
                    stroke-width="2"
                    stroke-linecap="round"
                  />
                  <path
                    d="M10 19H6a4 4 0 0 1-4-4v-4"
                    fill="none"
                    stroke="currentColor"
                    stroke-width="2"
                    stroke-linecap="round"
                  />
                </svg>
              </a>
            </div>
          </div>
          <div v-if="state.player.loading" class="stage-mask">
            {{ state.player.loadingHint || "正在加载播放源..." }}
          </div>
          <div v-else-if="state.player.error" class="stage-mask error">
            {{ state.player.error }}
          </div>
          <div v-else-if="state.player.notice" class="stage-mask">
            {{ state.player.notice }}
          </div>
          <div v-else-if="state.player.nextPreview" class="stage-hint">
            即将播放：{{ state.player.nextPreview }}
          </div>
        </div>

      </main>
    </section>
  </div>
</template>

<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import Hls from "hls.js";

const videoRef = ref(null);
const stageRef = ref(null);
let hlsInstance = null;
let feedStream = null;
let feedRefreshTimer = null;
let feedRefreshPending = false;
const playbackPositions = new Map();
const playbackCompleted = new Set();

const state = reactive({
  items: [],
  total: 0,
  page: 1,
  pageSize: 30,
  loading: false,
  loadingMore: false,
  activeIndex: -1,
  sidebarCollapsed: false,
  audioUnlocked: false,
  mobileTitleExpanded: false,
  player: {
    type: "",
    title: "",
    cover: "",
    avatar: "",
    nickname: "",
    sort_time: "",
    src: "",
    loading: false,
    error: "",
    orientation: "vertical",
    width: 0,
    height: 0,
    displayWidth: 0,
    displayHeight: 0,
    nextPreview: "",
    loadingHint: "",
    resumeTime: 0,
    pendingPlay: false,
    suppressAutoplay: false,
    replayReady: false,
    notice: "",
  },
});

const activeItem = computed(() => state.items[state.activeIndex] || null);
const hasMore = computed(() => state.items.length < state.total);
const hasPrev = computed(() => state.items.length > 0 && state.activeIndex > 0);
const hasNext = computed(
  () => state.items.length > 0 && state.activeIndex < state.items.length - 1
);
const activeTypeLabel = computed(() => {
  if (!state.player.type) {
    return "播放源";
  }
  return state.player.type === "live" ? "直播" : "视频";
});
const stageStyle = computed(() => {
  if (!state.player.cover) {
    return {};
  }
  return {
    "--stage-cover": `url(${mediaUrl(state.player.cover)})`,
  };
});
const playerVideoStyle = computed(() => {
  if (state.player.displayWidth && state.player.displayHeight) {
    return {
      width: `${state.player.displayWidth}px`,
      height: `${state.player.displayHeight}px`,
    };
  }
  return {};
});
const originUrl = computed(() => {
  if (!activeItem.value) {
    return "";
  }
  if (activeItem.value.type === "live") {
    if (activeItem.value.live_url) {
      return activeItem.value.live_url;
    }
    if (activeItem.value.web_rid) {
      return `https://live.douyin.com/${activeItem.value.web_rid}`;
    }
    if (activeItem.value.room_id) {
      return `https://live.douyin.com/${activeItem.value.room_id}`;
    }
    return "";
  }
  if (activeItem.value.aweme_id) {
    return `https://www.douyin.com/video/${activeItem.value.aweme_id}`;
  }
  return "";
});

const prefetchCache = new Map();
const prefetchQueue = new Set();
const ACTIVE_ITEM_KEY = "douyin-client-active-id";
const rawApiBase = import.meta.env.VITE_API_BASE || "";
const apiBase = rawApiBase.endsWith("/") ? rawApiBase.slice(0, -1) : rawApiBase;

const buildApiUrl = (path) => {
  if (!apiBase) {
    return path;
  }
  if (path.startsWith("http://") || path.startsWith("https://")) {
    return path;
  }
  if (path.startsWith("/")) {
    return `${apiBase}${path}`;
  }
  return `${apiBase}/${path}`;
};

const resolveOrientation = (width, height) => {
  const w = Number(width) || 0;
  const h = Number(height) || 0;
  if (!w || !h) {
    return "";
  }
  return w >= h ? "horizontal" : "vertical";
};

const getItemIdentity = (item) => {
  if (!item) {
    return "";
  }
  if (item.type === "live") {
    const liveId = item.sec_user_id || item.web_rid || item.room_id || "";
    return liveId ? `live:${liveId}` : "";
  }
  const awemeId = item.aweme_id || "";
  return awemeId ? `video:${awemeId}` : "";
};

const readActiveIdentity = () => {
  try {
    return localStorage.getItem(ACTIVE_ITEM_KEY) || "";
  } catch (error) {
    return "";
  }
};

const writeActiveIdentity = (item) => {
  const identity = getItemIdentity(item);
  if (!identity) {
    return;
  }
  try {
    localStorage.setItem(ACTIVE_ITEM_KEY, identity);
  } catch (error) {
  }
};

const applyPlayerSize = (width, height) => {
  const w = Number(width) || 0;
  const h = Number(height) || 0;
  state.player.width = w;
  state.player.height = h;
  const orientation = resolveOrientation(w, h);
  if (orientation) {
    state.player.orientation = orientation;
  }
  void nextTick(updateDisplaySize);
};

const applyResumeTime = () => {
  const resumeAt = Number(state.player.resumeTime) || 0;
  const video = videoRef.value;
  if (!resumeAt || !video || state.player.type !== "video") {
    return false;
  }
  const duration = Number(video.duration) || 0;
  if (!duration) {
    return false;
  }
  const safeTarget = Math.min(resumeAt, Math.max(0, duration - 0.4));
  try {
    video.currentTime = safeTarget;
  } catch (error) {
  }
  state.player.resumeTime = 0;
  return true;
};

const preparePlaybackState = (item) => {
  state.player.resumeTime = 0;
  state.player.pendingPlay = false;
  state.player.suppressAutoplay = false;
  state.player.replayReady = false;
  state.player.notice = "";
  if (!item || item.type !== "video") {
    return;
  }
  const identity = getItemIdentity(item);
  if (!identity) {
    return;
  }
  if (playbackCompleted.has(identity)) {
    state.player.suppressAutoplay = true;
    state.player.replayReady = true;
    return;
  }
  const resumeAt = playbackPositions.get(identity);
  if (resumeAt && resumeAt > 1) {
    state.player.resumeTime = resumeAt;
  }
};

const showDeletedNotice = () => {
  state.player.notice = "当前作品已删除";
  state.player.error = "";
  state.player.loading = false;
  state.player.nextPreview = "";
  state.player.replayReady = false;
  state.player.suppressAutoplay = true;
  cleanupPlayer();
};

const toggleSidebar = () => {
  state.sidebarCollapsed = !state.sidebarCollapsed;
};

const mediaUrl = (url) => {
  if (!url) {
    return "";
  }
  if (url.startsWith("http://") || url.startsWith("https://")) {
    return url;
  }
  if (url.startsWith("/")) {
    return buildApiUrl(url);
  }
  return buildApiUrl(`/client/douyin/media?url=${encodeURIComponent(url)}`);
};

const formatFeedTime = (value) => {
  if (!value) {
    return "";
  }
  const text = String(value);
  if (text.length >= 16) {
    return text.slice(5, 16);
  }
  return text;
};

const shouldProxyStream = (url) => {
  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    return host.includes("douyin") || host.includes("douyinvod");
  } catch (error) {
    return false;
  }
};

const streamUrl = (url) => {
  if (!url) {
    return "";
  }
  if (url.includes("/client/douyin/stream?url=")) {
    return url.startsWith("/") ? buildApiUrl(url) : url;
  }
  if (url.startsWith("http://") || url.startsWith("https://")) {
    if (shouldProxyStream(url)) {
      return buildApiUrl(`/client/douyin/stream?url=${encodeURIComponent(url)}`);
    }
    return url;
  }
  if (url.startsWith("/")) {
    return buildApiUrl(url);
  }
  return buildApiUrl(`/client/douyin/stream?url=${encodeURIComponent(url)}`);
};

const apiRequest = async (path, options = {}) => {
  const headers = options.body ? { "Content-Type": "application/json" } : {};
  const response = await fetch(buildApiUrl(path), {
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
    throw new Error(detail || "请求失败");
  }
  return data;
};

const itemKey = (item, index) => {
  if (item.type === "live") {
    return `live-${item.sec_user_id}-${index}`;
  }
  return `video-${item.aweme_id || index}`;
};

const loadFeed = async (append) => {
  if (!append) {
    state.loading = true;
  } else {
    state.loadingMore = true;
  }
  try {
    const query = new URLSearchParams({
      page: String(state.page),
      page_size: String(state.pageSize),
    });
    const data = await apiRequest(`/client/douyin/daily/feed?${query.toString()}`);
    state.total = data.total || 0;
    const items = data.items || [];
    state.items = append ? [...state.items, ...items] : items;
    if (!append && state.items.length) {
      const storedId = readActiveIdentity();
      const currentId = getItemIdentity(activeItem.value);
      const targetId = storedId || currentId;
      const index = targetId
        ? state.items.findIndex((item) => getItemIdentity(item) === targetId)
        : -1;
      await selectItem(index >= 0 ? index : 0, false);
    } else if (state.activeIndex === -1 && state.items.length) {
      await selectItem(0, false);
    }
  } catch (error) {
    state.items = append ? state.items : [];
    state.total = append ? state.total : 0;
    state.player.error = error.message || "加载失败";
  } finally {
    state.loading = false;
    state.loadingMore = false;
  }
};

const refreshFeedSilently = async (cause = "") => {
  const deleteCause = cause === "delete" || cause === "cleanup";
  if (feedRefreshPending) {
    return;
  }
  feedRefreshPending = true;
  try {
    const query = new URLSearchParams({
      page: "1",
      page_size: String(state.pageSize),
    });
    const data = await apiRequest(`/client/douyin/daily/feed?${query.toString()}`);
    const items = data.items || [];
    const activeId = getItemIdentity(activeItem.value) || readActiveIdentity();
    state.total = data.total || 0;
    state.page = 1;
    if (!items.length) {
      state.items = [];
      state.activeIndex = -1;
      if (deleteCause) {
        showDeletedNotice();
      }
      return;
    }
    state.items = items;
    if (activeId) {
      const index = items.findIndex((item) => getItemIdentity(item) === activeId);
      if (index >= 0) {
        state.activeIndex = index;
        return;
      }
    }
    if (deleteCause) {
      state.activeIndex = -1;
      showDeletedNotice();
      return;
    }
    await selectItem(0, false);
  } catch (error) {
  } finally {
    feedRefreshPending = false;
  }
};

const scheduleFeedRefresh = (cause = "") => {
  if (feedRefreshTimer) {
    clearTimeout(feedRefreshTimer);
  }
  feedRefreshTimer = setTimeout(() => {
    refreshFeedSilently(cause);
  }, 600);
};

const connectFeedStream = () => {
  if (feedStream) {
    return;
  }
  if (typeof EventSource === "undefined") {
    return;
  }
  feedStream = new EventSource(buildApiUrl("/client/douyin/feed/stream"));
  feedStream.addEventListener("feed", (event) => {
    let payload = {};
    try {
      payload = JSON.parse(event.data || "{}");
    } catch (error) {
    }
    scheduleFeedRefresh(payload?.reason || "");
  });
  feedStream.onmessage = () => scheduleFeedRefresh("");
  feedStream.onerror = () => {
    if (feedStream) {
      feedStream.close();
      feedStream = null;
    }
    setTimeout(connectFeedStream, 4000);
  };
};

const loadMore = async () => {
  if (!hasMore.value || state.loadingMore) {
    return;
  }
  state.page += 1;
  await loadFeed(true);
};

const cleanupPlayer = () => {
  if (hlsInstance) {
    hlsInstance.destroy();
    hlsInstance = null;
  }
  const video = videoRef.value;
  if (video) {
    video.pause();
    video.removeAttribute("src");
    video.load();
  }
};

const playMedia = async () => {
  const video = videoRef.value;
  if (!video) {
    return;
  }
  try {
    video.muted = !state.audioUnlocked;
    video.volume = state.audioUnlocked ? 1 : 0;
    await video.play();
  } catch (error) {
  }
};

const unlockAudio = () => {
  if (state.audioUnlocked) {
    return;
  }
  state.audioUnlocked = true;
  const media = videoRef.value;
  if (!media) {
    return;
  }
  media.muted = false;
  media.volume = 1;
  media.play().catch(() => {});
};

const toggleMobileTitle = () => {
  state.mobileTitleExpanded = !state.mobileTitleExpanded;
};

const updateOrientation = () => {
  const video = videoRef.value;
  if (video && video.videoWidth && video.videoHeight) {
    applyPlayerSize(video.videoWidth, video.videoHeight);
    if (applyResumeTime() && state.player.pendingPlay) {
      state.player.pendingPlay = false;
      playMedia();
    }
    updateDisplaySize();
    return;
  }
  if (state.player.width && state.player.height) {
    applyPlayerSize(state.player.width, state.player.height);
  }
};

const updateDisplaySize = () => {
  const stage = stageRef.value;
  const w = Number(state.player.width) || 0;
  const h = Number(state.player.height) || 0;
  if (!stage || !w || !h) {
    state.player.displayWidth = 0;
    state.player.displayHeight = 0;
    return;
  }
  const maxW = stage.clientWidth || 0;
  const maxH = stage.clientHeight || 0;
  if (!maxW || !maxH) {
    return;
  }
  const scale = Math.min(maxW / w, maxH / h);
  state.player.displayWidth = Math.max(1, Math.floor(w * scale));
  state.player.displayHeight = Math.max(1, Math.floor(h * scale));
};

const prefetchDetail = async (item) => {
  if (!item || item.type !== "video" || !item.aweme_id) {
    return;
  }
  if (prefetchCache.has(item.aweme_id) || prefetchQueue.has(item.aweme_id)) {
    return;
  }
  prefetchQueue.add(item.aweme_id);
  try {
    const data = await apiRequest(
      `/client/douyin/detail?aweme_id=${encodeURIComponent(item.aweme_id)}`
    );
    const detail = data.data || {};
    if (detail.type === "video" && detail.video_url) {
      prefetchCache.set(item.aweme_id, detail);
    }
  } catch (error) {
  } finally {
    prefetchQueue.delete(item.aweme_id);
  }
};

const prefetchNext = async (index) => {
  const nextItem = state.items[index + 1];
  if (!nextItem || nextItem.type !== "video") {
    return;
  }
  await prefetchDetail(nextItem);
};

const toggleFullscreen = async () => {
  const target = stageRef.value || videoRef.value;
  if (!target) {
    return;
  }
  try {
    if (!document.fullscreenElement) {
      if (target.requestFullscreen) {
        await target.requestFullscreen();
      } else if (target.webkitRequestFullscreen) {
        target.webkitRequestFullscreen();
      }
      return;
    }
    if (document.exitFullscreen) {
      await document.exitFullscreen();
    }
  } catch (error) {
  }
};

const replayCurrent = async () => {
  const item = activeItem.value;
  if (!item || item.type !== "video") {
    return;
  }
  const identity = getItemIdentity(item);
  if (identity) {
    playbackCompleted.delete(identity);
  }
  state.player.replayReady = false;
  state.player.suppressAutoplay = false;
  state.player.notice = "";
  const video = videoRef.value;
  if (video) {
    try {
      video.currentTime = 0;
    } catch (error) {
    }
  }
  await playMedia();
};

const attachVideo = async (url) => {
  cleanupPlayer();
  const video = videoRef.value;
  if (!video) {
    return;
  }
  const sourceUrl = streamUrl(url);
  if (!sourceUrl) {
    state.player.error = "未获取到播放地址";
    return;
  }
  video.src = sourceUrl;
  await nextTick();
  updateOrientation();
  if (!state.player.suppressAutoplay) {
    if (state.player.resumeTime) {
      state.player.pendingPlay = true;
    } else {
      await playMedia();
    }
  }
};

const attachLive = async (url) => {
  cleanupPlayer();
  const video = videoRef.value;
  if (!video) {
    return;
  }
  const sourceUrl = streamUrl(url);
  if (!sourceUrl) {
    state.player.error = "未获取到直播地址";
    return;
  }
  if (Hls.isSupported()) {
    hlsInstance = new Hls({
      lowLatencyMode: true,
      backBufferLength: 30,
    });
    hlsInstance.loadSource(sourceUrl);
    hlsInstance.attachMedia(video);
  } else if (video.canPlayType("application/vnd.apple.mpegurl")) {
    video.src = sourceUrl;
  } else {
    state.player.error = "当前浏览器不支持直播播放";
    return;
  }
  await nextTick();
  updateOrientation();
  await playMedia();
};

const pickStreamUrl = (hlsMap, flvMap) => {
  const hlsValues = hlsMap && typeof hlsMap === "object" ? Object.values(hlsMap) : [];
  if (hlsValues.length) {
    return hlsValues[0];
  }
  const flvValues = flvMap && typeof flvMap === "object" ? Object.values(flvMap) : [];
  if (flvValues.length) {
    return flvValues[0];
  }
  return "";
};

const resolveVideoSource = async (item) => {
  state.player.loading = true;
  state.player.error = "";
  state.player.type = "video";
  state.player.cover = item.cover || "";
  state.player.title = item.title || item.aweme_id || "未命名作品";
  state.player.avatar = item.avatar || "";
  state.player.nickname = item.nickname || "";
  state.player.sort_time = item.sort_time || "";
  state.player.orientation = "vertical";
  applyPlayerSize(item.width, item.height);
  try {
    let detail = prefetchCache.get(item.aweme_id);
    if (!detail) {
      const data = await apiRequest(
        `/client/douyin/detail?aweme_id=${encodeURIComponent(item.aweme_id || "")}`
      );
      detail = data.data || {};
    }
    if (detail.type === "note") {
      state.player.error = "该内容为图文，暂不支持播放";
      return;
    }
    if (!detail.video_url) {
      state.player.error = "未获取到视频地址";
      return;
    }
    state.player.cover = detail.cover || state.player.cover;
    state.player.title = detail.title || state.player.title;
    state.player.nickname = detail.nickname || state.player.nickname;
    state.player.avatar = detail.avatar || state.player.avatar;
    applyPlayerSize(detail.width, detail.height);
    await attachVideo(detail.video_url);
  } catch (error) {
    state.player.error = error.message || "获取视频失败";
  } finally {
    state.player.loading = false;
    state.player.loadingHint = "";
  }
};

const resolveLiveSource = async (item) => {
  state.player.loading = true;
  state.player.error = "";
  state.player.type = "live";
  state.player.cover = item.cover || "";
  state.player.title = item.title || "直播中";
  state.player.avatar = item.avatar || "";
  state.player.nickname = item.nickname || "";
  state.player.sort_time = item.sort_time || item.last_live_at || "";
  state.player.orientation = "vertical";
  applyPlayerSize(item.width, item.height);
  let hlsMap = item.hls_pull_url_map || {};
  let flvMap = item.flv_pull_url || {};
  try {
    if (!Object.keys(hlsMap).length && !Object.keys(flvMap).length) {
      const data = await apiRequest(
        `/client/douyin/users/${encodeURIComponent(item.sec_user_id)}/live`,
        { method: "POST" }
      );
      const liveData = data.data || {};
      const room = liveData.room || {};
      hlsMap = room.hls_pull_url_map || {};
      flvMap = room.flv_pull_url || {};
      state.player.cover = room.cover || state.player.cover;
      state.player.title = room.title || state.player.title;
      state.player.sort_time = item.last_live_at || state.player.sort_time;
      applyPlayerSize(room.width, room.height);
    }
    const streamUrl = pickStreamUrl(hlsMap, flvMap);
    if (!streamUrl) {
      state.player.error = "未获取到直播流";
      return;
    }
    await attachLive(streamUrl);
  } catch (error) {
    state.player.error = error.message || "直播加载失败";
  } finally {
    state.player.loading = false;
    state.player.loadingHint = "";
  }
};

const selectItem = async (index, userAction, keepLoadingHint = false) => {
  if (index < 0 || index >= state.items.length) {
    return;
  }
  if (index === state.activeIndex) {
    if (userAction) {
      unlockAudio();
    }
    return;
  }
  state.activeIndex = index;
  state.mobileTitleExpanded = false;
  state.player.nextPreview = "";
  state.player.notice = "";
  if (!keepLoadingHint) {
    state.player.loadingHint = "";
  }
  if (userAction) {
    unlockAudio();
  }
  const item = state.items[index];
  if (!item) {
    return;
  }
  preparePlaybackState(item);
  writeActiveIdentity(item);
  if (item.type === "live") {
    await resolveLiveSource(item);
  } else {
    await resolveVideoSource(item);
  }
  void prefetchNext(index);
};

const playNext = async (autoAdvance = false) => {
  if (!hasNext.value) {
    return;
  }
  if (autoAdvance) {
    const nextItem = state.items[state.activeIndex + 1];
    if (nextItem) {
      state.player.loadingHint = `即将播放：${nextItem.title || nextItem.aweme_id || "下一条"}`;
    }
  }
  unlockAudio();
  await selectItem(state.activeIndex + 1, true, autoAdvance);
};

const playPrev = async () => {
  if (!hasPrev.value) {
    return;
  }
  unlockAudio();
  await selectItem(state.activeIndex - 1, true);
};

const handleEnded = async () => {
  if (state.player.type === "video") {
    const identity = getItemIdentity(activeItem.value);
    if (identity) {
      playbackCompleted.add(identity);
      playbackPositions.delete(identity);
    }
  }
  if (hasNext.value) {
    state.player.nextPreview = "";
    await playNext(true);
    return;
  }
  if (state.items.length) {
    state.player.loadingHint = `即将播放：${state.items[0].title || state.items[0].aweme_id || "下一条"}`;
    await selectItem(0, true, true);
  }
};

const handleTimeUpdate = () => {
  const video = videoRef.value;
  if (!video || state.player.type !== "video") {
    state.player.nextPreview = "";
    return;
  }
  const identity = getItemIdentity(activeItem.value);
  if (identity && !playbackCompleted.has(identity)) {
    playbackPositions.set(identity, video.currentTime || 0);
  }
  if (!hasNext.value || !Number.isFinite(video.duration) || video.duration <= 0) {
    state.player.nextPreview = "";
    return;
  }
  const remaining = video.duration - video.currentTime;
  if (remaining <= 3) {
    const nextItem = state.items[state.activeIndex + 1];
    state.player.nextPreview = nextItem
      ? nextItem.title || nextItem.aweme_id || "下一条"
      : "";
  } else {
    state.player.nextPreview = "";
  }
};

const swipe = reactive({
  startX: 0,
  startY: 0,
  active: false,
});

let resizeHandler = null;
let unlockHandler = null;

const onTouchStart = (event) => {
  const touch = event.touches?.[0];
  if (!touch) {
    return;
  }
  swipe.startX = touch.clientX;
  swipe.startY = touch.clientY;
  swipe.active = true;
};

const onTouchEnd = async (event) => {
  if (!swipe.active) {
    return;
  }
  swipe.active = false;
  const touch = event.changedTouches?.[0];
  if (!touch) {
    return;
  }
  const deltaX = touch.clientX - swipe.startX;
  const deltaY = touch.clientY - swipe.startY;
  if (Math.abs(deltaY) < 60 || Math.abs(deltaY) < Math.abs(deltaX)) {
    return;
  }
  unlockAudio();
  if (deltaY < 0) {
    await playNext();
  } else {
    await playPrev();
  }
};

let lastWheelAt = 0;
const onStageWheel = async (event) => {
  if (state.player.loading) {
    return;
  }
  const now = Date.now();
  if (now - lastWheelAt < 650) {
    return;
  }
  if (Math.abs(event.deltaY) < 45) {
    return;
  }
  lastWheelAt = now;
  if (event.deltaY > 0) {
    await playNext();
  } else {
    await playPrev();
  }
};

const onFeedScroll = async (event) => {
  const target = event.target;
  if (!target) {
    return;
  }
  const reachBottom = target.scrollTop + target.clientHeight >= target.scrollHeight - 120;
  if (reachBottom) {
    await loadMore();
  }
};

onMounted(async () => {
  await loadFeed(false);
  connectFeedStream();
  const video = videoRef.value;
  if (video) {
    video.addEventListener("ended", handleEnded);
    video.addEventListener("loadedmetadata", updateOrientation);
    video.addEventListener("loadeddata", updateOrientation);
    video.addEventListener("timeupdate", handleTimeUpdate);
  }
  state.sidebarCollapsed = true;
  unlockHandler = unlockAudio;
  window.addEventListener("pointerdown", unlockHandler, { once: true });
  resizeHandler = () => {
    if (window.innerWidth < 980) {
      state.sidebarCollapsed = true;
    }
    updateDisplaySize();
  };
  window.addEventListener("resize", resizeHandler);
  void nextTick(updateDisplaySize);
});

watch(
  () => state.sidebarCollapsed,
  async () => {
    await nextTick();
    updateDisplaySize();
    if (state.audioUnlocked) {
      await playMedia();
    }
  }
);

onBeforeUnmount(() => {
  const video = videoRef.value;
  if (video) {
    video.removeEventListener("ended", handleEnded);
    video.removeEventListener("loadedmetadata", updateOrientation);
    video.removeEventListener("loadeddata", updateOrientation);
    video.removeEventListener("timeupdate", handleTimeUpdate);
  }
  cleanupPlayer();
  if (resizeHandler) {
    window.removeEventListener("resize", resizeHandler);
  }
  if (unlockHandler) {
    window.removeEventListener("pointerdown", unlockHandler);
  }
  if (feedStream) {
    feedStream.close();
    feedStream = null;
  }
  if (feedRefreshTimer) {
    clearTimeout(feedRefreshTimer);
    feedRefreshTimer = null;
  }
});
</script>
