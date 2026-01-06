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
              <span
                v-if="item.type === 'video' && isPlaybackCompleted(item)"
                class="thumb-replay"
                role="button"
                tabindex="0"
                aria-label="重新播放"
                @click.stop="replayFromList(item, index)"
                @keydown.enter.prevent.stop="replayFromList(item, index)"
                @keydown.space.prevent.stop="replayFromList(item, index)"
              >
                <span class="thumb-replay-icon" aria-hidden="true">
                  <svg viewBox="0 0 24 24">
                    <path
                      d="M5 12a7 7 0 1 1 2.1 4.9"
                      fill="none"
                      stroke="currentColor"
                      stroke-width="2"
                      stroke-linecap="round"
                    />
                    <path
                      d="M5 8v4h4"
                      fill="none"
                      stroke="currentColor"
                      stroke-width="2"
                      stroke-linecap="round"
                      stroke-linejoin="round"
                    />
                  </svg>
                </span>
              </span>
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
                  v-if="index === state.activeIndex && !isPlaybackCompleted(item)"
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
            <div v-if="item.type === 'video'" class="feed-progress">
              <div
                class="feed-progress-bar"
                :style="{ width: `${getPlaybackPercent(item)}%` }"
              ></div>
            </div>
            <div v-if="item.type === 'video'" class="feed-progress-text">
              {{ getPlaybackLabel(item) }}
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
            :preload="videoPreload"
            :poster="state.player.cover ? mediaUrl(state.player.cover) : ''"
            :style="playerVideoStyle"
          ></video>
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
let hlsRetryTimer = null;
const hlsRecovery = {
  network: 0,
  media: 0,
  rebuild: 0,
};
const HLS_NETWORK_RETRY_LIMIT = 4;
const HLS_MEDIA_RETRY_LIMIT = 2;
const HLS_REBUILD_LIMIT = 2;
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
const videoPreload = computed(() => {
  return state.player.type === "video" ? "auto" : "metadata";
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
const prefetchSegmentCache = new Set();
const prefetchSegmentQueue = new Set();
const PREFETCH_SEGMENT_BYTES = 512 * 1024;
const ACTIVE_ITEM_KEY = "douyin-client-active-id";
const PLAYBACK_STORE_KEY = "douyin-client-playback-positions";
const PLAYBACK_STORE_LIMIT = 180;
const PLAYBACK_PERSIST_INTERVAL = 4;
const rawApiBase = import.meta.env.VITE_API_BASE || "";
const apiBase = rawApiBase.endsWith("/") ? rawApiBase.slice(0, -1) : rawApiBase;
const playbackStore = { loaded: false, items: {} };
const playbackPersisted = new Map();
let playbackSaveTimer = null;
const playbackRevision = ref(0);
let lastPlaybackSecond = -1;
const playbackHeal = {
  timer: null,
  attempts: 0,
  lastAt: 0,
};
const HEAL_RETRY_WINDOW = 15000;
const HEAL_MAX_ATTEMPTS = 3;
const HEAL_DELAY = 1200;

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

const bumpPlaybackRevision = () => {
  playbackRevision.value += 1;
};

const loadPlaybackStore = () => {
  if (playbackStore.loaded) {
    return;
  }
  playbackStore.loaded = true;
  try {
    const raw = localStorage.getItem(PLAYBACK_STORE_KEY);
    if (!raw) {
      return;
    }
    const data = JSON.parse(raw);
    if (data && typeof data === "object") {
      playbackStore.items = data;
    }
  } catch (error) {
  }
};

const flushPlaybackStore = () => {
  if (!playbackStore.loaded) {
    return;
  }
  try {
    localStorage.setItem(PLAYBACK_STORE_KEY, JSON.stringify(playbackStore.items));
  } catch (error) {
  }
};

const schedulePlaybackSave = () => {
  if (playbackSaveTimer) {
    return;
  }
  playbackSaveTimer = setTimeout(() => {
    playbackSaveTimer = null;
    flushPlaybackStore();
  }, 1000);
};

const prunePlaybackStore = () => {
  const entries = Object.entries(playbackStore.items);
  if (entries.length <= PLAYBACK_STORE_LIMIT) {
    return;
  }
  entries.sort((a, b) => (a[1]?.updated_at || 0) - (b[1]?.updated_at || 0));
  const removeCount = entries.length - PLAYBACK_STORE_LIMIT;
  for (let index = 0; index < removeCount; index += 1) {
    delete playbackStore.items[entries[index][0]];
  }
};

const getStoredPlaybackRecord = (identity) => {
  if (!identity) {
    return null;
  }
  loadPlaybackStore();
  const record = playbackStore.items[identity];
  return record && typeof record === "object" ? record : null;
};

const getStoredPlaybackPosition = (identity) => {
  if (!identity) {
    return 0;
  }
  const record = getStoredPlaybackRecord(identity);
  const time = Number(record?.time || 0);
  return Number.isFinite(time) ? time : 0;
};

const updatePlaybackRecord = (identity, payload) => {
  if (!identity) {
    return;
  }
  loadPlaybackStore();
  const record = playbackStore.items[identity] || {};
  const previousProgress =
    typeof record.progress === "number" && Number.isFinite(record.progress)
      ? record.progress
      : 0;
  if (payload && typeof payload === "object") {
    const force = Boolean(payload.force);
    if (typeof payload.time === "number" && Number.isFinite(payload.time)) {
      const safeTime = Math.max(0, payload.time);
      if (
        force ||
        !Number.isFinite(record.time) ||
        safeTime >= Number(record.time || 0)
      ) {
        record.time = safeTime;
      }
    }
    if (typeof payload.duration === "number" && Number.isFinite(payload.duration)) {
      if (payload.duration > 0) {
        record.duration = Math.max(Number(record.duration || 0), payload.duration);
      }
    }
    if (typeof payload.completed === "boolean") {
      record.completed = payload.completed;
    }
  }
  if (record.completed) {
    record.progress = 100;
  } else if (Number.isFinite(record.time) && Number.isFinite(record.duration) && record.duration > 0) {
    const nextProgress = Math.min(100, Math.floor((record.time / record.duration) * 100));
    record.progress = Math.max(previousProgress, nextProgress);
  } else if (previousProgress) {
    record.progress = previousProgress;
  }
  record.updated_at = Date.now();
  playbackStore.items[identity] = record;
  prunePlaybackStore();
  schedulePlaybackSave();
};

const setStoredPlaybackPosition = (identity, time, duration, force = false) => {
  updatePlaybackRecord(identity, { time, duration, force });
};

const setStoredPlaybackCompleted = (identity, completed) => {
  updatePlaybackRecord(identity, { completed });
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

const resetPlaybackHeal = () => {
  playbackHeal.attempts = 0;
  playbackHeal.lastAt = 0;
  if (playbackHeal.timer) {
    clearTimeout(playbackHeal.timer);
    playbackHeal.timer = null;
  }
};

const schedulePlaybackHeal = () => {
  if (playbackHeal.timer || state.player.loading) {
    return;
  }
  if (state.player.suppressAutoplay || state.player.replayReady) {
    return;
  }
  playbackHeal.timer = setTimeout(() => {
    playbackHeal.timer = null;
    void attemptPlaybackHeal();
  }, HEAL_DELAY);
};

const attemptPlaybackHeal = async () => {
  const video = videoRef.value;
  if (!video || state.player.loading || !state.player.src) {
    return;
  }
  if (video.paused && !state.player.pendingPlay) {
    return;
  }
  const now = Date.now();
  if (now - playbackHeal.lastAt > HEAL_RETRY_WINDOW) {
    playbackHeal.attempts = 0;
  }
  if (playbackHeal.attempts >= HEAL_MAX_ATTEMPTS) {
    return;
  }
  playbackHeal.attempts += 1;
  playbackHeal.lastAt = now;
  if (state.player.type === "live") {
    if (hlsInstance) {
      hlsInstance.startLoad();
      hlsInstance.recoverMediaError();
      await playMedia();
      if (playbackHeal.attempts >= 2) {
        scheduleLiveReattach(state.player.src);
      }
      return;
    }
    scheduleLiveReattach(state.player.src);
    return;
  }
  const resumeAt = video.currentTime || 0;
  state.player.resumeTime = resumeAt;
  state.player.pendingPlay = true;
  video.pause();
  video.src = state.player.src;
  video.load();
  await nextTick();
  updateOrientation();
  if (!state.player.resumeTime && !state.player.suppressAutoplay) {
    await playMedia();
  }
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
  const record = getStoredPlaybackRecord(identity);
  if (record?.completed) {
    playbackCompleted.add(identity);
  }
  if (playbackCompleted.has(identity)) {
    state.player.suppressAutoplay = true;
    state.player.replayReady = true;
    return;
  }
  const cachedResume = playbackPositions.get(identity) || 0;
  let resumeAt = cachedResume > 1 ? cachedResume : 0;
  if (!resumeAt) {
    resumeAt = getStoredPlaybackPosition(identity);
    if (resumeAt) {
      playbackPositions.set(identity, resumeAt);
    }
  }
  if (resumeAt && resumeAt > 1) {
    state.player.resumeTime = resumeAt;
  }
};

const isPlaybackCompleted = (item) => {
  if (!item || item.type !== "video") {
    return false;
  }
  playbackRevision.value;
  const identity = getItemIdentity(item);
  if (!identity) {
    return false;
  }
  if (playbackCompleted.has(identity)) {
    return true;
  }
  const record = getStoredPlaybackRecord(identity);
  return Boolean(record?.completed);
};

const getPlaybackPercent = (item) => {
  playbackRevision.value;
  if (!item || item.type !== "video") {
    return 0;
  }
  const identity = getItemIdentity(item);
  const record = getStoredPlaybackRecord(identity);
  if (record?.completed || playbackCompleted.has(identity)) {
    return 100;
  }
  const storedProgress =
    typeof record?.progress === "number" && Number.isFinite(record.progress)
      ? record.progress
      : null;
  const duration = Number(record?.duration || item.duration || 0);
  if (!duration) {
    return storedProgress ?? 0;
  }
  const timeValue = Math.max(
    Number(playbackPositions.get(identity) || 0),
    Number(record?.time || 0),
  );
  const percent = Math.min(100, Math.max(0, (timeValue / duration) * 100));
  const normalized = Math.floor(percent);
  if (storedProgress !== null) {
    return Math.min(100, Math.max(storedProgress, normalized));
  }
  return normalized;
};

const getPlaybackLabel = (item) => {
  if (!item || item.type !== "video") {
    return "";
  }
  playbackRevision.value;
  const identity = getItemIdentity(item);
  const record = getStoredPlaybackRecord(identity);
  if (record?.completed || playbackCompleted.has(identity)) {
    return "已看完";
  }
  const percent = getPlaybackPercent(item);
  if (percent <= 0) {
    return "未播放";
  }
  return `已播 ${percent}%`;
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

const streamUrl = (url, options = {}) => {
  if (!url) {
    return "";
  }
  const isLive = Boolean(options.live);
  const streamPath = isLive ? "/client/douyin/stream-live" : "/client/douyin/stream";
  if (url.includes("/client/douyin/stream-live?url=") || url.includes("/client/douyin/stream?url=")) {
    return url.startsWith("/") ? buildApiUrl(url) : url;
  }
  if (url.startsWith("http://") || url.startsWith("https://")) {
    if (shouldProxyStream(url)) {
      return buildApiUrl(`${streamPath}?url=${encodeURIComponent(url)}`);
    }
    return url;
  }
  if (url.startsWith("/")) {
    return buildApiUrl(url);
  }
  return buildApiUrl(`${streamPath}?url=${encodeURIComponent(url)}`);
};

const prefetchStreamUrl = (url) => {
  if (!url) {
    return "";
  }
  if (url.includes("/client/douyin/stream?url=")) {
    return url.startsWith("/") ? buildApiUrl(url) : url;
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
  resetPlaybackHeal();
  if (hlsInstance) {
    hlsInstance.destroy();
    hlsInstance = null;
  }
  if (hlsRetryTimer) {
    clearTimeout(hlsRetryTimer);
    hlsRetryTimer = null;
  }
  const video = videoRef.value;
  if (video) {
    video.pause();
    video.removeAttribute("src");
    video.load();
  }
  state.player.src = "";
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
  if (state.player.suppressAutoplay || state.player.replayReady) {
    return;
  }
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

const prefetchStreamSegment = async (item, detail) => {
  if (!item || item.type !== "video") {
    return;
  }
  const awemeId = item.aweme_id || "";
  if (!awemeId || !detail?.video_url) {
    return;
  }
  if (prefetchSegmentCache.has(awemeId) || prefetchSegmentQueue.has(awemeId)) {
    return;
  }
  const url = prefetchStreamUrl(detail.video_url);
  if (!url) {
    return;
  }
  prefetchSegmentQueue.add(awemeId);
  try {
    const response = await fetch(url, {
      headers: {
        Range: `bytes=0-${PREFETCH_SEGMENT_BYTES - 1}`,
      },
    });
    if (response.ok || response.status === 206) {
      await response.arrayBuffer();
      prefetchSegmentCache.add(awemeId);
    }
  } catch (error) {
  } finally {
    prefetchSegmentQueue.delete(awemeId);
  }
};

const prefetchNext = async (index) => {
  const nextItem = state.items[index + 1];
  if (!nextItem || nextItem.type !== "video") {
    return;
  }
  await prefetchDetail(nextItem);
  const cached = prefetchCache.get(nextItem.aweme_id);
  if (cached) {
    void prefetchStreamSegment(nextItem, cached);
  }
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
    const record = getStoredPlaybackRecord(identity);
    const duration = Number(record?.duration || videoRef.value?.duration || 0);
    playbackCompleted.delete(identity);
    setStoredPlaybackCompleted(identity, false);
    playbackPersisted.delete(identity);
    setStoredPlaybackPosition(identity, 0, duration, true);
    playbackPositions.set(identity, 0);
    playbackPersisted.set(identity, 0);
    bumpPlaybackRevision();
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

const replayFromList = async (item, index) => {
  if (!item || item.type !== "video") {
    return;
  }
  const identity = getItemIdentity(item);
  if (identity) {
    playbackCompleted.delete(identity);
    setStoredPlaybackCompleted(identity, false);
    playbackPersisted.delete(identity);
    playbackPositions.set(identity, 0);
    playbackPersisted.set(identity, 0);
    const record = getStoredPlaybackRecord(identity);
    const duration = Number(record?.duration || 0);
    setStoredPlaybackPosition(identity, 0, duration, true);
    bumpPlaybackRevision();
  }
  if (index !== state.activeIndex) {
    await selectItem(index, true);
    return;
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
  const sourceUrl = streamUrl(url, { live: true });
  if (!sourceUrl) {
    state.player.error = "未获取到播放地址";
    return;
  }
  state.player.src = sourceUrl;
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

const resetHlsRecovery = () => {
  hlsRecovery.network = 0;
  hlsRecovery.media = 0;
  hlsRecovery.rebuild = 0;
  if (hlsRetryTimer) {
    clearTimeout(hlsRetryTimer);
    hlsRetryTimer = null;
  }
};

const scheduleLiveReattach = (sourceUrl) => {
  if (hlsRetryTimer) {
    return;
  }
  hlsRetryTimer = setTimeout(() => {
    hlsRetryTimer = null;
    if (!sourceUrl || state.player.type !== "live" || state.player.src !== sourceUrl) {
      return;
    }
    attachLive(sourceUrl);
  }, 1200);
};

const handleHlsError = (data, sourceUrl) => {
  if (!data || !data.fatal) {
    return;
  }
  if (data.type === Hls.ErrorTypes.NETWORK_ERROR) {
    if (hlsRecovery.network < HLS_NETWORK_RETRY_LIMIT) {
      hlsRecovery.network += 1;
      hlsInstance?.startLoad();
      return;
    }
  } else if (data.type === Hls.ErrorTypes.MEDIA_ERROR) {
    if (hlsRecovery.media < HLS_MEDIA_RETRY_LIMIT) {
      hlsRecovery.media += 1;
      hlsInstance?.recoverMediaError();
      return;
    }
  }
  if (hlsRecovery.rebuild < HLS_REBUILD_LIMIT) {
    hlsRecovery.rebuild += 1;
    if (hlsInstance) {
      hlsInstance.destroy();
      hlsInstance = null;
    }
    scheduleLiveReattach(sourceUrl);
    return;
  }
  state.player.error = "直播流连接异常";
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
  state.player.src = sourceUrl;
  if (Hls.isSupported()) {
    resetHlsRecovery();
    hlsInstance = new Hls({
      lowLatencyMode: false,
      backBufferLength: 120,
      liveSyncDuration: 12,
      liveMaxLatencyDuration: 40,
      maxLiveSyncPlaybackRate: 1.0,
      maxBufferLength: 90,
      maxMaxBufferLength: 180,
      maxBufferSize: 120 * 1000 * 1000,
      capLevelToPlayerSize: true,
      enableWorker: true,
    });
    hlsInstance.on(Hls.Events.ERROR, (_, data) => {
      handleHlsError(data, sourceUrl);
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
  resetPlaybackHeal();
  if (index === state.activeIndex) {
    if (userAction) {
      unlockAudio();
    }
    return;
  }
  const currentItem = activeItem.value;
  if (currentItem && currentItem.type === "video") {
    const currentIdentity = getItemIdentity(currentItem);
    const currentVideo = videoRef.value;
    if (
      currentIdentity &&
      currentVideo &&
      !playbackCompleted.has(currentIdentity) &&
      Number.isFinite(currentVideo.currentTime)
    ) {
      const lastTime = Number(currentVideo.currentTime || 0);
      const storedTime = getStoredPlaybackPosition(currentIdentity) || 0;
      const cachedTime = playbackPositions.get(currentIdentity) || 0;
      const safeTime = Math.max(lastTime, storedTime, cachedTime);
      if (safeTime > 0) {
        playbackPositions.set(currentIdentity, safeTime);
        playbackPersisted.set(currentIdentity, safeTime);
        setStoredPlaybackPosition(currentIdentity, safeTime, currentVideo.duration || 0);
      }
      bumpPlaybackRevision();
    }
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
    const video = videoRef.value;
    if (identity) {
      const duration = Number(video?.duration || video?.currentTime || 0);
      playbackCompleted.add(identity);
      playbackPositions.set(identity, duration);
      playbackPersisted.set(identity, duration);
      updatePlaybackRecord(identity, {
        time: duration,
        duration,
        completed: true,
        force: true,
      });
      bumpPlaybackRevision();
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
    const currentTime = Number(video.currentTime || 0);
    const cachedTime = playbackPositions.get(identity) || 0;
    const storedTime = getStoredPlaybackPosition(identity) || 0;
    const safeTime = Math.max(currentTime, cachedTime, storedTime);
    playbackPositions.set(identity, safeTime);
    const lastPersisted = playbackPersisted.get(identity) || 0;
    if (Math.abs(safeTime - lastPersisted) >= PLAYBACK_PERSIST_INTERVAL) {
      playbackPersisted.set(identity, safeTime);
      setStoredPlaybackPosition(identity, safeTime, video.duration || 0);
    }
    const currentSecond = Math.floor(safeTime);
    if (currentSecond !== lastPlaybackSecond) {
      lastPlaybackSecond = currentSecond;
      bumpPlaybackRevision();
    }
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

const handlePlaybackStalled = () => {
  schedulePlaybackHeal();
};

const handlePlaybackWaiting = () => {
  schedulePlaybackHeal();
};

const handlePlaybackError = () => {
  schedulePlaybackHeal();
};

const handlePlaybackRecovered = () => {
  resetPlaybackHeal();
};

const handlePlaybackPause = () => {
  if (state.player.type !== "live") {
    return;
  }
  if (hlsInstance) {
    hlsInstance.stopLoad();
  }
};

const handlePlaybackPlay = () => {
  if (state.player.type !== "live") {
    return;
  }
  if (hlsInstance) {
    hlsInstance.startLoad();
  }
};

onMounted(async () => {
  loadPlaybackStore();
  await loadFeed(false);
  connectFeedStream();
  const video = videoRef.value;
  if (video) {
    video.addEventListener("ended", handleEnded);
    video.addEventListener("loadedmetadata", updateOrientation);
    video.addEventListener("loadeddata", updateOrientation);
    video.addEventListener("timeupdate", handleTimeUpdate);
    video.addEventListener("pause", handlePlaybackPause);
    video.addEventListener("play", handlePlaybackPlay);
    video.addEventListener("stalled", handlePlaybackStalled);
    video.addEventListener("waiting", handlePlaybackWaiting);
    video.addEventListener("error", handlePlaybackError);
    video.addEventListener("playing", handlePlaybackRecovered);
    video.addEventListener("canplay", handlePlaybackRecovered);
  }
  unlockHandler = unlockAudio;
  window.addEventListener("pointerdown", unlockHandler, { once: true });
  resizeHandler = () => {
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
  }
);

onBeforeUnmount(() => {
  const video = videoRef.value;
  if (video) {
    video.removeEventListener("ended", handleEnded);
    video.removeEventListener("loadedmetadata", updateOrientation);
    video.removeEventListener("loadeddata", updateOrientation);
    video.removeEventListener("timeupdate", handleTimeUpdate);
    video.removeEventListener("pause", handlePlaybackPause);
    video.removeEventListener("play", handlePlaybackPlay);
    video.removeEventListener("stalled", handlePlaybackStalled);
    video.removeEventListener("waiting", handlePlaybackWaiting);
    video.removeEventListener("error", handlePlaybackError);
    video.removeEventListener("playing", handlePlaybackRecovered);
    video.removeEventListener("canplay", handlePlaybackRecovered);
  }
  cleanupPlayer();
  if (playbackSaveTimer) {
    clearTimeout(playbackSaveTimer);
    playbackSaveTimer = null;
  }
  flushPlaybackStore();
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
