<template>
  <section class="card detail-hero">
    <div class="detail-hero-header">
      <RouterLink class="ghost btn-mini back-link" to="/playlists">
        返回列表
      </RouterLink>
      <div class="detail-actions">
        <button class="ghost" :disabled="state.loading.refresh" @click="reloadAll">
          刷新
        </button>
        <button class="ghost" :disabled="state.loading.clear" @click="clearPlaylist">
          清空
        </button>
        <button class="danger ghost" :disabled="state.loading.delete" @click="deletePlaylist">
          删除
        </button>
      </div>
    </div>

    <div class="detail-profile">
      <div class="profile-main">
        <div class="profile-title">
          <h2>{{ state.playlist.name || "播放列表" }}</h2>
          <span class="pill">{{ state.playlist.item_count || 0 }} 条</span>
        </div>
        <div class="profile-meta">
          <span>创建时间 <span class="mono">{{ state.playlist.created_at || "-" }}</span></span>
          <span>更新时间 <span class="mono">{{ state.playlist.updated_at || "-" }}</span></span>
        </div>
      </div>
    </div>
  </section>

  <section class="card works-card">
    <div class="card-header">
      <div>
        <h2>播放列表内容</h2>
        <p class="muted">按加入时间倒序排列。</p>
      </div>
      <button class="ghost" :disabled="state.items.loading" @click="reloadItems">
        刷新
      </button>
    </div>
    <div class="works-grid">
      <article v-for="item in state.items.items" :key="item.aweme_id" class="work-card">
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
          <div class="work-views">
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path
                d="M12 5c5.5 0 9.5 5.1 9.5 7s-4 7-9.5 7S2.5 14 2.5 12 6.5 5 12 5Zm0 2c-4 0-7.1 3.7-7.5 5 .4 1.3 3.5 5 7.5 5s7.1-3.7 7.5-5c-.4-1.3-3.5-5-7.5-5Zm0 2.5a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5Z"
              />
            </svg>
            <span>{{ formatCount(item.play_count) }}</span>
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
            <button
              class="danger ghost btn-mini"
              :disabled="state.items.removingId === item.aweme_id"
              @click="removeItem(item)"
            >
              移除
            </button>
          </div>
        </div>
      </article>
      <div v-if="!state.items.items.length && !state.items.loading" class="empty">
        暂无播放列表内容
      </div>
    </div>
    <PaginationBar
      :page="state.items.page"
      :page-size="state.items.pageSize"
      :total="state.items.total"
      @change="handleItemsPageChange"
    />
  </section>

  <section class="card">
    <div class="card-header">
      <div>
        <h2>导入作品</h2>
        <p class="muted">从作品库中选择后导入到播放列表。</p>
      </div>
      <button
        class="primary"
        :disabled="state.loading.import"
        @click="importSelected"
      >
        导入选中
      </button>
    </div>

    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th style="width: 48px;"></th>
            <th>作品</th>
            <th>播放</th>
            <th>时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in state.import.items" :key="item.aweme_id">
            <td>
              <input
                type="checkbox"
                v-model="state.import.selected[item.aweme_id]"
              />
            </td>
            <td>
              <div class="name-cell">
                <span class="name-avatar">
                  <img
                    v-if="item.cover"
                    :src="mediaUrl(item.cover)"
                    referrerpolicy="no-referrer"
                    alt="封面"
                  />
                  <span v-else class="name-avatar-fallback">无</span>
                </span>
                <span class="name-text" :title="item.desc || item.aweme_id">
                  {{ item.desc || item.aweme_id }}
                </span>
              </div>
            </td>
            <td class="mono">{{ formatCount(item.play_count) }}</td>
            <td class="mono">{{ formatDateTime(item) }}</td>
          </tr>
          <tr v-if="!state.import.items.length && !state.import.loading">
            <td colspan="4" class="empty">暂无可导入作品</td>
          </tr>
        </tbody>
      </table>
    </div>

    <PaginationBar
      :page="state.import.page"
      :page-size="state.import.pageSize"
      :total="state.import.total"
      @change="handleImportPageChange"
    />
  </section>
</template>

<script setup>
import { computed, inject, onMounted, reactive, watch } from "vue";
import { RouterLink, useRoute, useRouter } from "vue-router";
import PaginationBar from "../components/PaginationBar.vue";

const apiRequest = inject("apiRequest");
const setAlert = inject("setAlert");
const route = useRoute();
const router = useRouter();

const state = reactive({
  playlist: {},
  items: {
    items: [],
    total: 0,
    page: 1,
    pageSize: 12,
    loading: false,
    removingId: "",
  },
  import: {
    items: [],
    total: 0,
    page: 1,
    pageSize: 20,
    loading: false,
    selected: {},
  },
  loading: {
    refresh: false,
    clear: false,
    delete: false,
    import: false,
  },
});

const playlistId = computed(() => Number(route.params.playlistId || 0));

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

const loadPlaylist = async () => {
  if (!playlistId.value) {
    return;
  }
  const data = await apiRequest(`/admin/douyin/playlists/${playlistId.value}`);
  state.playlist = data || {};
};

const loadItems = async (page, append = false) => {
  if (!append) {
    state.items.loading = true;
  }
  try {
    const query = new URLSearchParams({
      page: String(page),
      page_size: String(state.items.pageSize),
    });
    const data = await apiRequest(
      `/admin/douyin/playlists/${playlistId.value}/items?${query.toString()}`
    );
    state.items.items = data.items || [];
    state.items.total = data.total || 0;
    state.items.page = page;
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.items.loading = false;
  }
};

const loadImportWorks = async (page) => {
  state.import.loading = true;
  try {
    const query = new URLSearchParams({
      page: String(page),
      page_size: String(state.import.pageSize),
    });
    const data = await apiRequest(`/admin/douyin/works/stored?${query.toString()}`);
    const items = data.items || [];
    let filteredItems = items;
    if (items.length) {
      const awemeIds = items.map((item) => item.aweme_id).filter(Boolean);
      if (!awemeIds.length) {
        state.import.items = items;
        state.import.total = data.total || 0;
        state.import.page = page;
        state.import.selected = {};
        return;
      }
      try {
        const check = await apiRequest(
          `/admin/douyin/playlists/${playlistId.value}/items/check`,
          {
            method: "POST",
            body: { aweme_ids: awemeIds },
          }
        );
        const exists = new Set(check?.data?.exists || []);
        filteredItems = items.filter((item) => !exists.has(item.aweme_id));
      } catch (error) {
        setAlert("error", error.message);
      }
    }
    state.import.items = filteredItems;
    state.import.total = data.total || 0;
    state.import.page = page;
    state.import.selected = {};
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.import.loading = false;
  }
};

const reloadItems = async () => {
  await loadItems(1);
};

const reloadAll = async () => {
  state.loading.refresh = true;
  try {
    await loadPlaylist();
    await loadItems(1);
    await loadImportWorks(1);
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.refresh = false;
  }
};

const importSelected = async () => {
  if (state.loading.import) {
    return;
  }
  const awemeIds = Object.entries(state.import.selected)
    .filter(([, selected]) => selected)
    .map(([awemeId]) => awemeId);
  if (!awemeIds.length) {
    setAlert("error", "请先选择作品");
    return;
  }
  state.loading.import = true;
  try {
    await apiRequest(`/admin/douyin/playlists/${playlistId.value}/items/import`, {
      method: "POST",
      body: { aweme_ids: awemeIds },
    });
    state.import.selected = {};
    await loadPlaylist();
    await loadItems(1);
    await loadImportWorks(state.import.page);
    setAlert("success", "导入完成");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.import = false;
  }
};

const clearPlaylist = async () => {
  if (!window.confirm("确认清空播放列表内容？")) {
    return;
  }
  state.loading.clear = true;
  try {
    await apiRequest(`/admin/douyin/playlists/${playlistId.value}/clear`, {
      method: "POST",
    });
    await loadPlaylist();
    await loadItems(1);
    await loadImportWorks(state.import.page);
    setAlert("success", "播放列表已清空");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.clear = false;
  }
};

const deletePlaylist = async () => {
  if (!window.confirm("确认删除该播放列表？")) {
    return;
  }
  state.loading.delete = true;
  try {
    await apiRequest(`/admin/douyin/playlists/${playlistId.value}`, {
      method: "DELETE",
    });
    setAlert("success", "播放列表已删除");
    await router.push("/playlists");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.delete = false;
  }
};

const removeItem = async (item) => {
  const awemeId = item?.aweme_id || "";
  if (!awemeId) {
    return;
  }
  if (!window.confirm("确认从当前播放列表移除该作品？")) {
    return;
  }
  if (state.items.removingId) {
    return;
  }
  state.items.removingId = awemeId;
  try {
    await apiRequest(`/admin/douyin/playlists/${playlistId.value}/items/remove`, {
      method: "POST",
      body: { aweme_ids: [awemeId] },
    });
    await loadPlaylist();
    await loadItems(state.items.page);
    await loadImportWorks(state.import.page);
    setAlert("success", "已移除");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.items.removingId = "";
  }
};

const handleItemsPageChange = async (page) => {
  await loadItems(page);
};

const handleImportPageChange = async (page) => {
  await loadImportWorks(page);
};

onMounted(async () => {
  await reloadAll();
});

watch(
  () => playlistId.value,
  async () => {
    state.items.items = [];
    state.import.items = [];
    state.import.selected = {};
    await reloadAll();
  }
);
</script>
