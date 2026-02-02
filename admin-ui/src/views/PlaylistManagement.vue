<template>
  <section class="card">
    <div class="card-header">
      <div>
        <h2>播放列表</h2>
        <p class="muted">维护播放列表，支持导入作品。</p>
      </div>
      <button class="ghost" :disabled="state.loading.list" @click="loadPlaylists">
        刷新
      </button>
    </div>

    <div class="form-row">
      <label class="field grow">
        <span>播放列表名称</span>
        <input
          v-model="state.form.name"
          placeholder="例如：早间合集"
          @keydown.enter.prevent="createPlaylist"
        />
      </label>
      <button
        class="primary"
        :disabled="state.loading.create || !state.form.name.trim()"
        @click="createPlaylist"
      >
        新增
      </button>
    </div>

    <div class="table-wrap playlist-table-wrap">
      <table class="playlist-table">
        <thead>
          <tr>
            <th>名称</th>
            <th>作品数</th>
            <th>更新时间</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in state.items" :key="item.id">
            <td>{{ item.name }}</td>
            <td class="mono">{{ item.item_count || 0 }}</td>
            <td class="mono">{{ item.updated_at || "-" }}</td>
            <td class="actions">
              <div class="action-group">
                <a
                  v-if="item.item_count > 0"
                  class="ghost btn-mini"
                  :href="clientPlaylistUrl(item.id)"
                  target="_blank"
                  rel="noreferrer"
                >
                  播放
                </a>
                <RouterLink class="ghost btn-mini" :to="`/playlists/${item.id}`">
                  详情
                </RouterLink>
                <button
                  class="danger ghost btn-mini"
                  @click="deletePlaylist(item.id, item.name)"
                >
                  删除
                </button>
              </div>
            </td>
          </tr>
          <tr v-if="!state.items.length && !state.loading.list">
            <td colspan="4" class="empty">暂无播放列表</td>
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
</template>

<script setup>
import { inject, onMounted, reactive } from "vue";
import { RouterLink } from "vue-router";
import PaginationBar from "../components/PaginationBar.vue";

const apiRequest = inject("apiRequest");
const setAlert = inject("setAlert");

const state = reactive({
  items: [],
  total: 0,
  page: 1,
  pageSize: 20,
  loading: {
    list: false,
    create: false,
    delete: false,
  },
  form: {
    name: "",
  },
});

const loadPlaylists = async () => {
  state.loading.list = true;
  try {
    const query = new URLSearchParams({
      page: String(state.page),
      page_size: String(state.pageSize),
    });
    const data = await apiRequest(`/admin/douyin/playlists?${query.toString()}`);
    state.items = data.items || [];
    state.total = data.total || 0;
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.list = false;
  }
};

const createPlaylist = async () => {
  const name = state.form.name.trim();
  if (!name || state.loading.create) {
    return;
  }
  state.loading.create = true;
  try {
    await apiRequest("/admin/douyin/playlists", {
      method: "POST",
      body: { name },
    });
    state.form.name = "";
    state.page = 1;
    await loadPlaylists();
    setAlert("success", "播放列表已创建");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.create = false;
  }
};

const deletePlaylist = async (id, name) => {
  if (!window.confirm(`确认删除播放列表“${name}”？`)) {
    return;
  }
  if (state.loading.delete) {
    return;
  }
  state.loading.delete = true;
  try {
    await apiRequest(`/admin/douyin/playlists/${id}`, { method: "DELETE" });
    await loadPlaylists();
    setAlert("success", "播放列表已删除");
  } catch (error) {
    setAlert("error", error.message);
  } finally {
    state.loading.delete = false;
  }
};

const clientPlaylistUrl = (id) => `/client-ui/playlist/${encodeURIComponent(id)}`;

const handlePageChange = async (page) => {
  state.page = page;
  await loadPlaylists();
};

onMounted(async () => {
  await loadPlaylists();
});
</script>
