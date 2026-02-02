import { createRouter, createWebHashHistory } from "vue-router";
import UserManagement from "../views/UserManagement.vue";
import UserDetail from "../views/UserDetail.vue";
import PlaylistManagement from "../views/PlaylistManagement.vue";
import PlaylistDetail from "../views/PlaylistDetail.vue";
import SettingsCookies from "../views/SettingsCookies.vue";
import ScheduleSettings from "../views/ScheduleSettings.vue";

const routes = [
  {
    path: "/",
    redirect: "/users",
  },
  {
    path: "/users",
    component: UserManagement,
  },
  {
    path: "/users/:secUserId",
    component: UserDetail,
  },
  {
    path: "/playlists",
    component: PlaylistManagement,
  },
  {
    path: "/playlists/:playlistId",
    component: PlaylistDetail,
  },
  {
    path: "/settings",
    component: SettingsCookies,
  },
  {
    path: "/schedule",
    component: ScheduleSettings,
  },
];

const router = createRouter({
  history: createWebHashHistory(import.meta.env.BASE_URL),
  routes,
});

export default router;
