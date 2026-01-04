import { createRouter, createWebHashHistory } from "vue-router";
import UserManagement from "../views/UserManagement.vue";
import UserDetail from "../views/UserDetail.vue";
import SettingsCookies from "../views/SettingsCookies.vue";

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
    path: "/settings",
    component: SettingsCookies,
  },
];

const router = createRouter({
  history: createWebHashHistory(import.meta.env.BASE_URL),
  routes,
});

export default router;
