import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import { resolve } from "node:path";

export default defineConfig({
  plugins: [vue()],
  base: "/admin-ui/",
  build: {
    outDir: resolve(__dirname, "../static/admin"),
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      "/admin/": "http://127.0.0.1:5555",
    },
  },
});
